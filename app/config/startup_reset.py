# config/startup_reset.py
import os
import sys
import pkgutil
import importlib
import socket
import platform
import pickle
from pathlib import Path
import time
from sqlalchemy.exc import SQLAlchemyError
from app.config.dotenv import get_env
from app.config.reset_db import recreate_db
from app.database import sync_engine as engine
from app.ultils.logger import log_message

# -----------------------------------------------------------
# 🔥 RESOLUÇÃO DE CAMINHO UNIVERSAL (Script vs PyInstaller)
# -----------------------------------------------------------
if getattr(sys, "frozen", False):
    # Se for um executável (.exe compilado pelo PyInstaller)
    # Pega na pasta exata onde o main.exe está localizado
    BASE_DIR = os.path.dirname(sys.executable)
else:
    # Se for script normal, sobe 3 níveis: app/config/startup_reset.py -> raiz do projeto
    BASE_DIR = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
FLAG_FILE = get_env("FLAG_FILE", "db_initialized.flag")
# O ficheiro será criado de forma segura na raiz (junto ao .exe ou raiz do projeto)
DEFAULT_FLAG_PATH = os.path.join(BASE_DIR, FLAG_FILE)


# Caminho da pasta de models
MODELS_PACKAGE = "app.models"


# -----------------------------------------------------------
# ⚙️ Funções utilitárias
# -----------------------------------------------------------
def already_initialized() -> bool:
    """Verifica se o banco já foi inicializado anteriormente."""
    print(f" Verificando flag de inicialização em: {DEFAULT_FLAG_PATH}")
    return os.path.exists(DEFAULT_FLAG_PATH)


def mark_initialized():
    """Marca o banco como inicializado."""
    os.makedirs(os.path.dirname(DEFAULT_FLAG_PATH), exist_ok=True)
    with open(DEFAULT_FLAG_PATH, "wb") as f:
        # Substituímos os.path.getctime(__file__) pelo momento atual (time.time())
        pickle.dump({"initialized": True, "timestamp": time.time()}, f)
    log_message(f"📦 Flag de inicialização criada em {DEFAULT_FLAG_PATH}", "info")


def clear_initialization_flag():
    """Remove a flag de inicialização (útil para testes)."""
    if os.path.exists(DEFAULT_FLAG_PATH):
        os.remove(DEFAULT_FLAG_PATH)
        log_message("🗑️ Flag de inicialização removida", "info")


# -----------------------------------------------------------
# 🧩 Carregamento dinâmico de models
# -----------------------------------------------------------
def load_all_models():
    """
    Importa dinamicamente todos os módulos dentro de app.models.
    Assim, qualquer novo model adicionado será incluído automaticamente.
    """
    log_message(f"📦 Carregando modelos do pacote: {MODELS_PACKAGE}", "info")

    try:
        package = importlib.import_module(MODELS_PACKAGE)
    except ImportError as e:
        log_message(f"❌ Falha ao importar pacote base de models: {e}", "error")
        return False

    success_count = 0
    error_count = 0

    for _, module_name, is_pkg in pkgutil.iter_modules(
        package.__path__, package.__name__ + "."
    ):
        if not is_pkg:
            try:
                importlib.import_module(module_name)
                log_message(f"✅ Módulo importado: {module_name}", "success")
                success_count += 1
            except Exception as e:
                log_message(f"⚠️ Erro ao importar módulo {module_name}: {e}", "error")
                error_count += 1

    log_message(
        f"📊 Carregamento concluído: {success_count} sucessos, {error_count} erros",
        "info",
    )
    return error_count == 0


# -----------------------------------------------------------
# 🧱 Aplicação de atualizações de schema
# -----------------------------------------------------------
def apply_model_updates():
    """
    Cria tabelas ausentes de todos os modelos detectados.
    Não apaga nem altera dados existentes.
    """
    log_message("🔄 Aplicando alterações de models no banco de dados...", "info")

    if not load_all_models():
        log_message(
            "❌ Falha no carregamento de models, abortando sincronização", "error"
        )
        return False

    try:
        from app.database import Base
    except ImportError as e:
        log_message(
            f"❌ Erro: não foi possível importar Base de app.database: {e}", "error"
        )
        return False

    try:
        # Lista tabelas existentes antes da criação
        from sqlalchemy import inspect

        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        log_message(f"📋 Tabelas existentes: {len(existing_tables)}", "info")

        # Cria novas tabelas
        Base.metadata.create_all(bind=engine, checkfirst=True)

        # Lista tabelas após criação
        new_tables = inspector.get_table_names()
        created_tables = set(new_tables) - set(existing_tables)

        if created_tables:
            log_message(
                f"✅ {len(created_tables)} novas tabelas criadas: {list(created_tables)}",
                "success",
            )
        else:
            log_message("ℹ️ Nenhuma nova tabela criada (todas já existiam)", "info")

        return True

    except SQLAlchemyError as e:
        log_message(f"❌ Erro de SQLAlchemy ao sincronizar o banco: {e}", "error")
        return False
    except Exception as e:
        log_message(f"❌ Erro inesperado ao sincronizar o banco: {e}", "error")
        return False


def get_log_path() -> Path:
    """
    Retorna o caminho correto para o arquivo de logs, considerando:
    - Sistema operacional (Windows / Linux)
    - Ambiente (Local / Azure App Service)
    - Hostname e IP
    """
    # Importação mantida internamente apenas se houver risco de importação circular
    from app.ultils.logger import log_message

    # 1. Coleta de informações de rede
    try:
        hostname = socket.gethostname()
        current_ip = socket.gethostbyname(hostname)
    except socket.error:
        hostname, current_ip = "localhost", "127.0.0.1"

    is_windows = platform.system().lower() == "windows"

    # 2. Caminho de emergência absoluto (Fallback)
    fallback_dir = Path("C:/logs" if is_windows else "/tmp/logs")

    # 3. Detecta se é ambiente Azure (App Service)
    is_azure = (
        "WEBSITE_SITE_NAME" in os.environ
        or "WEBSITE_INSTANCE_ID" in os.environ
        or os.path.exists("/home/LogFiles")
        or os.path.exists("D:\\home\\LogFiles")
    )

    # 4. Define o diretório principal com base no ambiente
    if is_azure:
        log_dir = Path(
            "D:/home/LogFiles/Application"
            if is_windows
            else "/home/LogFiles/Application"
        )
    else:
        log_dir = Path(
            "logs"
        )  # Local: usa pasta relativa na raiz do projeto por padrão

    # 5. Tenta criar o diretório e garante que tem permissão de escrita
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        # Verifica se o programa consegue realmente escrever neste diretório
        if not os.access(log_dir, os.W_OK):
            raise PermissionError("Sem permissão de escrita no diretório.")
    except Exception as e:
        # Ativa o fallback de emergência
        log_dir = fallback_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        log_message(
            f"⚠️ Falha ao usar diretório principal ({e}). Fallback para {log_dir}",
            "warning",
        )

    log_path = log_dir / "logs.txt"

    # 6. Log informativo
    env_label = "Azure App Service" if is_azure else "Localhost"
    log_message(
        f"[LOG PATH] SO: {'Windows' if is_windows else 'Linux'}, Ambiente: {env_label}, "
        f"Host: {hostname}, IP: {current_ip}, Caminho: {log_path}",
        "info",
    )

    return log_path


def should_run_initialization() -> bool:
    """
    Determina se a inicialização deve ser executada.
    """
    env = get_env("ENV", "dev").lower()
    force_reset = get_env("FORCE_DB_RESET", "false").lower() == "true"

    if env != "dev" and not force_reset:
        log_message(
            f"🚫 Sincronização automática desativada (ENV={env}, FORCE_DB_RESET={force_reset})",
            "warning",
        )
        return False

    if force_reset:
        log_message("🚨 FORCE_DB_RESET ativado - reinicializando banco", "warning")
        clear_initialization_flag()

    return True


# -----------------------------------------------------------
# 🚀 Inicialização no startup
# -----------------------------------------------------------
def init_on_startup():
    """
    Executa durante o startup da aplicação.
    """
    LOG_FILE = get_log_path()
    log_message(f"📄 Log file: {LOG_FILE}", "info")

    if not should_run_initialization():
        return

    if not already_initialized():
        log_message("🔧 Iniciando processo de inicialização do banco...", "info")
        print(" Inicializando banco de dados...", already_initialized())
        if recreate_db():
            if apply_model_updates():
                mark_initialized()
                log_message("🎉 Inicialização concluída com sucesso!", "success")
            else:
                log_message("❌ Falha na aplicação das atualizações do modelo", "error")
        else:
            log_message("❌ Falha na recriação do banco", "error")
    else:
        log_message("🔒 Banco já inicializado anteriormente — sem alterações.", "info")
        # Aplica apenas atualizações incrementais em produção
        log_message("🔄 Verificando atualizações incrementais...", "info")
        apply_model_updates()


def get_initialization_status() -> dict:
    """
    Retorna o status atual da inicialização.
    """
    status = {
        "initialized": already_initialized(),
        "flag_file": DEFAULT_FLAG_PATH,
        "flag_exists": os.path.exists(DEFAULT_FLAG_PATH),
        "environment": get_env("ENV", "dev"),
        "log_path": str(get_log_path()),
    }

    if status["flag_exists"]:
        try:
            with open(DEFAULT_FLAG_PATH, "rb") as f:
                flag_data = pickle.load(f)
            status["flag_data"] = flag_data
        except Exception as e:
            status["flag_error"] = str(e)

    return status
