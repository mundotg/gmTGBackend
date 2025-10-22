# config/startup_reset.py
import os
import pkgutil
import importlib
import pickle
import socket
import platform
from pathlib import Path
from sqlalchemy.exc import SQLAlchemyError
from app.config.dotenv import get_env
from app.config.reset_db import recreate_db
from app.database import sync_engine as engine
from app.ultils.logger import log_message

# Caminho da pasta de models
MODELS_PACKAGE = "app.models"
FLAG_FILE = get_env("FLAG_FILE", "app/config/initialized.pkl")


# -----------------------------------------------------------
# ⚙️ Funções utilitárias
# -----------------------------------------------------------
def already_initialized() -> bool:
    """Verifica se o banco já foi inicializado anteriormente."""
    return os.path.exists(FLAG_FILE)


def mark_initialized():
    """Marca o banco como inicializado."""
    os.makedirs(os.path.dirname(FLAG_FILE), exist_ok=True)
    with open(FLAG_FILE, "wb") as f:
        pickle.dump({"initialized": True, "timestamp": os.path.getctime(__file__)}, f)
    log_message(f"📦 Flag de inicialização criada em {FLAG_FILE}", "info")


def clear_initialization_flag():
    """Remove a flag de inicialização (útil para testes)."""
    if os.path.exists(FLAG_FILE):
        os.remove(FLAG_FILE)
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
    
    for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        if not is_pkg:
            try:
                importlib.import_module(module_name)
                log_message(f"✅ Módulo importado: {module_name}", "success")
                success_count += 1
            except Exception as e:
                log_message(f"⚠️ Erro ao importar módulo {module_name}: {e}", "error")
                error_count += 1
    
    log_message(f"📊 Carregamento concluído: {success_count} sucessos, {error_count} erros", "info")
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
        log_message("❌ Falha no carregamento de models, abortando sincronização", "error")
        return False

    try:
        from app.models import Base
    except ImportError as e:
        log_message(f"❌ Erro: não foi possível importar Base de app.models: {e}", "error")
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
            log_message(f"✅ {len(created_tables)} novas tabelas criadas: {list(created_tables)}", "success")
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
    Retorna o caminho correto para o arquivo de logs, de acordo com:
    - Sistema operacional (Windows / Linux)
    - Ambiente (Local / Azure App Service)
    - Hostname e IP
    """
    import os
    import socket
    import platform
    from pathlib import Path
    from app.ultils.logger import log_message

    try:
        hostname = socket.gethostname()
        current_ip = socket.gethostbyname(hostname)
    except socket.error:
        hostname = "localhost"
        current_ip = "127.0.0.1"

    system_os = platform.system().lower()
    local_ips = {"127.0.0.1", "localhost"}

    # 🔹 Detecta se é ambiente Azure
    is_azure = (
        "WEBSITE_SITE_NAME" in os.environ
        or "WEBSITE_INSTANCE_ID" in os.environ
        or os.path.exists("/home/LogFiles")
        or os.path.exists("D:\\home\\LogFiles")
    )

    # 🔹 Caminhos candidatos
    azure_path_windows = Path("D:/home/LogFiles/Application")
    azure_path_linux = Path("/home/LogFiles/Application")

    # 🔹 Decide o diretório
    if is_azure:
        if system_os == "windows" and azure_path_windows.drive and os.path.exists(azure_path_windows.drive + "\\"):
            log_dir = azure_path_windows
        elif system_os == "linux" and azure_path_linux.exists():
            log_dir = azure_path_linux
        else:
            # fallback se Azure não estiver acessível localmente
            log_dir = Path("C:/logs" if system_os == "windows" else "/tmp/logs")
    else:
        # Ambiente local
        log_dir = Path("logs" if system_os == "windows" else "/tmp/logs")

    # 🔹 Cria o diretório
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        # fallback de emergência
        log_dir = Path("C:/logs" if system_os == "windows" else "/tmp/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_message(f"⚠️ Falha ao criar diretório original, fallback para {log_dir}: {e}", "warning")

    log_path = log_dir / "logs.txt"

    # 🔹 Log informativo
    env_label = "Azure App Service" if is_azure else "Localhost"
    log_message(
        f"[LOG PATH] Sistema: {system_os}, Ambiente: {env_label}, "
        f"Hostname: {hostname}, IP: {current_ip}, Caminho: {log_path}",
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
        log_message(f"🚫 Sincronização automática desativada (ENV={env}, FORCE_DB_RESET={force_reset})", "warning")
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
        if get_env("ENV", "dev").lower() == "prod":
            log_message("🔄 Verificando atualizações incrementais...", "info")
            apply_model_updates()


def get_initialization_status() -> dict:
    """
    Retorna o status atual da inicialização.
    """
    status = {
        "initialized": already_initialized(),
        "flag_file": FLAG_FILE,
        "flag_exists": os.path.exists(FLAG_FILE),
        "environment": get_env("ENV", "dev"),
        "log_path": str(get_log_path())
    }
    
    if status["flag_exists"]:
        try:
            with open(FLAG_FILE, "rb") as f:
                flag_data = pickle.load(f)
            status["flag_data"] = flag_data
        except Exception as e:
            status["flag_error"] = str(e)
    
    return status