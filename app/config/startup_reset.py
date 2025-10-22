# config/startup_reset.py
import os
import pkgutil
import importlib
import pickle
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
        pickle.dump({"initialized": True}, f)
    log_message(f"📦 Flag de inicialização criada em {FLAG_FILE}", "info")


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
        return

    for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        if not is_pkg:
            try:
                importlib.import_module(module_name)
                log_message(f"✅ Módulo importado: {module_name}", "success")
            except Exception as e:
                log_message(f"⚠️ Erro ao importar módulo {module_name}: {e}", "error")


# -----------------------------------------------------------
# 🧱 Aplicação de atualizações de schema
# -----------------------------------------------------------
def apply_model_updates():
    """
    Cria tabelas ausentes de todos os modelos detectados.
    Não apaga nem altera dados existentes.
    """
    log_message("🔄 Aplicando alterações de models no banco de dados...", "info")

    load_all_models()

    try:
        from app.models import Base
    except ImportError:
        log_message("❌ Erro: não foi possível importar Base de app.models.", "error")
        return

    try:
        Base.metadata.create_all(bind=engine)
        log_message("✅ Estrutura de banco sincronizada com sucesso.", "success")
    except SQLAlchemyError as e:
        log_message(f"❌ Erro de SQLAlchemy ao sincronizar o banco: {e}", "error")
    except Exception as e:
        log_message(f"❌ Erro inesperado ao sincronizar o banco: {e}", "error")


# -----------------------------------------------------------
# 🚀 Inicialização no startup
# -----------------------------------------------------------
def init_on_startup():
    """
    Executa durante o startup da aplicação (apenas no ambiente de desenvolvimento).
    Evita recriação em produção.
    """
    env = get_env("ENV", "dev").lower()

    if env != "dev":
        log_message("🚫 Sincronização automática desativada (ENV != dev).", "warning")
        return

    if not already_initialized():
        recreate_db()
        apply_model_updates()
        mark_initialized()

        log_message("📦 Inicialização concluída e marcada.", "success")
    else:
        log_message("🔒 Banco já inicializado anteriormente — sem alterações.", "info")
