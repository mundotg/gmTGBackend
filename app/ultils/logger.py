import logging
from typing import Optional, Union, Literal
from sqlalchemy.orm import Session


# Configuração de logging
logging.basicConfig(
    filename="database_connector.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Definindo os níveis aceitos para melhor autocomplete e segurança (Type Hinting)
LogLevel = Literal["info", "error", "success", "warning"]


def _log_to_file(message: str, level: LogLevel = "info") -> None:
    """Adiciona mensagem ao log visual e ao arquivo de log"""
    # Dicionário substitui os vários if/elifs, deixando a busca O(1) e o código limpo
    level_map = {
        "info": logger.info,
        "error": logger.error,
        "success": logger.info,  # 'success' mapeia para info no logging padrão
        "warning": logger.warning,
    }

    # Executa a função correspondente ao nível, usando logger.info como padrão
    log_func = level_map.get(level, logger.info)
    log_func(message)


def log_message(
    message: str,
    level: LogLevel = "info",
    db: Optional[Session] = None,
    source: Optional[str] = None,
    user: Optional[Union[str, int]] = None,
    withBd: bool = False,
) -> None:
    """
    🔥 Logger otimizado:
    - Não quebra fluxo da aplicação
    - Usa DB se disponível
    - Fallback para file logger
    - Gerenciamento automático de sessão (Context Manager)
    """

    # 🔹 Log sempre no file/console (rápido)
    _log_to_file(message, level)
    if withBd:
        return
    # 🔹 Evita quebrar fluxo se DB falhar
    try:
        from app.database import SessionLocal
        from app.models.log_models import Log

        log_entry = Log(
            message=message,
            level=level,
            source=source,
            user=int(user) if user else None,
        )

        if db is not None:
            # ⚡ Sessão fornecida pelo caller: adiciona mas deixa o caller fazer o commit
            db.add(log_entry)
        else:
            # ⚡ Nenhuma sessão fornecida: cria uma temporária, commita e fecha automaticamente
            with SessionLocal() as temp_db:
                temp_db.add(log_entry)
                temp_db.commit()

    except Exception as e:
        # 🚨 Nunca deixa o sistema cair por causa de log
        logger.error(f"Erro ao salvar log no DB: {e}")
