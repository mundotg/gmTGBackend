import logging
import os
import sys
from pathlib import Path
from typing import Optional, Union, Literal
from sqlalchemy.orm import Session


# ------------------------------------------------------------
# Localização do ficheiro de log
# ------------------------------------------------------------
# `basicConfig(filename="database_connector.log")` grava num caminho relativo,
# ou seja, na *working directory* do processo. Essa directoria muda conforme
# quem arranca a aplicação: `uvicorn` a partir da raiz do repositório, o
# contentor a partir de `/app`, o .exe do PyInstaller a partir de onde o
# utilizador clicou. O ficheiro existe sempre — mas ninguém sabe onde.
#
# Resolvemos o caminho para absoluto no arranque e guardamo-lo, para que a
# rota que lê o log não tenha de adivinhar.
LOG_FILE_NAME = "database_connector.log"

# Variável própria em vez de reaproveitar `LOG_FILE`: essa já está no
# .env.example a apontar para "app.log" e é lida por
# `dotenv.get_logging_config()`. Reutilizá-la mudaria o nome do ficheiro em
# qualquer ambiente que tenha copiado o exemplo — incluindo o `tail -f
# /app/database_connector.log` do comand_for_visualizar_logs.bat.
LOG_FILE_ENV = "DATABASE_CONNECTOR_LOG_FILE"


def _resolve_log_file() -> Path:
    """
    Caminho absoluto do ficheiro de log.

    `DATABASE_CONNECTOR_LOG_FILE` sobrepõe-se ao padrão e aceita tanto um
    ficheiro como a directoria onde o criar.
    """
    configurado = os.getenv(LOG_FILE_ENV)
    if configurado:
        caminho = Path(configurado).expanduser()
        if caminho.is_dir() or not caminho.suffix:
            caminho = caminho / LOG_FILE_NAME
        return caminho.resolve()

    return (Path.cwd() / LOG_FILE_NAME).resolve()


LOG_FILE_PATH = _resolve_log_file()

try:
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
except OSError:
    # Directoria sem permissões de escrita: cai para a working directory, que
    # é o comportamento antigo. Nunca deve impedir o arranque da aplicação.
    LOG_FILE_PATH = Path(LOG_FILE_NAME).resolve()

# Configuração de logging
logging.basicConfig(
    filename=str(LOG_FILE_PATH),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    # Sem isto o handler usa o encoding da locale — cp1252 no Windows. As
    # mensagens desta aplicação levam acentos e emojis (❌, ⚠️); em cp1252 a
    # escrita rebenta com UnicodeEncodeError, que o `logging` engole, e a
    # linha nunca chega ao ficheiro.
    encoding="utf-8",
)

logger = logging.getLogger(__name__)


def _candidatos() -> list[Path]:
    """Sítios onde o ficheiro pode estar quando não é o handler a ditá-lo."""
    raiz_projecto = Path(__file__).resolve().parents[2]

    caminhos = [
        LOG_FILE_PATH,
        Path.cwd() / LOG_FILE_NAME,
        raiz_projecto / LOG_FILE_NAME,
        Path("/app") / LOG_FILE_NAME,  # contentor Docker
    ]

    if getattr(sys, "frozen", False):  # PyInstaller
        caminhos.append(Path(sys.executable).resolve().parent / LOG_FILE_NAME)
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            caminhos.append(Path(meipass) / LOG_FILE_NAME)

    return caminhos


def get_log_file_path() -> Optional[Path]:
    """
    Onde está, agora, o ficheiro de log.

    Pergunta primeiro ao próprio `logging`: o handler activo sabe em que
    ficheiro está a escrever, mesmo que outro módulo tenha reconfigurado o
    logging depois deste import. Só depois procura nos sítios habituais.

    Devolve `None` se o ficheiro ainda não existir (nada foi registado).
    """
    for handler in logging.getLogger().handlers:
        base = getattr(handler, "baseFilename", None)
        if base:
            caminho = Path(base)
            if caminho.is_file():
                return caminho

    vistos: set[Path] = set()
    for caminho in _candidatos():
        try:
            resolvido = caminho.resolve()
        except OSError:
            continue
        if resolvido in vistos:
            continue
        vistos.add(resolvido)
        if resolvido.is_file():
            return resolvido

    return None

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
