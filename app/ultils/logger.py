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

from logging.handlers import TimedRotatingFileHandler

# Configurações de Rotação (FinOps - Aceitação da Tarefa 1.1)
DEFAULT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
DEFAULT_BACKUP_COUNT = 5              # Retém 5 backups
DEFAULT_WHEN = "midnight"             # Rotação diária


class TimedAndSizeRotatingFileHandler(TimedRotatingFileHandler):
    """
    Handler híbrido de rotação por tempo e tamanho (FinOps):
    - Rotação diária à meia-noite (when="midnight", interval=1)
    - Rotação imediata se atingir o limite de tamanho (maxBytes)
    - Retém no máximo backupCount ficheiros rotacionados
    - Preserva encoding UTF-8 para emojis e caracteres acentuados
    """

    def __init__(
        self,
        filename: str,
        when: str = DEFAULT_WHEN,
        interval: int = 1,
        backupCount: int = DEFAULT_BACKUP_COUNT,
        encoding: str = "utf-8",
        maxBytes: int = DEFAULT_MAX_BYTES,
        **kwargs,
    ):
        super().__init__(
            filename=filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            **kwargs,
        )
        self.maxBytes = maxBytes

    def shouldRollover(self, record: logging.LogRecord) -> int:
        # Verifica se o arquivo precisa de rotação por tamanho
        if self.stream is None:
            self.stream = self._open()
        if self.maxBytes > 0:
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)  # Move cursor para o fim
            if self.stream.tell() + len(msg.encode(self.encoding or "utf-8")) >= self.maxBytes:
                return 1
        # Fallback para rotação diária agendada
        return super().shouldRollover(record)


# Resolução de limites a partir do ambiente ou defaults
try:
    MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(DEFAULT_MAX_BYTES)))
except ValueError:
    MAX_BYTES = DEFAULT_MAX_BYTES

try:
    BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", str(DEFAULT_BACKUP_COUNT)))
except ValueError:
    BACKUP_COUNT = DEFAULT_BACKUP_COUNT

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_FORMATTER = logging.Formatter(LOG_FORMAT)

# Configuração do File Handler com rotação inteligente
file_handler = TimedAndSizeRotatingFileHandler(
    filename=str(LOG_FILE_PATH),
    when=os.getenv("LOG_ROTATE_WHEN", DEFAULT_WHEN),
    interval=1,
    backupCount=BACKUP_COUNT,
    encoding="utf-8",
    maxBytes=MAX_BYTES,
)
file_handler.setFormatter(LOG_FORMATTER)
file_handler.setLevel(logging.INFO)

# StreamHandler para o console (stdout / docker logs) com tratamento de UTF-8 seguro
try:
    # Garante que no Windows emojis como 🔧 e ❌ não rebentem por causa de cp1252
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    console_stream = sys.stdout
except Exception:
    console_stream = sys.stdout

console_handler = logging.StreamHandler(console_stream)
console_handler.setFormatter(LOG_FORMATTER)
console_handler.setLevel(logging.INFO)

# Configuração do root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Limpa handlers anteriores para evitar duplicações em hot-reloads
root_logger.handlers.clear()
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

# Se o arquivo já existente no disco exceder o limite (ex: os 25MB detectados),
# força a primeira rotação imediatamente na inicialização
try:
    if LOG_FILE_PATH.exists() and LOG_FILE_PATH.stat().st_size >= MAX_BYTES:
        file_handler.doRollover()
except Exception:
    pass

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
LogLevel = Literal["info", "error", "success", "warning", "debug"]


def _log_to_file(message: str, level: LogLevel = "info") -> None:
    """Adiciona mensagem ao log visual e ao arquivo de log"""
    # Dicionário substitui os vários if/elifs, deixando a busca O(1) e o código limpo
    level_map = {
        "info": logger.info,
        "error": logger.error,
        "success": logger.info,  # 'success' mapeia para info no logging padrão
        "warning": logger.warning,
        "debug": logger.debug,
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
