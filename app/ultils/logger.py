import logging
# Configuração de logging
logging.basicConfig(
    filename="database_connector.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


logger = logging.getLogger(__name__)

def log_message(message, level="info"):
        """Adiciona mensagem ao log visual e ao arquivo de log"""
        if level == "info":
            logger.info(message)
        elif level == "error":
            logger.error(message)
        elif level == "success":
            logger.info(message)
        elif level == "warning":
            logger.warning(message)
        else:
            logger.info(message)