import threading
import time
from datetime import datetime

from app.config.cache_manager import CACHE_CLEANUP_INTERVAL, clear_expired_cache_files, logger

def schedule_cache_cleanup():
    """
    Agenda a limpeza periódica de caches antigos.
    Roda em segundo plano, sem bloquear a aplicação.
    """
    def _cleanup_loop():
        logger.info(f"[🧹 Cache Scheduler] Iniciando agendador de limpeza a cada {CACHE_CLEANUP_INTERVAL}s")
        while True:
            try:
                clear_expired_cache_files() # Remove até 10 arquivos expirado
                
                logger.debug(f"[🧹 Cache Scheduler] Execução concluída às {datetime.now()}")
            except Exception as e:
                logger.error(f"[🧹 Cache Scheduler] Erro ao limpar caches: {e}")
            time.sleep(CACHE_CLEANUP_INTERVAL)

    thread = threading.Thread(target=_cleanup_loop, daemon=True)
    thread.start()
