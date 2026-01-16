import threading
import time
from datetime import datetime

from app.config.cache_manager import (
    CACHE_CLEANUP_INTERVAL,
    clear_expired_cache_files,
    _read_cache,  # função com @lru_cache
)
from app.ultils.logger import log_message


def schedule_cache_cleanup():
    """
    Agenda a limpeza periódica de caches antigos.
    Roda em segundo plano, sem bloquear a aplicação.
    Inclui limpeza do cache LRU em memória.
    """

    def _cleanup_loop():
        log_message(f"[🧹 Cache Scheduler] Iniciando agendador de limpeza a cada {CACHE_CLEANUP_INTERVAL}s")

        while True:
            try:
                # 1️⃣ Limpa arquivos expirados no disco
                clear_expired_cache_files()

                # 2️⃣ Limpa também o cache em memória (LRU)
                if hasattr(_read_cache, "cache_clear"):
                    _read_cache.cache_clear()
                    log_message("[🧠 LRU Cache] Cache em memória limpo com sucesso.", "debug")

                log_message(f"[🧹 Cache Scheduler] Execução concluída às {datetime.now()}")
            except Exception as e:
                log_message(f"[🧹 Cache Scheduler] Erro ao limpar caches: {e}", "error")

            time.sleep(CACHE_CLEANUP_INTERVAL)

    # Executa em thread separada (daemon = True para não bloquear encerramento do app)
    thread = threading.Thread(target=_cleanup_loop, daemon=True)
    thread.start()
