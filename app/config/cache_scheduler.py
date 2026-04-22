import threading
import time
from datetime import datetime

from app.config.cache_manager import MEMORY_CACHE_TTL
from app.services.ocr._OCR_CACHE import MEMORY_CACHE
from app.ultils.logger import log_message

# importa do teu sistema


def schedule_cache_cleanup(interval: int = 60):
    """
    Scheduler de limpeza:
    - limpa cache em memória expirado (L1)
    - opcional: limpa Redis por segurança (casos edge)
    """

    def _cleanup_loop():
        log_message(
            f"[🧹 Cache Scheduler] Iniciado (intervalo: {interval}s)",
            "info",
        )

        while True:
            try:
                now = time.time()

                # -------------------------
                # 1️⃣ Limpeza L1 (RAM)
                # -------------------------
                removed = 0

                for key in list(MEMORY_CACHE.keys()):
                    entry = MEMORY_CACHE.get(key)

                    if not entry:
                        continue

                    ttl = entry.get("ttl", MEMORY_CACHE_TTL)

                    if ttl is not None and (now - entry["ts"]) > ttl:
                        MEMORY_CACHE.pop(key, None)
                        removed += 1

                if removed > 0:
                    log_message(
                        f"[🧠 L1 CLEANUP] {removed} entradas removidas",
                        "debug",
                    )

                # -------------------------
                # 2️⃣ Redis (opcional)
                # -------------------------
                # Normalmente NÃO precisa (Redis já expira sozinho)
                # Mas útil se TTL = None ou bugs antigos

                # Exemplo seguro:
                # limpar apenas chaves antigas manualmente (se quiser ativar)
                """
                if redis_client:
                    cursor = 0
                    while True:
                        cursor, keys = redis_client.scan(
                            cursor=cursor,
                            match=f"{CACHE_PREFIX}*",
                            count=100
                        )
                        # aqui podias validar TTL se quiseres
                        if cursor == 0:
                            break
                """

                log_message(
                    f"[🧹 Cache Scheduler] OK ({datetime.now()})",
                    "debug",
                )

            except Exception as e:
                log_message(
                    f"[🧹 Cache Scheduler] ERRO: {e}",
                    "error",
                )

            time.sleep(interval)

    thread = threading.Thread(target=_cleanup_loop, daemon=True)
    thread.start()
