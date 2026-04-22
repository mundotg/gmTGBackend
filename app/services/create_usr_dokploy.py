import httpx
import secrets
import asyncio
from typing import Optional, Dict

from app.ultils.logger import log_message

_in_progress: Dict[str, asyncio.Lock] = {}
semaphore = asyncio.Semaphore(10)

client = httpx.AsyncClient(timeout=10)


async def create_user_in_dokploy(email: str) -> Optional[dict]:
    url = "http://3.66.65.58:3000/api/user.createUserWithCredentials"

    if not email or "@" not in email:
        log_message(f"❌ Email inválido: {email}", "warning")
        return None

    email = email.strip().lower()

    lock = _in_progress.get(email)
    if not lock:
        lock = asyncio.Lock()
        _in_progress[email] = lock

    async with lock:
        async with semaphore:  # 🚦 controla carga
            try:
                password = secrets.token_urlsafe(16)

                payload = {
                    "email": email,
                    "password": password,
                    "role": "user",
                }

                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "Authorization": "Bearer SEU_TOKEN_ADMIN",
                        "Content-Type": "application/json",
                    },
                )

                try:
                    data = response.json()
                except Exception:
                    data = {"raw": response.text}

                if response.status_code in (200, 201):
                    log_message(f"✅ Criado: {email}", "info")
                    return data

                if response.status_code == 409:
                    log_message(f"⚠️ Já existe: {email}", "warning")
                    return data

                log_message(
                    f"❌ Erro\nStatus: {response.status_code}\nEmail: {email}",
                    "error",
                )
                return None

            except Exception as e:
                log_message(f"❌ Erro inesperado: {str(e)}", "error")
                return None

            finally:
                _in_progress.pop(email, None)
