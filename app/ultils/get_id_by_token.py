from typing import Optional
from fastapi import Cookie, HTTPException, status

from app.auth import decode_token


def get_current_user_id(
    access_token: Optional[str] = Cookie(None)
) -> int:
    # print(f"[TOKEN] Access token recebido: {access_token}")
    if not access_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token vazio ou não fornecido")
    payload = decode_token(access_token)
    # print(f"[TOKEN] Payload decodificado: {payload}")
    if not payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token inválido ou expirado")
    return int(payload)

