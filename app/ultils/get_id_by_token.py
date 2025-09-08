from fastapi import Cookie, Header, HTTPException, status
from typing import Optional
import base64
from app.auth import decode_token

def get_current_user_id(
    access_token: Optional[str] = Cookie(None),
    authorization: Optional[str] = Header(None)
) -> int:
    token = None

    # 1. Se veio no cookie, usa ele
    if access_token:
        token = access_token
    
    # 2. Caso contrário, tenta pegar do Authorization Basic
    elif authorization and authorization.startswith("Basic "):
        # print("Tentando pegar token do Authorization Basic",authorization)
        try:
            token = authorization.split(" ")[1] 
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Credenciais Basic inválidas"
            )

    # 3. Se não recebeu nenhum tipo de credencial
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token vazio ou não fornecido"
        )

    # 4. Decodifica e valida
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado"
        )

    return int(payload)
