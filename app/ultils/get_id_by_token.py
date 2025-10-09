from fastapi import Cookie, Header, HTTPException, status
from typing import Optional
from app.auth import decode_token
from fastapi import Depends, HTTPException, status, Cookie, Header
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import Optional

security = HTTPBasic()

def decode_tokenInit(token: str) -> Optional[str]:
    """
    Função fake para validar token JWT.
    Retorne o user_id (string ou int) se válido.
    """
    if token == "meu_token_valido":
        return "1"
    return decode_token(token)


def get_current_user_id(
    access_token: Optional[str] = Cookie(None),
    authorization: Optional[str] = Header(None),
    # credentials: HTTPBasicCredentials = Depends(security),
) -> int:
    token = None

    # 1️⃣ Primeiro: verifica se veio no Cookie
    if access_token:
        token = access_token

    # 2️⃣ Depois: verifica se veio Authorization: Bearer <token>
    elif authorization and authorization.startswith("Bearer "):
        try:
            token = authorization.split(" ")[1]
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization Bearer inválido"
            )

    # 3️⃣ Se não veio token -> tenta Basic Auth
    # else:
    #     if credentials.username != "admin" or credentials.password != "admin123":
    #         raise HTTPException(
    #             status_code=status.HTTP_401_UNAUTHORIZED,
    #             detail="Credenciais Basic inválidas",
    #             headers={"WWW-Authenticate": "Basic"},
    #         )
    #     return 1  # ID do admin hardcoded

    # 4️⃣ Valida token (cookie ou bearer)
    payload = decode_tokenInit(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado"
        )

    return int(payload)
