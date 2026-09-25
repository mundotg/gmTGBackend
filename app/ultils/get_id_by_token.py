from __future__ import annotations

from typing import Any, Optional

from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials

from app.auth import decode_token
from app.config.api_security import bearer_scheme, cookie_scheme


def _extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header inválido. Use: Bearer <token>",
        )
    return parts[1].strip()


def _get_sub_from_payload(payload: Any) -> Optional[str]:
    """
    Suporta:
      - payload dict: {"sub": "..."}
      - payload string/int direto: "123" / 123
    """
    if payload is None:
        return None

    if isinstance(payload, dict):
        sub = payload.get("sub")
        return str(sub) if sub is not None else None

    if isinstance(payload, (str, int)):
        return str(payload)

    # payload em formato inesperado
    return None


# def get_current_user_id(
#     access_token: Optional[str] = Cookie(None, alias="access_token"),
#     authorization: Optional[str] = Header(None),
# ) -> int:
#     # 1) token via cookie tem prioridade
#     token = access_token or _extract_bearer_token(authorization)

#     if not token:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token não fornecido (cookie access_token ou Authorization Bearer).",
#         )

#     payload = decode_token(token)
#     sub = _get_sub_from_payload(payload)

#     if not sub:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token inválido ou expirado.",
#         )

#     try:
#         return int(sub)
#     except ValueError:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Token inválido: 'sub' não é numérico.",
#         )
def get_current_user_id(
    access_token: Optional[str] = Security(cookie_scheme),
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> int:
    # O cookie mantém prioridade sobre o header, como antes. O HTTPBearer já
    # valida o formato "Bearer <token>" e devolve None quando está malformado.
    token = access_token or (credentials.credentials if credentials else None)

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token não fornecido (cookie access_token ou Authorization Bearer).",
        )

    payload = decode_token(token)
    sub = _get_sub_from_payload(payload)

    if not sub:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado.",
        )

    try:
        return int(sub)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido: 'sub' não é numérico.",
        )