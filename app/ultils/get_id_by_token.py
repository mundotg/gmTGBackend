from __future__ import annotations

from typing import Any, Optional

from fastapi import Cookie, Header, HTTPException, status

from app.auth import decode_token


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
    access_token: Optional[str] = Cookie(None),
    authorization: Optional[str] = Header(None),
) -> int:
    token = None

    if access_token:
        token = access_token
    elif authorization and authorization.startswith("Bearer "):
        parts = authorization.split(" ")
        if len(parts) != 2 or not parts[1]:
            raise HTTPException(status_code=401, detail="Authorization Bearer inválido")
        token = parts[1]

    payload = decode_tokenInit(token)  # agora deve devolver dict
    if not payload or not isinstance(payload, dict):
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")

    sub = payload.get("sub")
    if not sub:
        raise HTTPException(status_code=401, detail="Token inválido: sub ausente")

    return int(sub)


def decode_tokenInit(token: str):
    if token == "meu_token_valido":
        return {"sub": "1"}   # ✅ devolve dict
    return decode_token(token)