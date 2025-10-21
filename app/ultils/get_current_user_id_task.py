from fastapi import Header, HTTPException, status
from typing import Optional
from app.auth import decode_token_task


async def get_current_user_id_task(
    tokena: Optional[str] = Header(None),
    refresh_token: Optional[str] = Header(None,convert_underscores=False),
) -> str:
    """
    Extrai e valida o access_token (Bearer ou Cookie).
    Caso o access_token esteja expirado, tenta usar o refresh_token.
    Retorna o user_id do token válido.
    """
    token = None
    # print(f"tokena={tokena}  \nrefresh_token={refresh_token}")
    # 1️⃣ Tenta primeiro Authorization: Bearer <token>
    if tokena and tokena.startswith("Bearer "):
        token = tokena.split(" ")[1]
    # print("token:",token)
    # 3️⃣ Se não tiver token nenhum
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de acesso ausente"
        )

    # 4️⃣ Decodifica o access_token
    payload = decode_token_task(token)
    if not payload:
        # 5️⃣ Se o access_token for inválido, tenta refresh_token
        if refresh_token:
            try:
                refresh_payload = decode_token_task(refresh_token)
                # if refresh_payload:
                    # return refresh_payload
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Refresh token inválido: {e}"
                )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de acesso inválido ou expirado"
        )

    # 6️⃣ Retorna user_id do access_token válido
    # print(f"payload= {payload}")
    user_id = payload.get("user_id") if isinstance(payload, dict) else payload
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token sem user_id"
        )

    return user_id
