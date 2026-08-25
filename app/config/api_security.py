"""
Esquemas de segurança declarados no OpenAPI.

Antes desta camada, cada endpoint declarava `Cookie(None)` + `Header(None)`
directamente na assinatura. O FastAPI documentava-os como parâmetros normais,
o que partia o Swagger de duas maneiras:

1. `access_token` aparecia como um campo de texto `in: cookie`. O browser
   proíbe definir o header `Cookie` a partir de JS, por isso o Swagger UI
   descartava o valor em silêncio e o "Try it out" devolvia sempre 401.
2. Não havia `securitySchemes`, logo não havia botão **Authorize** — o token
   tinha de ser colado endpoint a endpoint.

Ao usar as classes de segurança do FastAPI, os dois parâmetros saem da lista
de parâmetros e passam a alimentar o botão Authorize.
"""

from __future__ import annotations

from fastapi.security import APIKeyCookie, HTTPBearer

# O cookie é emitido por POST /auth/login. Como o Swagger UI é servido pela
# própria API (mesma origem), o browser reenvia-o automaticamente nos pedidos
# seguintes — não é preciso preencher nada.
cookie_scheme = APIKeyCookie(
    name="access_token",
    scheme_name="cookieAuth",
    description=(
        "Autenticação por cookie (predefinição da aplicação).\n\n"
        "Faz `POST /auth/login` aqui no Swagger: o cookie `access_token` fica "
        "guardado no browser e viaja sozinho nos pedidos seguintes."
    ),
    auto_error=False,
)

# Alternativa para clientes que não guardam cookies (Postman, curl, testes).
bearer_scheme = HTTPBearer(
    scheme_name="bearerAuth",
    description=(
        "Alternativa ao cookie: envia `Authorization: Bearer <access_token>`.\n\n"
        "Útil para clientes sem cookie jar. O cookie tem prioridade quando "
        "ambos estão presentes."
    ),
    auto_error=False,
)

# Mesmo mecanismo, mas para o cookie de refresh (só usado em /auth/refresh e
# /auth/logout). Fica separado para não sugerir que serve os outros endpoints.
refresh_cookie_scheme = APIKeyCookie(
    name="refresh_token",
    scheme_name="refreshCookieAuth",
    description="Cookie `refresh_token`, emitido no login. Usado só para renovar a sessão.",
    auto_error=False,
)
