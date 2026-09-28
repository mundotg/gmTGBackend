"""
🔐 Decisão de SSL para ligações PostgreSQL — uma regra só, usada pelo engine
da app (asyncpg) e pelas ferramentas de linha de comando (pg_dump/pg_restore).

Porquê: o `sslmode` guardado na conexão é quase sempre o default da coluna
(`disable`), não uma escolha do utilizador. Levado à letra, um servidor de
produção que exige SSL recusava a ligação; ao contrário, forçar SSL (o antigo
`PGSSLMODE=require` do .env) partia o Postgres local, que não fala SSL.

Regra:
  1. `require` / `verify-ca` / `verify-full` na conexão → respeitado (é uma
     escolha explícita; não se baixa a segurança pedida).
  2. Host local (localhost, 127.0.0.1, ::1, host.docker.internal, ...) →
     `disable`. O Postgres local normalmente não tem SSL e, atrás do proxy do
     Docker Desktop, até a tentativa de upgrade falha ("rejected SSL upgrade").
  3. Qualquer outro host → `prefer`: tenta SSL e, se o servidor responder que
     não suporta, continua sem. Funciona contra servidores com SSL obrigatório,
     opcional ou inexistente — sem ninguém ter de acertar o `sslmode`.

`prefer`/`require` cifram mas não validam o certificado (semântica libpq), o
que é o que se quer com os certificados self-signed comuns em produção. Quem
precisar de validação escolhe `verify-ca`/`verify-full` na conexão.
"""

from __future__ import annotations

from typing import Optional, Union

from app.config.dotenv import get_env

STRICT_SSLMODES = {"require", "verify-ca", "verify-full"}

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal"}


def _is_local_host(host: Optional[str]) -> bool:
    h = (host or "").strip().lower().strip("[]")
    if not h:
        # Sem host o libpq usa o socket local.
        return True
    # O alias para o anfitrião é configurável (ver dependencies._maybe_remap_host).
    alias = (get_env("DB_LOCALHOST_ALIAS", "host.docker.internal") or "").strip().lower()
    return h in _LOCAL_HOSTS or (bool(alias) and h == alias)


def resolve_pg_sslmode(host: Optional[str], sslmode: Optional[str]) -> str:
    """Devolve o sslmode libpq a usar: disable, prefer ou o modo estrito pedido."""
    mode = (sslmode or "").strip().lower()
    if mode in STRICT_SSLMODES:
        return mode
    if _is_local_host(host):
        return "disable"
    return "prefer"


def asyncpg_ssl_arg(host: Optional[str], sslmode: Optional[str]) -> Union[bool, str]:
    """
    Valor para `connect_args={"ssl": ...}` do asyncpg. O asyncpg aceita os
    mesmos nomes do libpq ('prefer', 'require', 'verify-full', ...); `disable`
    vira False para nem sequer tentar o upgrade.
    """
    mode = resolve_pg_sslmode(host, sslmode)
    return False if mode == "disable" else mode
