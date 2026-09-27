#!/bin/sh
# Aplica as migracoes antes de entregar o controlo ao servidor.
#
# Porque e aqui e nao no arranque da app: com ENV=production o
# app/config/startup_reset.py nao cria nem sincroniza tabelas
# (should_run_initialization() devolve False fora de dev). Num banco novo
# ninguem criava o esquema e o seed do lifespan morria com
# «relation "empresas" does not exist», deixando o contentor em loop de
# reinicio.
#
# A URL vem de DATABASE_URL_ALEMBIC — o alembic/env.py le o .env por si
# (load_dotenv), por isso basta o ficheiro estar em /app. Atencao: uma
# variavel definida no ambiente da plataforma GANHA sobre o .env
# (load_dotenv usa override=False).
set -e

if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
  # Identifica o destino ANTES de ligar. Sem isto, um erro de credenciais
  # dava 100 linhas de traceback do SQLAlchemy sem dizer a que servidor,
  # com que utilizador nem a partir de que variavel se tentou ligar.
  python - <<'PY' || true
import os
from urllib.parse import urlparse

url = os.environ.get("DATABASE_URL_ALEMBIC")
origem = "ambiente"
if not url:
    from dotenv import dotenv_values
    url = (dotenv_values(".env") or {}).get("DATABASE_URL_ALEMBIC")
    origem = ".env"
if not url:
    print("[entrypoint] DATABASE_URL_ALEMBIC nao definida (nem no ambiente nem no .env)")
else:
    p = urlparse(url)
    print(
        f"[entrypoint] destino: {p.hostname}:{p.port or 5432}"
        f" base={(p.path or '').lstrip('/')} utilizador={p.username}"
        f" driver={p.scheme} (de {origem})"
    )
PY

  echo "[entrypoint] alembic upgrade head"
  if ! alembic upgrade head; then
    echo "[entrypoint] ERRO: as migracoes falharam."
    echo "[entrypoint] Para o diagnostico completo (URLs, ligacao, revisoes,"
    echo "[entrypoint] tabelas, Redis, storage) corre no contentor:"
    echo "[entrypoint]     python scripts/diagnostico.py"
    exit 1
  fi
  echo "[entrypoint] migracoes aplicadas"
else
  # Valvula de escape para arrancar com uma migracao quebrada. Definir como
  # variavel de ambiente da plataforma: o .env e lido pelo Python, nao pela
  # shell, por isso RUN_MIGRATIONS ali dentro nao tem efeito nenhum.
  echo "[entrypoint] RUN_MIGRATIONS=false - migracoes ignoradas"
fi

exec "$@"
