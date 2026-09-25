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
# (load_dotenv), por isso basta o ficheiro estar em /app.
set -e

if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
  echo "[entrypoint] alembic upgrade head"
  alembic upgrade head
  echo "[entrypoint] migracoes aplicadas"
else
  # Valvula de escape para arrancar com uma migracao quebrada. Definir como
  # variavel de ambiente da plataforma: o .env e lido pelo Python, nao pela
  # shell, por isso RUN_MIGRATIONS ali dentro nao tem efeito nenhum.
  echo "[entrypoint] RUN_MIGRATIONS=false - migracoes ignoradas"
fi

exec "$@"
