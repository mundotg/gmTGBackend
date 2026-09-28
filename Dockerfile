# -------- Base --------
FROM python:3.12.2-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# -------- Dependências do sistema --------
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    build-essential \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# -------- Ferramentas de backup/restore (sempre) --------
# pg_dump/pg_restore/psql e mysqldump/mysql são o que os backups de PostgreSQL
# e MySQL executam. Eram opcionais (INSTALL_DUMP_TOOLS=false por omissão) e o
# deploy não passa esse build-arg: em produção não havia pg_dump e todo o
# backup PostgreSQL falhava.
#
# O cliente PostgreSQL vem do repositório oficial (PGDG) e não do Debian: o
# `postgresql-client` do bookworm é o 15, e o pg_dump recusa servidores mais
# novos do que ele ("server version mismatch"). Um pg_dump N faz dump de
# servidores até N — PG_CLIENT_VERSION deve ser >= à versão mais alta servida.
ARG PG_CLIENT_VERSION=17
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates gzip \
    && . /etc/os-release \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client-${PG_CLIENT_VERSION} default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# -------- mongodump/mongorestore (opcional, download pesado) --------
# O backup MongoDB usa PyMongo/BSON (db_backup_restore_pymongo) e não precisa
# destas ferramentas. Ativa com `--build-arg INSTALL_DUMP_TOOLS=true` se for
# preciso o formato do mongodump.
ARG INSTALL_DUMP_TOOLS=false
RUN if [ "$INSTALL_DUMP_TOOLS" = "true" ]; then \
      curl -fsSL https://pgp.mongodb.com/server-7.0.asc \
        | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg \
      && echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" \
        > /etc/apt/sources.list.d/mongodb-org-7.0.list \
      && apt-get update \
      && apt-get install -y --no-install-recommends mongodb-database-tools \
      && rm -rf /var/lib/apt/lists/* ; \
    fi

# -------- Microsoft SQL (FIX moderno) --------
RUN curl https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/mssql-release.list

# (se precisares do driver)
# RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# -------- Dependências Python --------
COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# -------- Código --------
COPY . .

# -------- Segurança (APENAS NO FINAL) --------
RUN chmod +x /app/docker-entrypoint.sh \
    && useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# -------- Run --------
# ENTRYPOINT + CMD (e não só CMD): o entrypoint corre `alembic upgrade head`
# antes do servidor e sobrevive a plataformas de deploy que substituem o
# comando. Para saltar as migrações numa emergência: RUN_MIGRATIONS=false.
ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]