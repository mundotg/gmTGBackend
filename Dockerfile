# -------- Base --------
FROM python:3.12.2-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app 

# -------- Dependências do sistema --------
# -------- Base --------
FROM python:3.12.2-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app 

# -------- Dependências do sistema --------
# CORREÇÃO: Cada pacote na sua linha, a barra sempre isolada no final
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    build-essential \
    gcc \
    g++ \
    git \
    unixodbc \
    unixodbc-dev \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Baixar chaves e adicionar repositório da Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list


WORKDIR /app

# -------- Cache de Dependências --------
COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# INSTALA APENAS A VERSÃO CPU (O Render não tem GPU Grátis)
# Isto vai poupar MUITO tempo e espaço
# RUN python -m pip install paddlepaddle==3.2.0 -i https://www.paddlepaddle.org.cn/packages/stable/cpu/

# -------- Copia o Código --------
COPY . .

# -------- Segurança --------
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# -------- Run --------
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# -------- Cache de Dependências --------
COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# INSTALA APENAS A VERSÃO CPU (O Render não tem GPU Grátis)
# Isto vai poupar MUITO tempo e espaço
# RUN python -m pip install paddlepaddle==3.2.0 -i https://www.paddlepaddle.org.cn/packages/stable/cpu/

# -------- Copia o Código --------
COPY . .

# -------- Segurança --------
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# -------- Run (CORRIGIDO) --------
# Se o ficheiro é /app/app/main.py, o comando deve ser app.main:app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]