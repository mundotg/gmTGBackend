# -------- Base --------
FROM python:3.12.2-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# -------- Dependências do sistema --------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    curl \
    git \
    unixodbc \
    unixodbc-dev \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -------- Diretório --------
WORKDIR /app

# -------- Cache inteligente (IMPORTANTE) --------
# Só copia requirements primeiro (melhora MUITO o build)
COPY requirements.txt .

# Atualiza pip + instala deps
RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

RUN python -m pip install paddlepaddle==3.2.0 -i https://www.paddlepaddle.org.cn/packages/stable/cpu/
RUN python -m pip install paddlepaddle-gpu==3.2.0 -i https://www.paddlepaddle.org.cn/packages/stable/cu118/
# -------- Copia app --------
COPY . .

# -------- Segurança (rodar sem root) --------
RUN useradd -m appuser
USER appuser

# -------- Porta --------
EXPOSE 8000

# -------- Run --------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]