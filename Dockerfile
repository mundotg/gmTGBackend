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
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# -------- Run --------
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]