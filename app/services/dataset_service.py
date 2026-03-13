from __future__ import annotations

import ipaddress
import re
import socket
import uuid
from io import BytesIO
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlparse

import httpx
import pandas as pd
from fastapi import HTTPException, UploadFile, status
from sqlalchemy import create_engine

DATASET_DIR = Path("datasets_locais")
ALLOWED_EXTENSIONS = {".csv", ".json", ".xls", ".xlsx"}
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB
SOURCE_TYPE = Literal["file", "url"]


def sanitize_filename(filename: str) -> str:
    """
    Limpa o nome do ficheiro para evitar caracteres perigosos.
    """
    filename = Path(filename).name.strip().lower()
    filename = filename.replace(" ", "_")
    filename = re.sub(r"[^a-zA-Z0-9._-]", "", filename)

    if not filename:
        filename = f"dataset_{uuid.uuid4().hex[:8]}.csv"

    return filename


def build_safe_table_name(filename: str) -> str:
    """
    Gera um nome de tabela seguro a partir do nome do ficheiro.
    """
    stem = Path(filename).stem.lower()
    stem = re.sub(r"[^a-zA-Z0-9_]", "_", stem)
    stem = re.sub(r"_+", "_", stem).strip("_")

    if not stem:
        stem = f"dataset_{uuid.uuid4().hex[:8]}"

    if stem[0].isdigit():
        stem = f"t_{stem}"

    return stem[:60]


def _is_private_host(hostname: str) -> bool:
    """
    Tenta bloquear hosts privados/locais para reduzir risco de SSRF.
    """
    try:
        ip = ipaddress.ip_address(socket.gethostbyname(hostname))
        return (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
        )
    except Exception:
        return False


def validate_public_url(url: str) -> str:
    """
    Valida a URL pública recebida.
    """
    if not url or not url.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL inválida.",
        )

    parsed = urlparse(url.strip())

    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A URL deve começar com http:// ou https://.",
        )

    if not parsed.netloc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL inválida.",
        )

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URL inválida.",
        )

    if hostname in {"localhost", "127.0.0.1", "::1"} or _is_private_host(hostname):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="URLs locais ou privadas não são permitidas.",
        )

    return url.strip()


def get_filename_from_url(url: str) -> str:
    """
    Extrai um nome de ficheiro da URL.
    """
    parsed = urlparse(url)
    filename = Path(parsed.path).name.lower()

    if not filename:
        filename = f"dataset_{uuid.uuid4().hex[:8]}.csv"

    filename = sanitize_filename(filename)

    if Path(filename).suffix not in ALLOWED_EXTENSIONS:
        filename = f"{Path(filename).stem}.csv"

    return filename


def _validate_size(contents: bytes, source_label: str) -> None:
    if not contents:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"O conteúdo recebido de {source_label} está vazio.",
        )

    if len(contents) > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"O arquivo de {source_label} excede o tamanho máximo permitido de 50 MB.",
        )


async def read_dataset_source(
    file: Optional[UploadFile],
    url: Optional[str],
) -> tuple[bytes, str, SOURCE_TYPE]:
    """
    Lê o conteúdo do dataset a partir de upload ou URL.
    Retorna: (conteúdo, filename, source_type)
    """
    if not file and not url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Forneça um arquivo de upload ou uma URL.",
        )

    if file and url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Forneça apenas um: arquivo ou URL, não ambos.",
        )

    if file:
        contents = await file.read()
        _validate_size(contents, "upload")

        filename = sanitize_filename(file.filename or f"dataset_{uuid.uuid4().hex[:8]}.csv")
        return contents, filename, "file"

    validated_url = validate_public_url(str(url))

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(validated_url)
            response.raise_for_status()
            contents = response.content

        _validate_size(contents, "URL")

        filename = get_filename_from_url(validated_url)
        return contents, filename, "url"

    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Falha ao baixar o dataset. Status remoto: {e.response.status_code}.",
        )
    except httpx.RequestError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Não foi possível aceder à URL fornecida.",
        )


def _make_unique_columns(columns: list[str]) -> list[str]:
    """
    Garante nomes de colunas únicos após sanitização.
    """
    seen: dict[str, int] = {}
    unique_columns: list[str] = []

    for col in columns:
        base = re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", str(col)).strip()).strip("_")
        base = base or "col"

        if base not in seen:
            seen[base] = 1
            unique_columns.append(base)
        else:
            seen[base] += 1
            unique_columns.append(f"{base}_{seen[base]}")

    return unique_columns


def _read_csv_with_fallbacks(contents: bytes) -> pd.DataFrame:
    """
    Lê CSV tentando múltiplos encodings e autodetecção de separador.
    """
    last_error: Exception | None = None

    for encoding in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(BytesIO(contents), sep=None, engine="python", encoding=encoding)
        except Exception as e:
            last_error = e

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Não foi possível interpretar o CSV. Erro: {str(last_error)}",
    )


def read_dataframe(contents: bytes, filename: str) -> pd.DataFrame:
    """
    Converte o conteúdo recebido num DataFrame do pandas.
    """
    ext = Path(filename).suffix.lower()

    try:
        if ext == ".csv":
            df = _read_csv_with_fallbacks(contents)
        elif ext in {".xls", ".xlsx"}:
            df = pd.read_excel(BytesIO(contents))
        elif ext == ".json":
            df = pd.read_json(BytesIO(contents))
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Formato não suportado. Use CSV, Excel ou JSON.",
            )

    except HTTPException:
        raise
    except pd.errors.EmptyDataError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="O dataset está vazio.",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Não foi possível interpretar o dataset: {str(e)}",
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Falha ao ler o conteúdo do dataset. Verifique o formato do arquivo.",
        )

    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="O dataset não contém linhas.",
        )

    df.columns = _make_unique_columns(list(df.columns))

    return df


def save_dataframe_to_sqlite(df: pd.DataFrame, user_id: int, table_name: str) -> str:
    """
    Salva o DataFrame num ficheiro SQLite local.
    Retorna o caminho absoluto do ficheiro.
    """
    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    db_filename = f"user_{user_id}_{table_name}_{uuid.uuid4().hex[:8]}.db"
    db_path = (DATASET_DIR / db_filename).resolve()

    engine_sqlite = create_engine(f"sqlite:///{db_path}")

    try:
        df.to_sql(name=table_name, con=engine_sqlite, if_exists="replace", index=False)
    finally:
        engine_sqlite.dispose()

    return str(db_path)