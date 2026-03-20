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
MAX_SQLITE_COLUMNS = 1500
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


def _make_unique_columns(columns: list[str]) -> list[str]:
    """
    Garante nomes de colunas únicos após sanitização.
    """
    seen: dict[str, int] = {}
    unique_columns: list[str] = []

    for col in columns:
        base = re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9_]", "_", str(col)).strip()).strip("_")
        base = base or "col"

        if base[0].isdigit():
            base = f"col_{base}"

        if base not in seen:
            seen[base] = 1
            unique_columns.append(base)
        else:
            seen[base] += 1
            unique_columns.append(f"{base}_{seen[base]}")

    return unique_columns


def clean_dataframe_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa o DataFrame para reduzir problemas comuns de ficheiros Excel/CSV
    antes de salvar no SQLite.
    """
    df = df.copy()

    # remover linhas totalmente vazias
    df = df.dropna(axis=0, how="all")

    # remover colunas totalmente vazias
    df = df.dropna(axis=1, how="all")

    # remover colunas do tipo Unnamed: x
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed", na=False)]

    normalized_cols: list[str] = []

    for i, col in enumerate(df.columns):
        name = str(col).strip()

        if not name or name.lower().startswith("unnamed"):
            name = f"column_{i}"

        name = (
            name.replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace(".", "_")
        )

        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")

        if not name:
            name = f"column_{i}"

        normalized_cols.append(name)

    df.columns = _make_unique_columns(normalized_cols)

    return df


def _read_csv_with_fallbacks(contents: bytes) -> pd.DataFrame:
    """
    Lê CSV tentando múltiplos encodings e autodetecção de separador.
    """
    last_error: Exception | None = None

    for encoding in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(
                BytesIO(contents),
                sep=None,
                engine="python",
                encoding=encoding,
            )
        except Exception as e:
            last_error = e

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Não foi possível interpretar o CSV. Erro: {str(last_error)}",
    )


def read_dataframe(contents: bytes, filename: str) -> dict[str, pd.DataFrame]:
    """
    Converte o conteúdo recebido num dicionário de DataFrames.
    Para Excel, lê todas as abas. Para CSV, retorna uma única entrada.
    A chave do dicionário será o nome da tabela (aba ou ficheiro).
    """
    ext = Path(filename).suffix.lower()
    base_name = build_safe_table_name(filename)
    
    dict_dfs: dict[str, pd.DataFrame] = {}

    try:
        if ext == ".csv":
            df = _read_csv_with_fallbacks(contents)
            dict_dfs[base_name] = df
            
        elif ext in {".xls", ".xlsx"}:
            # sheet_name=None lê todas as abas. Retorna { "NomeAba": DataFrame }
            raw_dict = pd.read_excel(BytesIO(contents), sheet_name=None)
            
            for sheet_name, df in raw_dict.items():
                # Gera um nome seguro para cada aba
                safe_sheet = build_safe_table_name(f"{base_name}_{sheet_name}")
                dict_dfs[safe_sheet] = df
                
        elif ext == ".json":
            # Lê json normal, assumindo estrutura plana para uma tabela
            df = pd.read_json(BytesIO(contents))
            dict_dfs[base_name] = df
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

    # Limpar todos os DataFrames e remover os que ficarem vazios
    cleaned_dict: dict[str, pd.DataFrame] = {}
    
    for table_name, df in dict_dfs.items():
        if df.empty:
            continue
            
        cleaned_df = clean_dataframe_for_sqlite(df)
        
        if not cleaned_df.empty and cleaned_df.shape[1] > 0:
             if cleaned_df.shape[1] > MAX_SQLITE_COLUMNS:
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"A tabela {table_name} possui muitas colunas ({cleaned_df.shape[1]}). "
                        f"O máximo permitido para importação segura é {MAX_SQLITE_COLUMNS}."
                    ),
                )
             cleaned_dict[table_name] = cleaned_df

    if not cleaned_dict:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nenhuma tabela válida encontrada após a limpeza do ficheiro.",
        )

    return cleaned_dict


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

        if not contents:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="O arquivo enviado está vazio.",
            )

        _validate_size(contents, "upload")

        filename = sanitize_filename(
            file.filename or f"dataset_{uuid.uuid4().hex[:8]}.csv"
        )
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

        if not contents:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A URL não retornou conteúdo.",
            )

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
def save_dataframe_to_sqlite(dict_dfs: dict[str, pd.DataFrame], user_id: int, original_filename: str) -> tuple[str, list[str]]:
    """
    Salva um dicionário de DataFrames num único ficheiro SQLite local.
    Adiciona automaticamente uma Primary Key (pk) a cada tabela processada.
    Retorna o caminho absoluto do ficheiro .db e uma lista com os nomes das tabelas.
    """
    if not dict_dfs:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="O dataset não contém dados válidos para salvar.",
        )

    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    # Gera um nome base seguro para o ficheiro a partir do nome original
    base_name = build_safe_table_name(original_filename)
    db_filename = f"user_{user_id}_{base_name}_{uuid.uuid4().hex[:8]}.db"
    db_path = (DATASET_DIR / db_filename).resolve()

    engine_sqlite = create_engine(f"sqlite:///{db_path}")
    tabelas_guardadas = []

    try:
        from sqlalchemy.types import Integer
        
        for table_name, df in dict_dfs.items():
            # Trabalhamos com uma cópia para não poluir o DataFrame original
            df_to_save = df.copy()
            
            # ==========================================
            # LÓGICA PARA ADICIONAR A PRIMARY KEY (pk)
            # ==========================================
            if 'pk' not in df_to_save.columns:
                df_to_save.insert(0, 'pk', range(1, len(df_to_save) + 1))

            # Guarda a tabela no SQLite
            df_to_save.to_sql(
                name=table_name, 
                con=engine_sqlite, 
                if_exists="replace", 
                index=False,
                dtype={'pk': Integer()} # Define explicitamente como Integer
            )
            
            # Executa o comando SQL bruto para garantir a indexação da Chave Primária
            with engine_sqlite.begin() as connection:
                connection.exec_driver_sql(f'CREATE UNIQUE INDEX idx_{table_name}_pk ON "{table_name}" (pk)')
            
            tabelas_guardadas.append(table_name)
            
    except Exception as e:
         raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao guardar as tabelas na base de dados SQLite: {str(e)}",
        )
    finally:
        engine_sqlite.dispose()

    return str(db_path), tabelas_guardadas


def save_dataframe_to_sqlite_versao_antiga(
    df: pd.DataFrame, 
    user_id: int, 
    table_name: str
) -> str:

    df = clean_dataframe_for_sqlite(df)

    if df.empty or df.shape[1] == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="O dataset não contém dados válidos para salvar.",
        )

    if df.shape[1] > MAX_SQLITE_COLUMNS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"O dataset possui muitas colunas ({df.shape[1]}). "
                f"O máximo permitido para importação segura é {MAX_SQLITE_COLUMNS}."
            ),
        )

    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    db_filename = f"user_{user_id}_{table_name}_{uuid.uuid4().hex[:8]}.db"
    db_path = (DATASET_DIR / db_filename).resolve()

    engine_sqlite = create_engine(f"sqlite:///{db_path}")

    # adiciona pk
    df = df.copy()
    df.insert(0, "pk", range(1, len(df) + 1))

    try:
        with engine_sqlite.begin() as conn:

            # 🔥 cria tabela com PRIMARY KEY REAL
            columns_sql = []

            for col in df.columns:
                if col == "pk":
                    columns_sql.append('"pk" INTEGER PRIMARY KEY')
                else:
                    columns_sql.append(f'"{col}" TEXT')

            create_table_sql = f'''
                CREATE TABLE "{table_name}" (
                    {", ".join(columns_sql)}
                )
            '''

            conn.exec_driver_sql(create_table_sql)

        # 🔥 agora insere os dados (sem recriar tabela)
        df.to_sql(
            name=table_name,
            con=engine_sqlite,
            if_exists="append",  # 👈 MUITO IMPORTANTE
            index=False
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao guardar na base de dados SQLite: {str(e)}",
        )
    finally:
        engine_sqlite.dispose()

    return str(db_path)