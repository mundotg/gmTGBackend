from __future__ import annotations

import ipaddress
import os
import re
import socket
import uuid
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Literal, Optional, Union
from urllib.parse import urlparse

import httpx
import pandas as pd
from fastapi import HTTPException, UploadFile, status
from sqlalchemy import create_engine

DATASET_DIR = Path("datasets_locais")
ALLOWED_EXTENSIONS = {".csv", ".json", ".xls", ".xlsx"}
MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024 * 1024  # 20 GB máximo
MAX_SQLITE_COLUMNS = 1500
SOURCE_TYPE = Literal["file", "url", "kaggle"]


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
            detail=f"O arquivo de {source_label} excede o tamanho máximo permitido de 20 GB.",
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


def _detect_delimiter_and_encoding(source: Union[bytes, BinaryIO]) -> tuple[str, str]:
    """
    Detecta o encoding e o delimitador do CSV a partir de uma amostra inicial (64KB),
    evitando ler todo o ficheiro com motor lento de Python.
    """
    if isinstance(source, bytes):
        sample = source[:65536]
    else:
        source.seek(0)
        sample = source.read(65536)
        source.seek(0)

    encoding = "utf-8"
    sample_text = ""

    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            sample_text = sample.decode(enc)
            encoding = enc
            break
        except UnicodeDecodeError:
            continue

    if not sample_text:
        sample_text = sample.decode("utf-8", errors="ignore")
        encoding = "utf-8"

    lines = [line.strip() for line in sample_text.splitlines() if line.strip()]
    if lines:
        header = lines[0]
        candidates = [",", ";", "\t", "|"]
        counts = {d: header.count(d) for d in candidates}
        best_sep = max(counts, key=counts.get)
        sep = best_sep if counts[best_sep] > 0 else ","
    else:
        sep = ","

    return sep, encoding


def _read_csv_with_fallbacks(source: Union[bytes, BinaryIO], max_rows: Optional[int] = None) -> pd.DataFrame:
    """
    Lê CSV com motor C ultrarrápido (dezenas de vezes mais veloz) e fallbacks seguros.
    Suporta tanto bytes em memória quanto streams abertos em disco (evitando MemoryError).
    """
    sep, detected_encoding = _detect_delimiter_and_encoding(source)
    last_error: Exception | None = None
    nrows = max_rows if (max_rows and max_rows > 0) else None

    def get_stream():
        if isinstance(source, bytes):
            return BytesIO(source)
        source.seek(0)
        return source

    # 1. Tenta motor C (ultra-rápido) com separador e encoding detectados
    encodings_to_try = [detected_encoding] + [
        enc for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252") if enc != detected_encoding
    ]

    for enc in encodings_to_try:
        try:
            return pd.read_csv(
                get_stream(),
                sep=sep,
                engine="c",
                encoding=enc,
                nrows=nrows,
                low_memory=False,
                on_bad_lines="skip",
            )
        except Exception as e:
            last_error = e

    # 2. Se falhar, tenta outros delimitadores comuns no motor C
    for alt_sep in (",", ";", "\t", "|"):
        if alt_sep == sep:
            continue
        try:
            return pd.read_csv(
                get_stream(),
                sep=alt_sep,
                engine="c",
                encoding=detected_encoding,
                nrows=nrows,
                low_memory=False,
                on_bad_lines="skip",
            )
        except Exception as e:
            last_error = e

    # 3. Fallback final: motor python (apenas como último recurso se houver caracteres estranhos)
    try:
        return pd.read_csv(
            get_stream(),
            sep=sep,
            engine="python",
            encoding=detected_encoding,
            nrows=nrows,
            on_bad_lines="skip",
        )
    except Exception as e:
        last_error = e

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Não foi possível interpretar o CSV. Erro: {str(last_error)}",
    )


def read_dataframe(source: Union[bytes, BinaryIO], filename: str, max_rows: Optional[int] = None) -> dict[str, pd.DataFrame]:
    """
    Converte o conteúdo recebido num dicionário de DataFrames.
    Para Excel, lê todas as abas. Para CSV, retorna uma única entrada.
    A chave do dicionário será o nome da tabela (aba ou ficheiro).
    """
    ext = Path(filename).suffix.lower()
    base_name = build_safe_table_name(filename)
    
    dict_dfs: dict[str, pd.DataFrame] = {}

    def get_stream():
        if isinstance(source, bytes):
            return BytesIO(source)
        source.seek(0)
        return source

    try:
        if ext == ".csv":
            df = _read_csv_with_fallbacks(source, max_rows=max_rows)
            dict_dfs[base_name] = df
            
        elif ext in {".xls", ".xlsx"}:
            # sheet_name=None lê todas as abas. Retorna { "NomeAba": DataFrame }
            raw_dict = pd.read_excel(get_stream(), sheet_name=None, nrows=max_rows if (max_rows and max_rows > 0) else None)
            
            for sheet_name, df in raw_dict.items():
                # Gera um nome seguro para cada aba
                safe_sheet = build_safe_table_name(f"{base_name}_{sheet_name}")
                dict_dfs[safe_sheet] = df
                
        elif ext == ".json":
            # Lê json normal, assumindo estrutura plana para uma tabela
            df = pd.read_json(get_stream())
            if max_rows and max_rows > 0 and len(df) > max_rows:
                df = df.iloc[:max_rows]
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
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Falha ao ler o conteúdo do dataset ({type(e).__name__}): {str(e) or 'formato inválido'}",
        )

    # Limpar todos os DataFrames e remover os que ficarem vazios
    cleaned_dict: dict[str, pd.DataFrame] = {}
    
    for table_name, df in dict_dfs.items():
        if df.empty:
            continue
            
        cleaned_df = clean_dataframe_for_sqlite(df)
        if max_rows and max_rows > 0 and len(cleaned_df) > max_rows:
            cleaned_df = cleaned_df.iloc[:max_rows]
        
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
    max_rows: Optional[int] = None,
) -> tuple[Union[bytes, BinaryIO], str, SOURCE_TYPE]:
    """
    Lê o conteúdo do dataset a partir de upload ou URL de forma segura para memória.
    Retorna: (stream_ou_conteúdo, filename, source_type)
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
        filename = sanitize_filename(
            file.filename or f"dataset_{uuid.uuid4().hex[:8]}.csv"
        )

        # Mede o tamanho real do arquivo temporário sem carregar na memória RAM
        file.file.seek(0, os.SEEK_END)
        file_size = file.file.tell()
        file.file.seek(0)

        if file_size == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="O arquivo enviado está vazio.",
            )

        # Se o arquivo ultrapassa o limite máximo global de 20 GB:
        if file_size > MAX_FILE_SIZE_BYTES:
            size_gb = file_size / (1024 * 1024 * 1024)
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"O arquivo enviado possui {size_gb:.2f} GB, excedendo o limite máximo permitido de 20 GB.",
            )

        return file.file, filename, "file"

    validated_url = validate_public_url(str(url))

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
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

        if len(contents) > MAX_FILE_SIZE_BYTES:
            size_gb = len(contents) / (1024 * 1024 * 1024)
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"O arquivo da URL possui {size_gb:.2f} GB, excedendo o limite máximo de 20 GB.",
            )

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
    Salva um dicionário de DataFrames num único ficheiro SQLite local de forma otimizada.
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
        
        # Otimizações de gravação em massa no SQLite
        with engine_sqlite.begin() as connection:
            connection.exec_driver_sql("PRAGMA synchronous = OFF;")
            connection.exec_driver_sql("PRAGMA journal_mode = MEMORY;")
            connection.exec_driver_sql("PRAGMA cache_size = 50000;")

        for table_name, df in dict_dfs.items():
            # Trabalhamos com uma cópia para não poluir o DataFrame original
            df_to_save = df.copy()
            
            # Adiciona Primary Key (pk) caso não exista
            if 'pk' not in df_to_save.columns:
                df_to_save.insert(0, 'pk', range(1, len(df_to_save) + 1))

            # Guarda a tabela no SQLite em lotes rápidos
            df_to_save.to_sql(
                name=table_name, 
                con=engine_sqlite, 
                if_exists="replace", 
                index=False,
                chunksize=10000,
                dtype={'pk': Integer()}
            )
            
            # Garante o índice da Chave Primária
            safe_idx_name = re.sub(r"[^a-zA-Z0-9_]", "_", f"idx_{table_name}_pk")[:60]
            with engine_sqlite.begin() as connection:
                connection.exec_driver_sql(f'CREATE UNIQUE INDEX IF NOT EXISTS "{safe_idx_name}" ON "{table_name}" ("pk")')
            
            tabelas_guardadas.append(table_name)
            
    except Exception as e:
         raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao guardar as tabelas na base de dados SQLite ({type(e).__name__}): {str(e)}",
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


# =========================================================
# Integração com Datasets do Kaggle
# =========================================================

def parse_kaggle_handle(raw_input: str) -> tuple[str, Optional[str]]:
    """
    Extrai o handle do Kaggle (owner/dataset) e opcionalmente o caminho do arquivo
    a partir de:
    1. Snippet de código Python (ex: kagglehub.load_dataset(..., 'owner/dataset', ...))
    2. URL do Kaggle (ex: https://www.kaggle.com/datasets/owner/dataset?select=file.csv)
    3. Handle direto (ex: owner/dataset)
    """
    raw = (raw_input or "").strip()
    file_path: Optional[str] = None

    if not raw:
        return "", None

    # Caso 1: Snippet Python com kagglehub
    if "kagglehub" in raw or "load_dataset" in raw or "dataset_download" in raw:
        match_handle = re.search(r"['\"]([a-zA-Z0-9_\-]+/[a-zA-Z0-9_\.\-]+)['\"]", raw)
        if match_handle:
            handle = match_handle.group(1)
            match_file = re.search(r"file_path\s*=\s*['\"]([^'\"]+)['\"]", raw)
            if match_file:
                fp = match_file.group(1).strip()
                if fp:
                    file_path = fp
            return handle, file_path

    # Caso 2: URL completa do Kaggle
    url_match = re.search(r"kaggle\.com/datasets/([a-zA-Z0-9_\-]+/[a-zA-Z0-9_\.\-]+)", raw)
    if url_match:
        handle = url_match.group(1)
        select_match = re.search(r"[?&]select=([^&#]+)", raw)
        if select_match:
            file_path = select_match.group(1).strip() or None
        return handle, file_path

    # Caso 3: Formato direto owner/dataset
    direct_match = re.search(r"^([a-zA-Z0-9_\-]+/[a-zA-Z0-9_\.\-]+)$", raw)
    if direct_match:
        return direct_match.group(1), None

    # Fallback
    return raw, None


def setup_kaggle_credentials(
    username: Optional[str] = None,
    key: Optional[str] = None,
    save_to_env: bool = False,
) -> None:
    """
    Configura as credenciais da API do Kaggle no runtime do kagglehub.
    Se save_to_env=True, também persiste no ficheiro .env.
    """
    import kagglehub.config as kh_config

    u = (username or "").strip()
    k = (key or "").strip()

    if u and k:
        kh_config.set_kaggle_credentials(u, k)
        os.environ["KAGGLE_USERNAME"] = u
        os.environ["KAGGLE_KEY"] = k

        if save_to_env:
            try:
                env_path = Path(".env")
                if env_path.exists():
                    content = env_path.read_text(encoding="utf-8")
                    if "KAGGLE_USERNAME=" in content:
                        content = re.sub(r"KAGGLE_USERNAME=.*", f"KAGGLE_USERNAME={u}", content)
                    else:
                        content += f"\nKAGGLE_USERNAME={u}"
                    if "KAGGLE_KEY=" in content:
                        content = re.sub(r"KAGGLE_KEY=.*", f"KAGGLE_KEY={k}", content)
                    else:
                        content += f"\nKAGGLE_KEY={k}"
                    env_path.write_text(content, encoding="utf-8")
            except Exception:
                pass
        return

    # Se não foram passados diretamente, verifica se já existem configurados
    creds = kh_config.get_kaggle_credentials()
    if not creds:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Credenciais do Kaggle necessárias. Informe o seu Kaggle Username e Key "
                "(obtenha gratuitamente no Kaggle em https://www.kaggle.com/settings/api -> 'Create New Token')."
            ),
        )


def list_kaggle_dataset_files(
    dataset_handle: str,
    username: Optional[str] = None,
    key: Optional[str] = None,
) -> list[dict]:
    """
    Lista os arquivos contidos em um dataset do Kaggle sem precisar baixá-los todos.
    """
    from kagglehub.clients import build_kaggle_client
    from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetRequest

    handle, _ = parse_kaggle_handle(dataset_handle)
    parts = handle.split("/")
    if len(parts) != 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Handle do Kaggle inválido '{handle}'. O formato deve ser 'owner/dataset' ou a URL da página.",
        )

    setup_kaggle_credentials(username, key)

    try:
        with build_kaggle_client() as api_client:
            req = ApiGetDatasetRequest()
            req.owner_slug = parts[0]
            req.dataset_slug = parts[1]
            dataset_info = api_client.datasets.dataset_api_client.get_dataset(req)

            files_list = []
            if hasattr(dataset_info, "files") and dataset_info.files:
                for f in dataset_info.files:
                    total_bytes = getattr(f, "total_bytes", 0) or 0
                    files_list.append({
                        "name": f.name,
                        "size_bytes": total_bytes,
                        "file_type": getattr(f, "file_type", "") or Path(f.name).suffix,
                    })
            return files_list
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        if "403" in error_msg or "Forbidden" in error_msg or "Unauthenticated" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Acesso não autorizado pelo Kaggle. Verifique seu Username e API Key em https://www.kaggle.com/settings/api.",
            )
        if "404" in error_msg or "NotFound" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset do Kaggle '{handle}' não encontrado.",
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Erro ao consultar dataset no Kaggle: {error_msg}",
        )


def read_kaggle_file_to_dataframe(
    file_path: Path,
    max_rows: Optional[int] = 50000,
) -> dict[str, pd.DataFrame]:
    """
    Lê o ficheiro descarregado do Kaggle e devolve dicionário {nome_tabela: DataFrame}.
    Aplica max_rows para proteger a memória caso o arquivo seja massivo.
    """
    ext = file_path.suffix.lower()
    table_name = build_safe_table_name(file_path.name)
    dict_dfs: dict[str, pd.DataFrame] = {}

    limit = max_rows if (max_rows and max_rows > 0) else None

    try:
        if ext == ".csv":
            last_err = None
            for encoding in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
                try:
                    df = pd.read_csv(
                        file_path,
                        nrows=limit,
                        encoding=encoding,
                        low_memory=False,
                    )
                    dict_dfs[table_name] = df
                    break
                except Exception as e:
                    last_err = e
            if table_name not in dict_dfs:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Falha ao ler CSV do Kaggle: {str(last_err)}",
                )

        elif ext in {".parquet", ".pq"}:
            df = pd.read_parquet(file_path)
            if limit:
                df = df.head(limit)
            dict_dfs[table_name] = df

        elif ext == ".json":
            try:
                df = pd.read_json(file_path, nrows=limit)
            except TypeError:
                df = pd.read_json(file_path)
                if limit:
                    df = df.head(limit)
            dict_dfs[table_name] = df

        elif ext in {".xls", ".xlsx"}:
            raw_dict = pd.read_excel(file_path, sheet_name=None, nrows=limit)
            for sheet_name, df in raw_dict.items():
                safe_sheet = build_safe_table_name(f"{table_name}_{sheet_name}")
                dict_dfs[safe_sheet] = df

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Formato '{ext}' não suportado diretamente. Formatos aceitos: .csv, .parquet, .json, .xlsx.",
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Erro ao processar arquivo '{file_path.name}': {str(e)}",
        )

    # Limpar DataFrames
    cleaned_dict: dict[str, pd.DataFrame] = {}
    for t_name, df in dict_dfs.items():
        if df.empty:
            continue
        cleaned = clean_dataframe_for_sqlite(df)
        if not cleaned.empty and cleaned.shape[1] > 0:
            if cleaned.shape[1] > MAX_SQLITE_COLUMNS:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"A tabela {t_name} possui muitas colunas ({cleaned.shape[1]}). Limite é {MAX_SQLITE_COLUMNS}.",
                )
            cleaned_dict[t_name] = cleaned

    if not cleaned_dict:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nenhuma tabela com dados válidos pôde ser extraída do arquivo.",
        )

    return cleaned_dict


def download_kaggle_dataset(
    dataset_handle: str,
    file_path: Optional[str] = None,
    max_rows: Optional[int] = 50000,
    username: Optional[str] = None,
    key: Optional[str] = None,
    save_to_env: bool = False,
) -> tuple[dict[str, pd.DataFrame], str]:
    """
    Executa o download via kagglehub e carrega o DataFrame.
    Retorna: (dict_dfs, original_filename)
    """
    import kagglehub

    handle, inferred_file = parse_kaggle_handle(dataset_handle)
    target_file = (file_path or "").strip() or inferred_file

    setup_kaggle_credentials(username, key, save_to_env)

    try:
        if target_file:
            downloaded = kagglehub.dataset_download(handle, path=target_file)
        else:
            downloaded = kagglehub.dataset_download(handle)
    except Exception as e:
        err = str(e)
        if "403" in err or "Forbidden" in err:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Acesso recusado pelo Kaggle. Verifique suas credenciais em https://www.kaggle.com/settings/api.",
            )
        if "404" in err or "NotFound" in err:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset '{handle}' não encontrado no Kaggle.",
            )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Erro ao baixar dataset do Kaggle: {err}",
        )

    downloaded_path = Path(downloaded)

    # Se for uma pasta, procurar os arquivos válidos
    if downloaded_path.is_dir():
        files = [
            f for f in downloaded_path.iterdir()
            if f.is_file() and f.suffix.lower() in {".csv", ".parquet", ".pq", ".json", ".xls", ".xlsx"}
        ]
        if not files:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Nenhum arquivo tabular (.csv, .parquet, .json, .xlsx) encontrado no dataset '{handle}'.",
            )
        if len(files) == 1:
            file_to_read = files[0]
        else:
            names = [f.name for f in files]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"O dataset contém múltiplos arquivos ({', '.join(names[:6])}). "
                    f"Por favor, selecione ou informe o arquivo desejado no campo 'Arquivo específico'."
                ),
            )
    else:
        file_to_read = downloaded_path

    dict_dfs = read_kaggle_file_to_dataframe(file_to_read, max_rows=max_rows)
    return dict_dfs, file_to_read.name
