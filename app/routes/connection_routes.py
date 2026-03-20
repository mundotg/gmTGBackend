from __future__ import annotations

import json
import os
import traceback
from typing import Any, Optional, cast
import pandas as pd
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    Form,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from sqlalchemy.orm import Session

from app.config.cache_manager import cache_result
from app.config.dependencies import get_session_by_connection
from app.config.engine_manager_cache import EngineManager
from app.cruds.connection_cruds import (
    create_connection_log,
    create_db_connection,
    delete_connection,
    desactivate_all_connections,
    disconnect_active_connection,
    get_active_connection_by_connid,
    get_active_connection_by_userid,
    get_connection_logs,
    get_connection_logs_pagination,
    get_db_connection_by_id,
    get_db_connections,
    get_db_connections_pagination_v1,
    map_status,
    query_connections_simple,
    set_active_connection,
    upsert_db_connection,
)
from app.database import get_db
from app.models.connection_models import DBConnection
from app.schemas.connetion_schema import (
    ConnectionPaginationOutput,
    ConnectionPassUserOut,
    ConnectionRequest,
    DBConnectionBase,
    DbConnectionOutput,
    SavedConnectionBase,
)
from app.services.crypto_utils import aes_encrypt
from app.services.dataset_service import (
    build_safe_table_name,
    read_dataframe,
    read_dataset_source,
    save_dataframe_to_sqlite,
)
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(prefix="/conn", tags=["connections"])


# =========================================================
# Cache
# =========================================================

@cache_result(ttl=300, user_id="user_{user_id}")
def get_db_connections_cached(db: Session, user_id: int):
    return get_db_connections(db, user_id)


@cache_result(ttl=300, user_id="user_{user_id}")
def get_db_connections_pagination_cached(db: Session, user_id: int, page: int, limit: int):
    return get_db_connections_pagination_v1(db, user_id, page, limit)


@cache_result(ttl=600, user_id="user_{user_id}")
def get_connection_logs_cached(db: Session, user_id: int):
    return get_connection_logs(db, user_id)


@cache_result(ttl=300, user_id="user_{user_id}")
def get_connection_logs_pagination_cached(
    db: Session,
    user_id: int,
    connection_id: Optional[int],
    page: int,
    limit: int,
):
    return get_connection_logs_pagination(db, user_id, connection_id or 0, page, limit)


@cache_result(ttl=1800, user_id="user_{user_id}")
def get_db_connection_by_id_cached(db: Session, conn_id: int) -> DBConnection | None:
    return get_db_connection_by_id(db, conn_id)


# =========================================================
# Helpers
# =========================================================

def _log_and_raise(
    *,
    db: Session,
    user_id: int,
    action: str,
    message: str,
    status_code: int,
    connection_id: Optional[int] = None,
    details: Optional[dict[str, Any]] = None,
    level: str = "error",
) -> None:
    """
    Padroniza log técnico + log de conexão + exceção HTTP.
    """
    log_message(f'{message} : {details}', level=level)

    try:
        create_connection_log(
            db,
            connection_id=connection_id,
            action=action,
            status="error" if status_code >= 400 else "success",
            details=details or {"message": message},
            user_id=user_id,
        )
    except Exception as log_error:
        log_message(
            f"⚠️ Falha ao registrar log de conexão: {str(log_error)}{details}",
            level="warning",
        )

    raise HTTPException(status_code=status_code, detail=message)


def _cleanup_engine(user_id: int) -> None:
    """
    Descarta engine ativo do usuário, se existir.
    """
    try:
        current_engine = EngineManager.get(user_id)
        if current_engine:
            current_engine.dispose()
            EngineManager.remove(user_id)
            log_message(f"🔁 Engine do usuário {user_id} descartado com sucesso.", level="info")
    except ValueError:
        log_message(f"ℹ️ Nenhum engine ativo para o usuário {user_id}.", level="info")
    except Exception as e:
        log_message(f"⚠️ Erro ao limpar engine do usuário {user_id}: {str(e)}", level="warning")


def _validate_connection_owner(conn: Optional[DBConnection], conn_id: int) -> DBConnection:
    """
    Garante que a conexão existe.
    """
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Conexão com ID {conn_id} não encontrada.",
        )
    return conn


def _build_connection_output(connection_id: int, message: str, connected: bool) -> DbConnectionOutput:
    return DbConnectionOutput(
        connection_id=connection_id,
        message=message,
        connect=connected,
    )


# =========================================================
# Endpoints de conexão
# =========================================================

@router.post("/salvarconnections/", response_model=SavedConnectionBase)
def save_connection(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Salva uma nova conexão de banco de dados.
    """
    try:
        saved_conn = create_db_connection(db, user_id, conn_data)

        create_connection_log(
            db,
            connection_id=cast(int,saved_conn.id) or 0,
            action="Conexão salva",
            status="success",
            details={
                "host": conn_data.host,
                "database": conn_data.database_name,
                "type": conn_data.type,
            },
            user_id=user_id,
        )

        return saved_conn

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao salvar conexão",
            message="Erro ao salvar conexão.",
            status_code=status.HTTP_400_BAD_REQUEST,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.post("/connect/", response_model=DbConnectionOutput)
def test_and_connect(
    request: ConnectionRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Testa a conexão, salva ou atualiza e define como ativa.
    """
    db_conn = None

    try:
        conn_data = request.conn_data
        action_type = request.tipo

        _cleanup_engine(user_id)

        engine = get_session_by_connection(conn_data)
        if not engine:
            raise ValueError("Engine não foi criado corretamente.")

        EngineManager.set(engine, user_id)
        conn_data.status = "connected"

        if action_type == "con":
            db_conn = create_db_connection(db, user_id, conn_data)
        elif action_type == "upsert":
            db_conn = upsert_db_connection(db, user_id, conn_data)
        else:
            raise ValueError("Tipo de operação inválido. Use 'con' ou 'upsert'.")

        desactivate_all_connections(db, user_id)
        set_active_connection(db, user_id, cast(int, db_conn.id))

        create_connection_log(
            db,
            connection_id=cast(int, db_conn.id),
            action="Conexão testada e ativada",
            status="success",
            details={
                "host": conn_data.host,
                "database": conn_data.database_name,
                "type": conn_data.type,
            },
            user_id=user_id,
        )

        return _build_connection_output(
            connection_id=cast(int, db_conn.id),
            message="✅ Conexão testada, salva e ativada com sucesso.",
            connected=True,
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Tentativa de conexão falhou",
            message="Falha ao conectar ou salvar a conexão. Verifique os dados e tente novamente.",
            status_code=status.HTTP_400_BAD_REQUEST,
            connection_id=cast(int, db_conn.id) if db_conn else None,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.put("/connect-toggle/", response_model=DbConnectionOutput)
def connect_or_disconnect(
    conn_id: int = Body(..., embed=True),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Alterna entre conectar e desconectar uma conexão salva.
    """
    try:
        active_conn = get_active_connection_by_connid(db, conn_id)

        if active_conn and active_conn["status"]:
            _cleanup_engine(user_id)
            disconnect_active_connection(db, active_conn["connection_id"])

            conn_data = _validate_connection_owner(
                get_db_connection_by_id(db, active_conn["connection_id"]),
                active_conn["connection_id"],
            )

            # CORRIGIDO: acessando como atributo, não como dicionário
            create_connection_log(
                db,
                connection_id=conn_data.id,
                action="Desconexão manual",
                status="disconnected",
                details={
                    "database": conn_data.database_name,
                    "type": conn_data.type,
                },
                user_id=user_id,
            )

            return _build_connection_output(
                connection_id=conn_data.id,
                message="🔌 Desconectado com sucesso.",
                connected=False,
            )

        conn_data = _validate_connection_owner(get_db_connection_by_id(db, conn_id), conn_id)

        desactivate_all_connections(db, user_id)

        engine = get_session_by_connection(conn_data)
        if not engine:
            raise ValueError("Engine não foi criada corretamente.")

        EngineManager.set(engine, user_id)
        # CORRIGIDO: acessando como atributo, não como dicionário
        set_active_connection(db, user_id, conn_data.id)

        # CORRIGIDO: acessando como atributo, não como dicionário
        create_connection_log(
            db,
            connection_id=conn_data.id,
            action="Conexão ativada",
            status="success",
            details={
                "database": conn_data.database_name,
                "type": conn_data.type,
            },
            user_id=user_id,
        )

        return _build_connection_output(
            connection_id=conn_data.id,
            message="✅ Conectado com sucesso.",
            connected=True,
        )

    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao alternar conexão",
            message="Erro ao conectar ou desconectar a conexão.",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            connection_id=conn_id,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.get("/connections/", response_model=ConnectionPaginationOutput)
def list_connections_paginated(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
):
    """
    Lista conexões salvas com paginação.
    """
    try:
        connections = get_db_connections_pagination_cached(db, user_id, page, limit)
        active_conn = get_active_connection_by_userid(db, user_id)
        active_conn_id = cast(int,active_conn.connection_id )if active_conn else None

        return ConnectionPaginationOutput(
            page=connections["page"],
            limit=connections["limit"],
            total=connections["total"],
            results=[
                SavedConnectionBase.model_validate(
                    {
                        "id": conn.id,
                        "name": conn.name,
                        "host": conn.host,
                        "database": conn.database_name,
                        "last_used": last_used,
                        "type": conn.type,
                        "status": map_status(conn.status, conn.id, active_conn_id),
                    }
                )
                for conn, last_used in connections["results"]
            ],
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao listar conexões",
            message=f"Erro interno ao listar conexões. {str(e)}{traceback.print_exc()}",
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            details={"error": str(e)},
        )


@router.delete("/delete_connection/{conn_id}", response_model=DbConnectionOutput)
def delete_connection_save(
    conn_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Deleta uma conexão salva.
    """
    try:
        conn = delete_connection(db, conn_id)
        if conn:
            # CORRIGIDO: aspas simples para evitar problemas com string interpolation
            create_connection_log(
                db,
                connection_id=conn_id,
                action=f"Conexão deletada: {conn.name}",
                status="success",
                details={
                    "database": conn.database_name,
                    "type": conn.type,
                },
                user_id=user_id,
            )

        _cleanup_engine(user_id)

        return _build_connection_output(
            connection_id=conn_id,
            message="Conexão deletada com sucesso.",
            connected=False,
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action=f"Erro ao deletar conexão {conn_id}",
            message="Erro ao deletar a conexão.",
            status_code=status.HTTP_400_BAD_REQUEST,
            connection_id=conn_id,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.get("/get_credencial_db/{conn_id}", response_model=ConnectionPassUserOut)
def get_credenciais(
    conn_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Obtém credenciais de uma conexão específica.
    """
    try:
        conn = get_db_connection_by_id_cached(db, conn_id)
        conn = _validate_connection_owner(conn, conn_id)

        # CORRIGIDO: acessando como atributo, não como dicionário
        return ConnectionPassUserOut(
            password=conn.password,
            username=conn.username,
            service=conn.service,
            sslmode=conn.sslmode,
            trustServerCertificate=conn.trustServerCertificate,
        )

    except HTTPException:
        raise
    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Erro ao obter credenciais",
            message="Erro ao obter credenciais da conexão.",
            status_code=status.HTTP_404_NOT_FOUND,
            connection_id=conn_id,
            details={"error": str(e)},
        )


@router.post("/testconnections/", response_model=DbConnectionOutput)
def test_connection(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Apenas testa uma conexão sem ativá-la.
    """
    temp_conn = None

    try:
        temp_conn = create_db_connection(db, user_id, conn_data)
        engine = get_session_by_connection(conn_data)

        if not engine:
            raise ValueError("Engine não foi criado corretamente.")

        try:
            engine.dispose()
        except Exception:
            pass

        create_connection_log(
            db,
            connection_id=temp_conn.id,
            action="Teste de conexão",
            status="success",
            details={
                "database": conn_data.database_name,
                "type": conn_data.type,
            },
            user_id=user_id,
        )

        return _build_connection_output(
            connection_id=temp_conn.id,
            message="Conexão testada com sucesso.",
            connected=True,
        )

    except Exception as e:
        _log_and_raise(
            db=db,
            user_id=user_id,
            action="Teste de conexão falhou",
            message="Erro ao testar conexão.",
            status_code=status.HTTP_400_BAD_REQUEST,
            connection_id=temp_conn.id if temp_conn else None,
            details={"error": str(e), "trace": traceback.format_exc()},
        )


@router.get("/paginate")
def listar_elementos_connections(
    search: str | None = Query(None, description="Texto para pesquisa"),
    filtro: str | None = Query(None, description="Filtro opcional em formato JSON"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Paginação genérica de conexões.
    """
    filters = None

    if filtro:
        try:
            filters = json.loads(filtro) or {}
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Formato inválido de filtro JSON.",
            )

    return query_connections_simple(
        db,
        user_id=user_id,
        search=search,
        page=page,
        limit=limit,
        filters=filters,
    )


# =========================================================
# Endpoint de dataset
# =========================================================
@router.post("/dataset/open")
async def open_dataset(
    file: Optional[UploadFile] = File(None, description="Arquivo para upload"),
    url: Optional[str] = Form(None, description="URL pública do dataset"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """
    Abre um dataset (que pode conter múltiplas tabelas) por upload ou URL pública,
    converte para um único ficheiro SQLite e regista como conexão do utilizador.
    """
    conn = None
    db_path: Optional[str] = None
    filename = "dataset"

    try:
        # 1. Lê os bytes do ficheiro/url
        contents, filename, source_type = await read_dataset_source(file=file, url=url)

        # 2. Transforma num dicionário de DataFrames (suporta múltiplas abas/tabelas)
        dict_dfs = read_dataframe(contents, filename)

        # 3. Guarda tudo no SQLite e obtém o caminho e as tabelas criadas
        db_path, tabelas_criadas = save_dataframe_to_sqlite(
            dict_dfs=dict_dfs,
            user_id=user_id,
            original_filename=filename,
        )

        # 4. Registar a conexão na base de dados principal (o MustaInf)
        # Usamos o nome da primeira tabela como database_name principal para referência, 
        # mas o SQLite tem todas lá dentro.
        main_table_name = tabelas_criadas[0] if tabelas_criadas else "main_table"

        conn_data = DBConnectionBase(
            name=f"Dataset: {filename}",
            type="SQLite",
            host=aes_encrypt(db_path),   # caminho absoluto do ficheiro SQLite
            port=0,
            username="",
            password="",
            database_name=main_table_name,
            status="available",
        )

        conn = create_db_connection(db, user_id, conn_data)

        # 5. Prepara os metadados de resposta para todas as tabelas
        # Calcula o total de linhas somando todas as tabelas
        total_rows_all_tables = sum(len(df) for df in dict_dfs.values())
        
        # Cria um resumo com as informações de cada tabela extraída
        tabelas_info = []
        for tab_name in tabelas_criadas:
            df = dict_dfs[tab_name]
            tabelas_info.append({
                "table_name": tab_name,
                "rows": int(len(df)),
                "columns": int(len(df.columns)),
                "column_names": list(df.columns),
                # Preview apenas das primeiras 3 linhas de cada tabela para não sobrecarregar o JSON
                "preview": df.head(3).where(pd.notna(df.head(3)), None).to_dict(orient="records")
            })

        log_message(
            f"📄 Dataset '{filename}' importado pelo usuário {user_id}. "
            f"Fonte: {source_type}. Tabelas: {len(tabelas_criadas)}. Linhas Totais: {total_rows_all_tables}.",
            level="info",
        )

        create_connection_log(
            db,
            connection_id=conn.id,
            action="Dataset importado e convertido para SQLite",
            status="success",
            details={
                "source": source_type,
                "host": db_path,
                "tables_created": tabelas_criadas,
                "total_rows_processed": total_rows_all_tables,
            },
            user_id=user_id,
        )

        return {
            "success": True,
            "source": source_type,
            "filename": filename,
            "connection_id": conn.id,
            "tables": tabelas_info, # Array com info detalhada de cada tabela importada
            "total_tables_extracted": len(tabelas_criadas),
            "message": f"Dataset carregado com sucesso. {len(tabelas_criadas)} tabela(s) criada(s).",
        }

    except HTTPException as e:
        log_message(
            f"⚠️ Falha ao abrir dataset '{filename}' para o usuário {user_id}: {e.detail}",
            level="warning",
        )

        if conn:
            create_connection_log(
                db,
                connection_id=conn.id,
                action="Falha ao importar dataset",
                status="error",
                details={
                    "error": e.detail,
                    "filename": filename,
                },
                user_id=user_id,
            )

        if db_path and os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass

        raise

    except Exception as e:
        log_message(
            f"❌ Erro interno ao abrir dataset '{filename}' para o usuário {user_id}: "
            f"{str(e)}\n{traceback.format_exc()}",
            level="error",
        )

        if conn:
            create_connection_log(
                db,
                connection_id=cast(int,conn.id),
                action="Erro crítico no processamento do dataset",
                status="error",
                details={
                    "error": str(e),
                    "filename": filename,
                },
                user_id=user_id,
            )

        if db_path and os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno ao processar o dataset.",
        )