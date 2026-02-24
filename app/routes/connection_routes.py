import json
import traceback
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import  Optional
from app.config.cache_manager import cache_result
from app.config.dependencies import EngineManager, get_session_by_connection
from app.cruds.connection_cruds import (
     create_connection_log, create_db_connection, 
    desactivate_all_connections, delete_connection, disconnect_active_connection, 
    get_active_connection_by_connid, get_active_connection_by_userid, 
    get_connection_logs, get_connection_logs_pagination, 
    get_db_connection_by_id, get_db_connections, 
    get_db_connections_pagination_v1, map_status, query_connections_simple, set_active_connection, upsert_db_connection
)
from app.database import get_db
from app.schemas.connetion_schema import (
    ConnectionPaginationOutput, ConnectionPassUserOut, ConnectionRequest, DBConnectionBase, DbConnectionOutput, SavedConnectionBase
)
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message

router = APIRouter(prefix="/conn", tags=["connections"])

# -----------------------------
# Funções com Cache
# -----------------------------

@cache_result(ttl=300, user_id="user_{user_id}")
def get_db_connections_cached(db: Session, user_id: int):
    """Obtém conexões do usuário com cache"""
    return get_db_connections(db, user_id)

@cache_result(ttl=300, user_id="user_{user_id}")
def get_db_connections_pagination_cached(db: Session, user_id: int, page: int, limit: int):
    """Obtém conexões paginadas com cache"""
    return get_db_connections_pagination_v1(db, user_id, page, limit)

@cache_result(ttl=600, user_id="user_{user_id}")
def get_connection_logs_cached(db: Session, user_id: int):
    """Obtém logs de conexão com cache"""
    return get_connection_logs(db, user_id)

@cache_result(ttl=300, user_id="user_{user_id}")
def get_connection_logs_pagination_cached(db: Session, user_id: int, connection_id: Optional[int], page: int, limit: int):
    """Obtém logs paginados com cache"""
    return get_connection_logs_pagination(db, user_id, connection_id, page, limit)

@cache_result(ttl=1800, user_id="user_{user_id}")
def get_db_connection_by_id_cached(db: Session, conn_id: int):
    """Obtém conexão por ID com cache"""
    return get_db_connection_by_id(db, conn_id)

# -----------------------------
# Utilitários
# -----------------------------

def _handle_engine_cleanup(user_id: int):
    """Limpa engine antigo do usuário"""
    try:
        old_engine = EngineManager.get(user_id)
        if old_engine:
            old_engine.dispose()
            log_message(f"🔁 Engine antigo do usuário {user_id} descartado.", level="info")
    except ValueError as e:
        log_message(f"ℹ️ Nenhum engine ativo anterior para o usuário {user_id}. {str(e)}", level="info")
    except Exception as e:
        log_message(f"⚠️ Erro ao tentar descartar engine antigo: {str(e)}", level="warning")

# -----------------------------
# Endpoints
# -----------------------------

@router.post("/salvarconnections/", response_model=SavedConnectionBase)
def save_connection(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """Salva uma nova conexão de banco de dados."""
    try:
        result = create_db_connection(db, user_id, conn_data)
        create_connection_log(
            db,
            connection_id=result.id,
            action="Conexão testada e salva",
            status="success",
            details={"host": conn_data.host, "database": conn_data.database_name, "type": conn_data.type},
            user_id=user_id
        )
        return result
    except Exception as e:
        log_message(f"❌ Erro ao salvar conexão: {str(e)}\n{traceback.format_exc()}", level="error")
        create_connection_log(
            db,
            connection_id=None,
            action="Erro ao salvar conexão",
            status="error",
            details={"error": str(e), "trace": traceback.format_exc()},
            user_id=user_id
        )
        raise HTTPException(status_code=400, detail="Erro ao salvar conexão.")

@router.post("/connect/", response_model=DbConnectionOutput)
def test_and_connect(
    request: ConnectionRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
   
    """Testa e conecta a uma nova conexão de banco de dados."""
    conn_data = request.conn_data
    tipo = request.tipo
    db_conn = None
    try:
        _handle_engine_cleanup(user_id)
        engine = get_session_by_connection(conn_data)
        if not engine:
            raise ValueError("Engine não foi criado corretamente.")
        EngineManager.set(engine, user_id)
        conn_data.status = "connected"
        # print("trustServerCertificate: ", conn_data , "   tipo:",tipo)
        if tipo == "con":
            db_conn = create_db_connection(db, user_id, conn_data)
        elif tipo == "upsert":
            db_conn = upsert_db_connection(db, user_id, conn_data)
        
        desactivate_all_connections(db, user_id)
        set_active_connection(db, user_id, db_conn.id)
        create_connection_log(
            db,
            connection_id=db_conn.id,
            action="Conexão testada e salva",
            status="success",
            details={"host": conn_data.host, "database": conn_data.database_name, "type": conn_data.type},
            user_id=user_id
        )
        return DbConnectionOutput(connection_id=db_conn.id, message="✅ Conexão testada e salva com sucesso!", connect=True)
    except Exception as e:
        log_message(f"❌ Erro ao conectar ou salvar: {str(e)}\n{traceback.format_exc()}", level="error")
        create_connection_log(
            db,
            connection_id=db_conn.id if db_conn else None,
            action="Tentativa de conexão falhou",
            status="error",
            details={"error": str(e), "trace": traceback.format_exc()},
            user_id=user_id
        )
        raise HTTPException(status_code=400, detail="❌ Falha ao conectar ou salvar a conexão. Verifique os dados e tente novamente.")

@router.put("/connect-toggle/", response_model=DbConnectionOutput)
def connect_or_disconnect(
    conn_id: int = Body(..., embed=True),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """Alterna entre conectar e desconectar uma conexão."""
    try:
        active_conn = get_active_connection_by_connid(db, conn_id)
        if active_conn and active_conn.status:
            current_engine = EngineManager.get(user_id)
            if current_engine:
                current_engine.dispose()
                EngineManager.remove(user_id)
            disconnect_active_connection(db, active_conn.connection_id)
            conn_data = get_db_connection_by_id(db, active_conn.connection_id)
            conn_data.status = "disconnected"
            db_conn = create_db_connection(db, user_id, conn_data)
            create_connection_log(
                db,
                connection_id=db_conn.id,
                action="Desconexão manual",
                status="disconnected",
                details={"database": conn_data.database_name, "type": conn_data.type},
                user_id=user_id
            )
            return DbConnectionOutput(connection_id=db_conn.id, message="🔌 Desconectado com sucesso.", connect=False)
        else:
            desactivate_all_connections(db, user_id)
            conn_data = get_db_connection_by_id(db, conn_id)
            engine = get_session_by_connection(conn_data)
            if not engine:
                raise ValueError("Engine não foi criada corretamente.")
            EngineManager.set(engine, user_id)
            conn_data.status = "connected"
            db_conn = create_db_connection(db, user_id, conn_data)
            set_active_connection(db, user_id, db_conn.id)
            create_connection_log(
                db,
                connection_id=db_conn.id,
                action="Conexão ativada",
                status="success",
                details={"database": conn_data.database_name, "type": conn_data.type},
                user_id=user_id
            )
            return DbConnectionOutput(connection_id=db_conn.id, message="✅ Conectado com sucesso!", connect=True)
    except Exception as e:
        log_message(f"❌ Erro ao alternar conexão: {str(e)}\n{traceback.format_exc()}", level="error")
        create_connection_log(
            db,
            connection_id=conn_id,
            action="Erro ao alternar conexão",
            status="error",
            details={"error": str(e)},
            user_id=user_id
        )
        raise HTTPException(status_code=500, detail="Erro ao conectar ou desconectar.")
    
@router.get("/connections/", response_model=ConnectionPaginationOutput)
def list_connections_paginated(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
    page: int = Query(1, ge=1),
    limit: int = Query(10, le=100)
):
    """
    Lista conexões salvas com paginação.
    Cache: 5 minutos
    """
    try:
        connections = get_db_connections_pagination_cached(db, user_id, page, limit)
        active_conn = get_active_connection_by_userid(db, user_id)
        active_conn_id = active_conn.connection_id if active_conn else None 
        
        return ConnectionPaginationOutput(
            limit=connections["limit"],
            page=connections["page"],
            total=connections["total"],
            results=[
                SavedConnectionBase.model_validate({
                    "id": conn.id,
                    "name": conn.name,
                    "host": conn.host,
                    "database": conn.database_name,
                    "last_used": last_used,
                    "type": conn.type,
                    "status": map_status(conn.status, conn.id, active_conn_id)
                })
                for conn, last_used in connections["results"] 
            ]
        )
    except Exception as e:
        log_message(f"❌ Erro ao listar conexões paginadas: {str(e)}", level="error")
        raise HTTPException(status_code=500, detail="Erro interno ao listar conexões.")

@router.delete("/delete_connection/{conn_id}", response_model=DbConnectionOutput)
def delete_connection_save(
    conn_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    """Deleta uma conexão salva."""
    conn = None
    try:
        conn = delete_connection(db, conn_id)
        create_connection_log(
            db,
            connection_id=None,
            action=f"Conexão deletada {conn.name}",
            status="success",
            details={"database": conn.database_name, "type": conn.type},
            user_id=user_id
        )
        _handle_engine_cleanup(user_id)
        return DbConnectionOutput(connection_id=conn_id, message="Conexão deletada com sucesso!", connect=False)
    except Exception as e:
        log_message(f"❌ Erro ao deletar conexão: {str(e)}{traceback.format_exc()}", level="error")
        create_connection_log(
            db,
            connection_id=None,
            action=f"Erro ao deletar conexão {conn_id}",
            status="error",
            details={"error": str(e)},
            user_id=user_id
        )
        raise HTTPException(status_code=400, detail="Erro ao deletar a conexão.")
    
@router.get('/get_credencial_db/{conn_id}', response_model=ConnectionPassUserOut)
def get_credenciais(
    conn_id: int, 
    db: Session = Depends(get_db), 
    user_id: int = Depends(get_current_user_id)
):
    """
    Obtém credenciais de uma conexão específica.
    Cache: 30 minutos (dados sensíveis)
    """
    try:
        conn = get_db_connection_by_id_cached(db, conn_id)
        return ConnectionPassUserOut(
            password=conn.password, 
            username=conn.username, 
            service=conn.service,
            sslmode=conn.sslmode,
            trustServerCertificate=conn.trustServerCertificate
        )
    except Exception as e:
        log_message(f"❌ Erro ao obter credenciais: {str(e)}", level="error")
        raise HTTPException(status_code=404, detail="Conexão não encontrada.")

@router.post("/testconnections/{conn_id}", response_model=DbConnectionOutput)
def test_connection_by_id(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """Testa uma conexão específica por ID."""
    conn = create_db_connection(db, user_id, conn_data)
    try:
        engine = get_session_by_connection(conn_data, db)
        engine.close()
        create_connection_log(
            db,
            connection_id=conn.id,
            action="Teste de conexão",
            status="success",
            details={"database": conn_data.database_name, "type": conn_data.type},
            user_id=user_id
        )
    except Exception as e:
        log_message(f"❌ Erro ao testar conexão: {str(e)}", level="error")
        create_connection_log(
            db,
            connection_id=conn.id,
            action="Teste de conexão falhou",
            status="error",
            details={"error": str(e)},
            user_id=user_id
        )
        raise HTTPException(status_code=400, detail=f"Erro ao testar conexão: {str(e)}")
    return DbConnectionOutput(connect=True, message="Conexão testada com sucesso!")


@router.get("/paginate")
def listar_elementos_conections(
    search: str | None = Query(None, description="Texto para pesquisa"),
    filtro: str | None = Query(None, description="Filtro opcional em formato JSON"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user_id)
):
    """Paginação genérica de entidades."""
    filters = None
    if filtro:
        try:
            filters = json.loads(filtro)
            if filters is None:
                filters = {}
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Formato inválido de filtro JSON.")
    return query_connections_simple(
        db,
        user_id=int(user),
        search=search,
        page=page,
        limit=limit,
        filters=filters
    )