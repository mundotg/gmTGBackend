import traceback
from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.params import Query
from sqlalchemy.orm import Session
from typing import List, Optional

from app.config.dependencies import EngineManager, get_session_by_connection
from app.cruds.connection_cruds import ( connect_active_connection, create_connection_log, create_db_connection, desactivate_all_connections, delete_connection, disconnect_active_connection, get_active_connection_by_connid, get_active_connection_by_userid, 
                                        get_connection_logs, get_connection_logs_pagination, 
                                        get_db_connection_by_id, get_db_connection_by_name, get_db_connections, get_db_connections_pagination,
                                        map_status, set_active_connection)
from app.database import get_db
from app.schemas.connetion_schema import (ConnectionLogBase, ConnectionLogPaginationOutput, 
                                          ConnectionPaginationOutput, ConnectionPassUserOut,
                                          DBConnectionBase, DbConnectionOutput, SavedConnectionBase)
from app.ultils.get_id_by_token import get_current_user_id
from app.ultils.logger import log_message


router = APIRouter(prefix="/conn", tags=["connections"])

# === Salvar nova conexão ===
@router.post("/salvarconnections/", response_model=SavedConnectionBase)
def save_connection(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    return create_db_connection(db, user_id, conn_data)

@router.post("/connect/", response_model=DbConnectionOutput)
def test_and_connect(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    db_conn = None  # Inicializa fora do try para uso no except
    try:
        # 1. Fecha engine antigo, se houver
        try:
            old_engine = EngineManager.get(user_id)
            if old_engine:
                old_engine.dispose()
                log_message(f"🔁 Engine antigo do usuário {user_id} descartado.", level="info")
        except ValueError as e:
            log_message(f"ℹ️ Nenhum engine ativo anterior para o usuário {user_id}. {str(e)} traceback: {traceback.format_exc()}", level="info")
        except Exception as e:
            log_message(f"⚠️ Erro ao tentar descartar engine antigo: {str(e)} traceback: {traceback.format_exc()}", level="warning")
        # 2. Testa nova conexão
        engine = get_session_by_connection(conn_data)
        if not engine:
            raise ValueError("Engine não foi criado corretamente.")
        EngineManager.set(engine, user_id)
        log_message(f"✅ Novo engine configurado com sucesso para o usuário {user_id}.", level="info")
        conn_data.status = "connected"
        # 3. Cria ou atualiza conexão no banco
        db_conn = create_db_connection(db, user_id, conn_data)

        desactivate_all_connections(db, user_id)  # Desativa todas as conexões ativas do usuário
        # 4. Define como conexão ativa
        set_active_connection(db, user_id, db_conn.id)

        # 5. Log de sucesso
        create_connection_log(
            db,
            connection_id=db_conn.id,
            action="Conexão testada e salva",
            status="success"
        )

        return DbConnectionOutput(
            connection_id=db_conn.id,
            message="✅ Conexão testada e salva com sucesso!",
            connect=True
        )

    except HTTPException as http_exc:
        # Repassa exceções HTTP diretamente
        raise http_exc

    except Exception as e:
        log_message(f"❌ Erro ao conectar ou salvar: {str(e)}\n{traceback.format_exc()}", level="error")

        # Tenta recuperar conexão para log de erro
        if not db_conn:
            db_conn = get_db_connection_by_name(db, conn_data.database_name)

        create_connection_log(
            db,
            connection_id=db_conn.id if db_conn else None,
            action="Tentativa de conexão falhou",
            status="error",
            # details=str(e)
        )

        raise HTTPException(
            status_code=400,
            detail=(
                "❌ Falha ao conectar ou salvar a conexão. "
                "Verifique os dados (host, porta, credenciais e tipo de banco) e tente novamente."
            )
        )
        
@router.put("/connect-toggle/", response_model=DbConnectionOutput)
def connect_or_disconnect(
    conn_id: int = Body(..., embed=True),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    try:
        # log_message(f"➡️ Iniciando toggle de conexão. conn_id={conn_id}, user_id={user_id}")
        # Verifica se já existe
        ative_conn = get_active_connection_by_connid(db, conn_id)
        log_message(f"🔍 Conexão ativa encontrada: {ative_conn is not None}, status={getattr(ative_conn, 'status', None)}")

        if ative_conn and ative_conn.status:
            # log_message("🔌 Conexão ativa detectada. Iniciando desconexão...")
            current_engine = EngineManager.get(user_id)
            if current_engine:
                # log_message("♻️ Fechando engine atual...")
                current_engine.dispose()
                EngineManager.remove(user_id)
            else:
                log_message("⚠️ Nenhum engine encontrado para este usuário.")
            disconnect_active_connection(db, ative_conn.connection_id)
            # log_message("✅ Conexão desconectada na base.")
            conn_data: DBConnectionBase = get_db_connection_by_id(db, ative_conn.connection_id)
            conn_data.status = "disconnected"
            db_conn = create_db_connection(db, user_id, conn_data)

            create_connection_log(
                db,
                connection_id=db_conn.id if db_conn else None,
                action="Desconexão manual",
                status="disconnected"
            )

            # log_message(f"✅ Desconexão finalizada com sucesso. conn_id={conn_id}")
            return DbConnectionOutput(
                connection_id=db_conn.id if db_conn else None,
                message="🔌 Desconectado com sucesso.",
                connect=False
            )
        else:
            log_message("⚡ Nenhuma conexão ativa detectada. Iniciando conexão...")
            desactivate_all_connections(db, user_id)
            conn_data: DBConnectionBase = get_db_connection_by_id(db, conn_id)
            engine = get_session_by_connection(conn_data)

            if not engine:
                log_message("❌ Erro: Engine não foi criada corretamente.")
                raise ValueError("Engine não foi criada corretamente.")

            EngineManager.set(engine, user_id)
            conn_data.status = "connected"
            db_conn = create_db_connection(db, user_id, conn_data)
            act = connect_active_connection(db,db_conn.id)
            if not act:
                act = set_active_connection(db, user_id, db_conn.id)
            create_connection_log(
                db, connection_id=db_conn.id,
                action="Conexão ativada",  
                status="success"
            )
            # log_message(f"✅ Conexão finalizada com sucesso. conn_id={conn_id}")
            return DbConnectionOutput(
                connection_id=db_conn.id,
                message="✅ Conectado com sucesso!",  connect=True
            )

    except Exception as e:
        log_message(f"❌ Erro ao alternar conexão: {str(e)}\n{traceback.format_exc()}", level="error")
        raise HTTPException(
            status_code=500,
            detail="Erro ao conectar ou desconectar. Verifique os dados e tente novamente."
        )


# === lista conexão  ===
@router.get("/connection_saved/", response_model=List[SavedConnectionBase])
def list_connections(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    connections = get_db_connections(db, user_id)
    ative_conn = get_active_connection_by_userid(db,user_id)
    ative_conn = ative_conn.connection_id if ative_conn else None 
    lista_convertida = [
        SavedConnectionBase.model_validate({
                "id": conn.id,
                "name": conn.name,
                "host": conn.host,
                "database": conn.database_name,  # verifique se esse nome está certo
                "type": conn.type,
                "status": map_status(conn.status,conn.id,ative_conn.connection_id )
            }) for conn in connections
    ]

    return lista_convertida

@router.get("/connections/", response_model=ConnectionPaginationOutput)
def list_connections(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
    page: int = Query(1, ge=1),
    limit: int = Query(10, le=100)
):
    connections = get_db_connections_pagination(db, user_id, page, limit)
    ative_conn = get_active_connection_by_userid(db,user_id)
    ative_conn = ative_conn.connection_id if ative_conn else None 
    lista_convertida = ConnectionPaginationOutput(
        limit=connections["limit"],
        page=connections["page"],
        total=connections["total"],
        results=[
            SavedConnectionBase.model_validate({
                "id": conn.id,
                "name": conn.name,
                "host": conn.host,
                "database": conn.database_name,  # ou conn.database_name, conforme seu modelo real
                "last_used": last_used,
                "type": conn.type,
                "status": map_status(conn.status,conn.id,ative_conn )  # transforma 'available' etc.
            })
            for conn, last_used in connections["results"] 
        ]
    )

    return lista_convertida


@router.delete("/delete_connection/{conn_id}", response_model=DbConnectionOutput)
def delete_connection_save(
    conn_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    conn = None  # Garante que conn existe mesmo se falhar
    try:
        # Deleta a conexão
        conn = delete_connection(db, conn_id)
        # Cria log de sucesso
        create_connection_log(
            db,
            connection_id=None,
            action="Conexão deletada "+conn.name, 
            status="success"
        )

        return DbConnectionOutput(
            connection_id=conn_id,
            message="Conexão deletada com sucesso!", connect=False
        )
    except Exception as e:
        log_message(f"❌ Erro ao deletar conexão: {str(e)}{traceback.format_exc()}", level="error")

        create_connection_log(
            db,
            connection_id=None ,
            action="Erro ao deletar conexão "+conn_id, 
            status="error"
        )

        raise HTTPException(
            status_code=400, detail="Erro ao deletar a conexão."
        )
        
@router.get('/get_credencial_db/{conn_id}',response_model=ConnectionPassUserOut)
def get_credenciais(conn_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    # lógica para buscar dados no banco
    conn = get_db_connection_by_id(db,conn_id)
    return ConnectionPassUserOut(password=conn.password, username=conn.username, service=conn.service,sslmode=conn.sslmode,
                                 trustServerCertificate=conn.trustServerCertificate)

# === Histórico de conexões ===
@router.get("/connection_history/", response_model=List[ConnectionLogBase])
def list_connection_history(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    logs = get_connection_logs(db, user_id)
    lista_logs = [
        ConnectionLogBase.model_validate({  
            "connection": str(log.connection_id),  # Verifique se o campo 'name' está correto
            "action": log.action,
            "timestamp": log.timestamp,
            "status": log.status
        }) for log in logs
    ]
        
    return lista_logs  # Retorna [] se não houver histórico

@router.get("/connection_logs/", response_model=ConnectionLogPaginationOutput)
def list_connection_logs(
    connection_id: Optional[int] = Query(None),   
    page: int = Query(1, ge=1),
    limit: int = Query(10, le=100),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    return get_connection_logs_pagination(db, user_id, connection_id, page, limit)



# === Testar uma conexão específica por ID ===
@router.post("/testconnections/{conn_id}", response_model=DbConnectionOutput)
def test_connection_by_id(
    conn_data: DBConnectionBase,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    
    conn = create_db_connection(db, user_id, conn_data)
    try:
        conn = get_session_by_connection(conn_data, db)
        conn.close()
        
        create_connection_log(
            db,
            connection_id=conn.id,
            action="Teste de conexão",
            status="success"
        )
    except Exception as e:
        log_message(f"Erro ao testar conexão: {str(e)}", level="error")
        create_connection_log(
                db,
                connection_id=conn.id,
                action="Teste de conexão",
                status="error"
            )
        raise HTTPException(status_code=400, detail=f"Erro ao testar conexão: {str(e)}")
        
    return DbConnectionOutput(
        connect=True,
        message="Conexão testada com sucesso!"
    )
