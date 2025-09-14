import traceback
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db, get_db_async
from app.routes.connection_routes import get_current_user_id
from app.schemas.queryhistory_schemas import AutoCreateRequest, InsertRequest, QueryPayload, UpdateRequest
from app.services import executar_query_e_salvar_stream
from app.services.insert_row_service import insert_row_service
from app.services.editar_linha import  update_row_service
from app.services.insert_service_auto import insert_row_service_auto
from app.services.query_executor import executar_query_e_salvar
from app.ultils.ativar_session_bd import get_connection_current, reativar_connection
from app.config.dependencies import EngineManager
from app.ultils.logger import log_message
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/exe", tags=["executeQuery"])

@router.post("/update_row")
def update_row(data: UpdateRequest, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        engine = EngineManager.get(user_id)

        if not engine:
            res = reativar_connection(db=db, id_user=user_id)
            if not res["success"]:
                raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")
            engine = EngineManager.get(user_id)

        connection, _ = get_connection_current(db, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="ID da conexão não está disponível")
        
        return update_row_service(data, engine, user_id, connection.type, connection.id,db)

    except Exception as e:
        db.rollback()
        log_message(f"Erro ao atualizar a linha: {str(e)}{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/insert_row")
def update_row(data: InsertRequest, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        engine = EngineManager.get(user_id)

        if not engine:
            res = reativar_connection(db=db, id_user=user_id)
            if not res["success"]:
                raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")
            engine = EngineManager.get(user_id)

        connection, _ = get_connection_current(db, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="ID da conexão não está disponível")
        
        return insert_row_service(data, engine, user_id, connection.type, connection.id,db)

    except Exception as e:
        db.rollback()
        log_message(f"Erro ao atualizar a linha: {str(e)}{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/auto-create")
def auto_create(
    data: AutoCreateRequest,
    user_id: int = Depends(get_current_user_id),  # substituir pelo usuário logado
    db: Session = Depends(get_db)
):
    """
    Recebe configs do frontend e insere múltiplas linhas.
    """
    
    try:
        engine = EngineManager.get(user_id)

        if not engine:
            res = reativar_connection(db=db, id_user=user_id)
            if not res["success"]:
                raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")
            engine = EngineManager.get(user_id)

        connection, _ = get_connection_current(db, user_id)
        if connection is None:
            raise HTTPException(status_code=400, detail="ID da conexão não está disponível")
        
        return insert_row_service_auto(data, engine, user_id, connection, db)

    except Exception as e:
        db.rollback()
        log_message(f"Erro ao atualizar a linha: {str(e)}{traceback.format_exc()}", "error")
        raise HTTPException(status_code=500, detail=str(e))
 


def _executar_query_interno(
    body: QueryPayload,
    db: Session,
    user_id: int
):
    engine = EngineManager.get(user_id)

    if not engine:
        res = reativar_connection(db=db, id_user=user_id)
        if not res["success"]:
            raise HTTPException(status_code=400, detail="Conexão do banco de dados não encontrada")
        engine = EngineManager.get(user_id)

    connection, _ = get_connection_current(db, user_id)
    if connection is None:
        raise HTTPException(status_code=400, detail="ID da conexão não está disponível")

    try:
        return executar_query_e_salvar(
            db=db,
            user_id=user_id,
            connection=connection,
            engine=engine,
            queryrequest=body
        )
    except Exception as e:
        log_message(f"Erro ao executar a query: {str(e)}{traceback.format_exc()}", "error") 
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute_query")
def executar_query(
    body: QueryPayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    return _executar_query_interno(body, db, user_id)


@router.post("/query-scroll")
def executar_query_scroll(
    body: QueryPayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    return _executar_query_interno(body, db, user_id)



@router.get("/query-sse")
async def executar_query_sse(
    body: QueryPayload,
    db: AsyncSession = Depends(get_db_async),
    user_id: int = Depends(get_current_user_id)
):
    return executar_query_e_salvar_stream(db, user_id, body)