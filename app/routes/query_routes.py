from logging import log
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.queryhistory_schemas import QueryPayload
from app.services.query_executor import executar_query_e_salvar
from app.ultils.ativar_session_bd import get_connection_current, reativar_connection
from app.config.dependencies import EngineManager
from app.ultils.logger import log_message

router = APIRouter(prefix="/exe", tags=["executeQuery"])

@router.post("/execute_query")
def executar_query(
    body: QueryPayload,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
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
        resultado = executar_query_e_salvar(
            db=db,
            user_id=user_id,
            connection=connection,
            engine=engine,
            queryrequest=body  # ✅ Aqui está a correção
        )
        return resultado
    except Exception as e:
        log_message(f"Erro ao executar a query: {str(e)}", "error") 
        raise HTTPException(status_code=500, detail=str(e))
