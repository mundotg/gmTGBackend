import traceback
from typing import Any, Dict, NoReturn
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.query_executor import executar_query_e_salvar
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message


class QueryExecutionService:
    """Serviço unificado para execução de queries (SELECT, DELETE, etc)."""

    def __init__(self) -> None:
        self.connection_manager = ConnectionManager()

    def _handle_execution_error(
        self, 
        operation: str, 
        error: Exception, 
        user_id: int
    ) -> NoReturn:
        """Tratamento centralizado de erros"""
        error_msg = f"Erro ao executar {operation} para usuário {user_id}: {str(error)}"

        if isinstance(error, ConnectionError):
            log_message(f"🔌 Erro de conexão em {operation}: {str(error)}", "error")
            raise HTTPException(status_code=503, detail="Erro de conexão com o banco de dados")
        elif isinstance(error, HTTPException):
            raise error
        elif isinstance(error, ValueError):
            log_message(f"📝 Erro de validação em {operation}: {str(error)}", "warning")
            raise HTTPException(status_code=400, detail=str(error))
        else:
            log_message(f"{error_msg}\n{traceback.format_exc()}", "error")
            raise HTTPException(status_code=500, detail=error_msg)

    async def execute_query(
        self, 
        query_payload: QueryPayload, 
        db: Session, 
        user_id: int
    ) -> Dict[str, Any]:
        """Executa query SELECT"""
        try:
            engine, connection = self.connection_manager.ensure_connection(db, user_id)

            connection_type = getattr(connection, "connection_type", "unknown")
            log_message(f"📊 Executando SELECT para usuário {user_id} ({connection_type})", "info")
            log_message(f"📋 Tabela: {query_payload.baseTable}", "debug")

            return await executar_query_e_salvar(
                db=db,
                user_id=user_id,
                connection=connection,
                engine=engine,
                queryrequest=query_payload,
            )

        except Exception as e:
            self._handle_execution_error("SELECT", e, user_id)

   