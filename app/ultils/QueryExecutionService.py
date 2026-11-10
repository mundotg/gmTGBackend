import traceback
from typing import Any, Dict, NoReturn
from sqlalchemy import or_, and_
from sqlalchemy.orm import Session
from fastapi import HTTPException

from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.query_executor import executar_query_e_salvar
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.logger import log_message
from app.models.connection_models import DBConnection, DBConnectionShare, EmpresaConnection
from app.models.dbstructure_models import DBStructure
from app.ultils.permissions import is_admin
from app.ultils.connection_access import load_actor


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
            if getattr(query_payload, "connection_id", None):
                engine, connection = self.connection_manager.ensure_idConn_connection(
                    db, user_id, query_payload.connection_id
                )
            else:
                engine, connection = self.connection_manager.ensure_connection(db, user_id)
                # Fallback: Se baseTable não existe na conexão ativa, tenta localizar em outra conexão do utilizador
                pure_base = query_payload.baseTable.split(".")[-1]
                active_has_table = db.query(DBStructure).filter(
                    DBStructure.db_connection_id == connection.id,
                    DBStructure.table_name == pure_base,
                    DBStructure.is_deleted == False,
                ).first()
                if not active_has_table:
                    actor = load_actor(db, user_id)
                    alt_struct_query = (
                        db.query(DBStructure)
                        .join(DBConnection, DBStructure.db_connection_id == DBConnection.id)
                        .filter(
                            DBStructure.table_name == pure_base,
                            DBStructure.is_deleted == False,
                        )
                    )
                    if not is_admin(actor):
                        emp_filter = (
                            and_(EmpresaConnection.empresa_id == actor.empresa_id, actor.empresa_id != None)
                            if getattr(actor, "empresa_id", None)
                            else False
                        )
                        alt_struct_query = alt_struct_query.outerjoin(
                            DBConnectionShare, DBConnectionShare.connection_id == DBConnection.id
                        ).outerjoin(
                            EmpresaConnection, EmpresaConnection.connection_id == DBConnection.id
                        ).filter(
                            or_(
                                DBConnection.user_id == user_id,
                                DBConnectionShare.user_id == user_id,
                                emp_filter,
                            )
                        )
                    alt_struct = alt_struct_query.first()
                    if alt_struct:
                        try:
                            alt_engine, alt_conn = self.connection_manager.ensure_idConn_connection(
                                db, user_id, alt_struct.db_connection_id
                            )
                            engine, connection = alt_engine, alt_conn
                        except Exception:
                            pass

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

   