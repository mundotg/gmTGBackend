import json
import re
from datetime import datetime, timezone
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from app.schemas.query_delete_schema import PayloadDeleteRow, DeleteResponse
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.cruds.queryhistory_crud import create_query_history
from app.services.cloudeAi_execute_query import QueryFilterBuilder
from app.services.query_executor import is_safe_identifier
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message
from app.ultils.logica_de_join_advance import build_join_clause_for_delete


class DeleteOperationService:
    """
    Serviço responsável por executar operações DELETE seguras e genéricas,
    com suporte a múltiplos dialetos SQL (PostgreSQL, MySQL, SQL Server, SQLite).
    """

    def __init__(self) -> None:
        self.connection_manager = ConnectionManager()

    def detect_dialect(self, engine) -> str:
        """Detecta o dialeto ativo."""
        name = engine.dialect.name.lower()
        if "postgres" in name:
            return "postgres"
        if "mysql" in name:
            return "mysql"
        if "mssql" in name or "sqlserver" in name:
            return "mssql"
        if "sqlite" in name:
            return "sqlite"
        return "generic"

    async def execute_delete_all(
        self,
        query_payload: QueryPayload,
        db: Session,
        current_user_id: int,
        client_ip: str = None,
        app_source: str = "API",
        executed_by: str = "sistema",
        modified_by: str = None,
    ) -> DeleteResponse:
        """
        Executa um DELETE genérico com suporte a JOINs e múltiplos dialetos SQL.
        """
        tabela_base = getattr(query_payload, "baseTable", None)
        if not tabela_base:
            raise ValueError("Tabela base não informada no QueryPayload")
        print("saffsdf: 1")
        if not is_safe_identifier(tabela_base):
            raise ValueError(f"Tabela insegura: {tabela_base}")
        print("saffsdf: 12")
        engine, connection = self.connection_manager.ensure_connection(db, current_user_id)
        dialect = self.detect_dialect(engine)
        params: Dict[str, Any] = {}

        if dialect in ["mysql", "mssql"]:
            sql_parts = [f"DELETE {tabela_base} FROM {tabela_base}"]
        else:
            sql_parts = [f"DELETE FROM {tabela_base}"]

        # 🔗 JOINs e WHERE
        
        join_clauses = build_join_clause_for_delete(db_type=connection.type, base_table= tabela_base,joins= query_payload.joins, table_list=query_payload.table_list, is_delete=True)
        filter_builder = QueryFilterBuilder()
        filters, params = await filter_builder.build_where_clause(
            query_payload.where or [], connection.type
        )

        # ⚠️ Segurança
        if not query_payload.where or len(filters) == 0:
            log_message("⚠️ Tentativa de DELETE sem filtro. Operação cancelada.", "warning")
            raise ValueError("Operação bloqueada: DELETE sem WHERE é perigoso.")

        if join_clauses:
            sql_parts.append(join_clauses)
        if filters:
            sql_parts.append(f" {filters}")

        sql = " ".join(sql_parts)
        log_message(f"🧹 DELETE ({dialect}) → {sql}\nParams: {params}", "debug")

        # 🚀 Execução
        try:
            start = datetime.now(timezone.utc)
            with engine.begin() as conn:
                result = conn.execute(text(sql), params)
                afetados = result.rowcount or 0
            duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)

            # 🧾 Histórico completo
            historico = QueryHistoryCreate(
                user_id=current_user_id,
                db_connection_id=connection.id,
                query=sql,
                query_type=QueryType.DELETE,
                executed_at=start,
                updated_at=datetime.now(timezone.utc),
                duration_ms=duration_ms,
                result_preview=json.dumps({"afetados": afetados}, ensure_ascii=False),
                error_message=None,
                is_favorite=False,
                tags=f"delete_generic_{dialect}",
                app_source=app_source,
                client_ip=client_ip,
                executed_by=executed_by,
                modified_by=modified_by or executed_by,
                meta_info={
                    "base_table": tabela_base,
                    "joins": query_payload.joins,
                    "filters": query_payload.where,
                    "dialect": dialect,
                    "timestamp": start.isoformat()
                },
            )
            create_query_history(db=db, data=historico)

            return DeleteResponse(
                success=True,
                mensagem=f"{afetados} registro(s) excluído(s) com sucesso.",
                itens_afetados=[{"tabela": tabela_base, "afetados": afetados}],
                executado_em=datetime.utcnow(),
            )

        except SQLAlchemyError as sa_err:
            error_msg = _lidar_com_erro_sql(sa_err)
            log_message(f"❌ Erro SQL: {error_msg}", "error")
            raise Exception(f"Erro SQL: {error_msg}")
        except Exception as e:
            log_message(f"❌ Erro inesperado no DELETE genérico: {e}", "error")
            raise Exception(str(e))

    async def execute_batch_delete(
        self,
        registros: List[PayloadDeleteRow],
        db: Session,
        current_user_id: int,
        client_ip: str = None,
        app_source: str = "API",
        executed_by: str = "sistema",
        modified_by: str = None,
    ) -> DeleteResponse:
        """
        Exclui múltiplos registros de forma segura, validando identificadores e registrando histórico completo.
        """
        itens_afetados: List[Dict[str, Any]] = []
        erros: List[str] = []
        engine, connection = self.connection_manager.ensure_connection(db, current_user_id)

        try:
            start = datetime.now(timezone.utc)
            with engine.begin() as conn:
                for registro in registros:
                    for tabela_name, dados in registro.rowDeletes.items():
                        pk_coluna = dados.primaryKey
                        pk_valor = dados.primaryKeyValue
                        key_type = dados.keyType

                        if not tabela_name or not pk_coluna:
                            msg = f"Tabela ou coluna inválida no payload: {tabela_name or '?'}"
                            erros.append(msg)
                            continue

                        if not pk_valor:
                            msg = f"Registro ignorado (chave primária vazia) → {tabela_name}.{pk_coluna}"
                            erros.append(msg)
                            continue

                        if not is_safe_identifier(tabela_name) or not is_safe_identifier(pk_coluna):
                            msg = f"Nome inseguro detectado → tabela: {tabela_name}, coluna: {pk_coluna}"
                            erros.append(msg)
                            continue

                        try:
                            if key_type in ("integer", "int"):
                                pk_valor = int(pk_valor)
                            elif key_type in ("float", "double", "decimal"):
                                pk_valor = float(pk_valor)
                            else:
                                pk_valor = str(pk_valor)
                        except ValueError:
                            erros.append(f"Valor inválido: {tabela_name}.{pk_coluna} = {pk_valor}")
                            continue

                        sql = f"DELETE FROM {tabela_name} WHERE {pk_coluna} = :pk_valor"
                        result = conn.execute(text(sql), {"pk_valor": pk_valor})
                        afetados = result.rowcount or 0

                        itens_afetados.append({
                            "tabela": tabela_name,
                            "chave": pk_coluna,
                            "valor": pk_valor,
                            "afetados": afetados
                        })

            duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)

            # Histórico completo
            historico = QueryHistoryCreate(
                user_id=current_user_id,
                db_connection_id=connection.id,
                query="DELETE (batch)",
                query_type=QueryType.DELETE,
                executed_at=start,
                updated_at=datetime.now(timezone.utc),
                duration_ms=duration_ms,
                result_preview=json.dumps(itens_afetados, default=str, ensure_ascii=False),
                error_message=json.dumps(erros, ensure_ascii=False) if erros else None,
                is_favorite=False,
                tags="delete_batch",
                app_source=app_source,
                client_ip=client_ip,
                executed_by=executed_by,
                modified_by=modified_by or executed_by,
                meta_info={
                    "total_itens": len(itens_afetados),
                    "tabelas_afetadas": [i["tabela"] for i in itens_afetados],
                    "erros": erros,
                    "timestamp": start.isoformat()
                },
            )
            create_query_history(db=db, data=historico)

            mensagem_final = f"{len(itens_afetados)} registro(s) excluído(s) com sucesso."
            if erros:
                mensagem_final += f" ⚠️ {len(erros)} aviso(s)/erro(s) foram registrados."

            return DeleteResponse(
                success=True if itens_afetados else False,
                mensagem=mensagem_final,
                itens_afetados=itens_afetados,
                executado_em=datetime.utcnow(),
            )

        except SQLAlchemyError as sa_err:
            error_msg = _lidar_com_erro_sql(sa_err)
            log_message(f"❌ Erro SQL no DELETE: {error_msg}", "error")
            raise Exception(f"Erro SQL: {error_msg}")
        except Exception as e:
            log_message(f"❌ Erro inesperado no DELETE: {e}", "error")
            raise Exception(str(e))
