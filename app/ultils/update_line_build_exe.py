import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.cruds.queryhistory_crud import create_query_history
from app.schemas.query_select_upAndInsert_schema import UpdateRequest
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.ultils.build_query import build_update_query
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message
# Assumindo que build_update_query e UpdateRequest estão importados corretamente no seu arquivo
# from app.ultils.build_query import build_update_query
# from app.schemas.update_schema import UpdateRequest

def update_row_service(
    data: UpdateRequest,
    engine: Engine,
    user_id: int,
    db_type: str,
    connection_id: str,
    db: Session,
    client_ip: Optional[str] = None,
    app_source: str = "API",
    executed_by: str = "sistema",
    modified_by: Optional[str] = None,
) -> dict:
    """
    Atualiza registros existentes em uma ou mais tabelas e salva no histórico completo.
    Garante transação ACID: Se uma tabela falhar, todas as alterações são revertidas.
    """
    resposta_query = ""
    query_string = ""
    start_time = time.time()
    
    sucesso = False
    error_msg = None

    try:
        # 🚀 Inicia a Transação: Tudo ou nada!
        with engine.begin() as conn:
            for table_name, primary_key_data in data.tables_primary_keys_values.items():
                
                # ✅ 1. Validação da chave primária
                primary_key = primary_key_data.get("primaryKey")
                primary_value = primary_key_data.get("valor")
                
                if not primary_key or primary_value is None:
                    raise ValueError(f"A tabela '{table_name}' não contém uma chave primária válida para o UPDATE.")

                # ✅ 2. Estruturação dos dados a atualizar
                raw_values = data.updatedRow.get(table_name, {})
                updated_values = {
                    col: {
                        "value": field["value"] if isinstance(field, dict) else getattr(field, "value", None),
                        "type_column": field.get("type_column", "text") if isinstance(field, dict) else getattr(field, "type_column", "text")
                    }
                    for col, field in raw_values.items()
                }

                # 🚨 MELHORIA: Se não houver nada para atualizar nesta tabela, apenas pula para a próxima
                if not updated_values:
                    log_message(f"Aviso: Tabela '{table_name}' ignorada pois não possui colunas alteradas.", "warning")
                    continue

                # ✅ 3. Monta a query SQL
                query = build_update_query(
                    table_name=table_name,
                    db_type=db_type,
                    updated_values=updated_values,
                    primary_key=primary_key,
                    primary_value=primary_value
                )
                if query is None:
                    continue
                # ✅ 4. Execução da query
                query_string += f"-- UPDATE em {table_name}\n{query}\n"
                
                # O ideal é que build_update_query retorne um objeto text() do SQLAlchemy com os parâmetros separados
                # para evitar SQL Injection. Assumindo que sua função já faz isso ou retorna string executável:
                rs = conn.execute(query)
                resposta_query += f"{table_name}: {rs.rowcount} linha(s) atualizada(s).\n"

        sucesso = True
        log_message(f"✅ Atualização em lote concluída:\n{resposta_query}", "success")

    except SQLAlchemyError as sa_err:
        error_msg = _lidar_com_erro_sql(sa_err)
        log_message(f"❌ Erro de Banco de Dados ao atualizar registro: {error_msg}", "error")
    except Exception as e:
        error_msg = str(e)
        log_message(f"❌ Erro inesperado ao atualizar registro: {error_msg}", "error")

    # ==========================================
    # 🧾 SALVAMENTO DO HISTÓRICO (Roda sempre)
    # ==========================================
    duration_ms = int((time.time() - start_time) * 1000)

    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection_id, # type: ignore
        query=query_string.strip() or "UPDATE não gerou query.",
        query_type=QueryType.UPDATE,
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=resposta_query.strip() if sucesso else "Sem resultado devido a falha.",
        error_message=error_msg, # Fica None se sucesso for True
        is_favorite=False,
        tags="update" if sucesso else "update_error",
        app_source=app_source,
        client_ip=client_ip,
        executed_by=executed_by,
        modified_by=modified_by or executed_by,
        meta_info={
            "db_type": db_type,
            "tables_attempted": list(data.updatedRow.keys()),
            "primary_keys": data.tables_primary_keys_values,
            "status": "success" if sucesso else "failed",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

    # Salva o histórico (mesmo se o update original deu Rollback, essa sessão DB salva o log)
    create_query_history(db=db, user_id=user_id, data=historico)

    # ==========================================
    # 🚀 RETORNO / EXCEÇÃO
    # ==========================================
    if not sucesso:
        # Repassa o erro formatado para o FastAPI devolver um 400/500 amigável pro frontend
        raise ValueError(error_msg)

    return {
        "status": "success",
        "updated": data.updatedRow,
        "response": resposta_query.strip() or "Nenhuma alteração foi realizada."
    }