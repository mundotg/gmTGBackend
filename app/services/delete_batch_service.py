from typing import List, Dict, Any, Optional, Protocol
from sqlalchemy.orm import Session
from app.ultils.QueryExecutionService import QueryExecutionService
from app.ultils.logger import log_message

class DeleteRequest:
    def __init__(self, table, conditions, parameter_values, dry_run, reason, user_id):
        self.table = table
        self.conditions = conditions
        self.parameter_values = parameter_values
        self.dry_run = dry_run
        self.reason = reason
        self.user_id = user_id

    def __getattr__(self, name):
        raise NotImplementedError

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


class DeleteBatchService:
    """Serviço especializado para operações de DELETE em lote"""
    
    def __init__(self):
        # Annotate query_service with a Protocol so static analyzers recognize `execute_delete` exists
        self.query_service = QueryExecutionService()
    async def delete_batch_records(
        self,
        db: Session,
        user_id: int,
        table: str,
        records: List[Dict[str, Any]],
        original_query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Elimina múltiplos registros em lote de forma eficiente
        """
        try:
            log_message(f"🗑️ Iniciando DELETE em lote para {len(records)} registros", "info")
            
            # Agrupar registros por chave primária
            pk_groups = self._group_records_by_primary_key(records)
            
            total_deleted = 0
            all_affected_tables = set()
            errors = []
            
            # Processar cada grupo de chave primária
            for pk_column, pk_values in pk_groups.items():
                try:
                    # Criar DeleteRequest para o grupo atual
                    delete_request = DeleteRequest(
                        table=table,
                        conditions=[
                            {
                                "table": table,
                                "column": pk_column,
                                "operator": "IN",
                                "value": f"(:{pk_column}_values)",
                                "value_type": "string",
                                "logical_operator": "AND"
                            }
                        ],
                        parameter_values={f"{pk_column}_values": pk_values},
                        dry_run=False,
                        reason=f"Eliminação em lote - {len(pk_values)} registros",
                        user_id=str(user_id)
                    )
                    
                    # Executar DELETE
                    result = await self.query_service.execute_delete(
                        delete_request, db, user_id
                    )
                    
                    if result.success:
                        total_deleted += result.deleted_count
                        all_affected_tables.update(result.affected_tables)
                    else:
                        errors.append(f"Erro para {pk_column}: {result.error}")
                        
                except Exception as e:
                    errors.append(f"Erro processando {pk_column}: {str(e)}")
                    log_message(f"❌ Erro no grupo {pk_column}: {str(e)}", "error")
            
            # Preparar resposta
            response = {
                "success": len(errors) == 0,
                "total_deleted": total_deleted,
                "affected_tables": list(all_affected_tables),
                "total_processed": len(records),
                "errors": errors
            }
            
            if errors:
                response["message"] = f"Completado com {len(errors)} erro(s)"
                log_message(f"⚠️ DELETE em lote completado com {len(errors)} erro(s)", "warning")
            else:
                response["message"] = f"✅ {total_deleted} registros eliminados com sucesso"
                log_message(f"✅ DELETE em lote concluído: {total_deleted} registros", "success")
            
            return response
            
        except Exception as e:
            log_message(f"❌ Erro geral no DELETE em lote: {str(e)}", "error")
            return {
                "success": False,
                "total_deleted": 0,
                "affected_tables": [],
                "total_processed": len(records),
                "errors": [str(e)],
                "message": f"Erro durante eliminação em lote: {str(e)}"
            }
    
    def _group_records_by_primary_key(self, records: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """
        Agrupa registros por chave primária para DELETE eficiente
        """
        pk_groups = {}
        
        for record in records:
            pk_column = record.get("primaryKeys")
            pk_value = record.get("value")
            
            if pk_column and pk_value is not None:
                if pk_column not in pk_groups:
                    pk_groups[pk_column] = []
                pk_groups[pk_column].append(pk_value)
        
        return pk_groups
    
    async def validate_batch_delete(
        self,
        db: Session,
        user_id: int,
        table: str,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Valida uma operação de DELETE em lote sem executar
        """
        try:
            log_message(f"🔍 Validando DELETE em lote para {len(records)} registros", "info")
            
            pk_groups = self._group_records_by_primary_key(records)
            validation_results = {}
            
            for pk_column, pk_values in pk_groups.items():
                # Criar DeleteRequest em modo dry_run
                delete_request = DeleteRequest(
                    table=table,
                    conditions=[
                        {
                            "table": table,
                            "column": pk_column,
                            "operator": "IN",
                            "value": f"(:{pk_column}_values)",
                            "value_type": "string",
                            "logical_operator": "AND"
                        }
                    ],
                    parameter_values={f"{pk_column}_values": pk_values},
                    dry_run=True,  # Apenas validação
                    reason=f"Validação de DELETE em lote - {len(pk_values)} registros",
                    user_id=str(user_id)
                )
                
                result = await self.query_service.execute_delete(
                    delete_request, db, user_id
                )
                
                validation_results[pk_column] = {
                    "would_affect": result.deleted_count,
                    "is_valid": result.success,
                    "error": result.error
                }
            
            total_affected = sum(r["would_affect"] for r in validation_results.values())
            has_errors = any(not r["is_valid"] for r in validation_results.values())
            
            return {
                "is_valid": not has_errors,
                "total_would_affect": total_affected,
                "validation_results": validation_results,
                "message": f"Validação: {total_affected} registros seriam afetados"
            }
            
        except Exception as e:
            log_message(f"❌ Erro na validação em lote: {str(e)}", "error")
            return {
                "is_valid": False,
                "total_would_affect": 0,
                "validation_results": {},
                "error": str(e)
            }