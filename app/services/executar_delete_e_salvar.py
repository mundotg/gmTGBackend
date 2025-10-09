"""
Serviço para operações de deleção no banco de dados.
Executa DELETEs de forma segura com validações e isolamento de dados.
"""

import traceback
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import bindparam, delete, Table, MetaData, and_

from app.schemas.query_delete_schema import DeleteRequest, DeleteByIdsRequest, DeleteResponse
from app.ultils.logger import log_message


class DeleteOperationService:
    """
    Serviço responsável por executar operações de DELETE de forma segura.
    Utiliza metadados dinâmicos e validações para garantir integridade dos dados.
    """

    async def execute_conditional_delete(
        self,
        delete_request: DeleteRequest,
        db_session: Session,
        current_user_id: int,
    ) -> DeleteResponse:
        """
        Executa deleção baseada em condições WHERE com validação de segurança.
        Garante isolamento de dados através do user_id.
        """
        target_table = delete_request.table
        where_conditions = delete_request.conditions

        # Validações iniciais
        if not where_conditions:
            raise ValueError("Pelo menos uma condição WHERE é obrigatória")

        # Carregar estrutura da tabela
        table_metadata = await self._load_table_metadata(target_table, db_session)
        
        # Validar colunas de segurança
        self._validate_required_columns(table_metadata)

        # Construir cláusulas WHERE com segurança
        where_clauses, query_parameters = self._build_where_clauses(
            table_metadata, where_conditions, current_user_id
        )

        # Executar deleção
        return await self._execute_delete_operation(
            table_metadata, where_clauses, query_parameters, db_session, target_table
        )

    async def execute_bulk_delete_by_ids(
        self,
        delete_request: DeleteByIdsRequest,
        db_session: Session,
        current_user_id: int,
    ) -> DeleteResponse:
        """
        Executa deleção em lote baseada em lista de IDs.
        Aplica automaticamente restrição de user_id para segurança.
        """
        target_table = delete_request.table
        record_ids = delete_request.ids

        if not record_ids:
            raise ValueError("Lista de IDs não pode estar vazia")

        # Carregar estrutura da tabela
        table_metadata = await self._load_table_metadata(target_table, db_session)
        
        # Validar colunas de segurança
        self._validate_required_columns(table_metadata)

        # Construir cláusulas WHERE para deleção por IDs
        primary_key_column = table_metadata.columns['id']
        user_id_column = table_metadata.columns['user_id']
        
        where_clauses = [
            primary_key_column.in_(record_ids),
            user_id_column == str(current_user_id)
        ]

        # Executar deleção
        return await self._execute_delete_operation(
            table_metadata, where_clauses, {}, db_session, target_table
        )

    async def _load_table_metadata(self, table_name: str, db_session: Session) -> Table:
        """Carrega metadados da tabela de forma segura."""
        dynamic_metadata = MetaData()
        try:
            return Table(table_name, dynamic_metadata, autoload_with=db_session.bind)
        except Exception as error:
            raise ValueError(f"Tabela '{table_name}' não encontrada: {str(error)}")

    def _validate_required_columns(self, table_metadata: Table):
        """Valida se a tabela possui colunas necessárias para operações seguras."""
        required_columns = ['id', 'user_id']
        for column_name in required_columns:
            if column_name not in table_metadata.columns:
                raise ValueError(
                    f"Tabela '{table_metadata.name}' deve ter a coluna '{column_name}' "
                    "para operações de segurança"
                )

    def _build_where_clauses(
        self, 
        table_metadata: Table, 
        conditions: List[Dict], 
        user_id: int
    ) -> tuple:
        """
        Constrói cláusulas WHERE com validação de segurança.
        Retorna tuple (cláusulas, parâmetros).
        """
        where_clauses = []
        query_parameters = {}
        parameter_counter = 0

        # Isolamento obrigatório por user_id
        user_id_column = table_metadata.columns['user_id']
        where_clauses.append(user_id_column == str(user_id))

        for condition in conditions:
            field_name = condition.get('field')
            operator_type = condition.get('operator', '=')
            field_value = condition.get('value')
            
            if not field_name or field_value is None:
                continue

            # Validar existência da coluna
            column_ref = table_metadata.columns.get(field_name)
            if column_ref is None:
                raise ValueError(f"Coluna '{field_name}' não encontrada")

            parameter_name = f"param_{parameter_counter}"
            parameter_counter += 1

            # Aplicar operador com validação
            where_clause, param_value = self._apply_operator(
                column_ref, operator_type, field_value, parameter_name
            )
            
            if param_value is not None:
                query_parameters[parameter_name] = param_value
            where_clauses.append(where_clause)

        return where_clauses, query_parameters

    def _apply_operator(self, column_ref, operator_type: str, value: Any, param_name: str):
        """Aplica operador SQL com validação adequada."""
        operator_mapping = {
            '=': (column_ref == bindparam(param_name), value),
            '!=': (column_ref != bindparam(param_name), value),
            '>': (column_ref > bindparam(param_name), value),
            '<': (column_ref < bindparam(param_name), value),
            '>=': (column_ref >= bindparam(param_name), value),
            '<=': (column_ref <= bindparam(param_name), value),
            'IN': (column_ref.in_(value), None)  # IN trata valores diretamente
        }

        if operator_type.upper() == 'IN':
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"Operador IN requer lista de valores: {column_ref.name}")
            return column_ref.in_(value), None

        if operator_type not in operator_mapping:
            raise ValueError(f"Operador '{operator_type}' não suportado")

        return operator_mapping[operator_type]

    async def _execute_delete_operation(
        self,
        table_metadata: Table,
        where_clauses: List,
        parameters: Dict,
        db_session: Session,
        table_name: str
    ) -> DeleteResponse:
        """Executa a operação DELETE de forma segura."""
        delete_statement = delete(table_metadata).where(and_(*where_clauses))

        try:
            execution_result = db_session.execute(delete_statement, parameters)
            records_deleted = execution_result.rowcount
            db_session.commit()

            return DeleteResponse(
                table=table_name,
                deleted_count=records_deleted,
                status="success",
                message=f"{records_deleted} registros removidos de '{table_name}'"
            )

        except Exception as error:
            db_session.rollback()
            log_message(
                f"Erro na execução do DELETE: {str(error)}\n{traceback.format_exc()}", 
                "error"
            )
            raise Exception(f"Falha na execução do DELETE: {error}")