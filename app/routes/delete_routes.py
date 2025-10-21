"""
Rotas de exclusão de registros (batch delete compatível com frontend).
"""

import traceback
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_delete_schema import (
    BatchDeleteRequest,
    DeleteResponse
)
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.executar_delete_e_salvar import DeleteOperationService
from app.ultils.logger import log_message

router = APIRouter(prefix="/delete", tags=["Data Deletion"])


@router.delete("/records", response_model=DeleteResponse)
async def delete_multiple_records(
    delete_request: BatchDeleteRequest,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id)
):
    """
    Exclui múltiplos registros de uma só vez (em lote).
    
    Espera um payload no formato:
    {
        "registros": [
            {
                "rowDeletes": { 
                    "tabela": { 
                        "primaryKey": "id", 
                        "primaryKeyValue": 5 
                    } 
                },
                "payloadSelectedRow": { "table": "tabela" }
            }
        ]
    }
    """
    try:
        registros = delete_request.registros or []
        total_registros = len(registros)

        if total_registros == 0:
            log_message("Nenhum registro fornecido para exclusão.", "warning")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A lista de registros não pode estar vazia."
            )

        log_message(f"Iniciando exclusão em lote ({total_registros} registro(s))...", "info")

        delete_service = DeleteOperationService()
        resultado = await delete_service.execute_batch_delete(
            registros=registros,
            db=db,
            current_user_id=current_user_id
        )

        total_afetados = len(resultado.itens_afetados or [])
        log_message(
            f"Exclusão em lote concluída com sucesso: {total_afetados} registro(s) removido(s).",
            "success"
        )

        return resultado

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            f"Erro interno durante exclusão em lote: {str(e)}\n{traceback.format_exc()}",
            "error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao tentar excluir registros: {str(e)}"
        )


@router.delete("/delete_all", response_model=DeleteResponse)
async def delete_all_records(
    query_payload: QueryPayload,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id)
):
    """
    Exclui todos os registros retornados por uma query salva.
    """
    try:
        delete_service = DeleteOperationService()
        result = await delete_service.execute_delete_all(
            query_payload=query_payload,
            db=db,
            current_user_id=current_user_id
        )

        total_afetados = len(result.itens_afetados or [])
        log_message(
            f"Exclusão total concluída com sucesso ({total_afetados} registro(s) removido(s)).",
            "success"
        )

        return result

    except Exception as e:
        log_message(
            f"Erro durante exclusão total: {str(e)}\n{traceback.format_exc()}",
            "error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao excluir todos os registros: {str(e)}"
        )


