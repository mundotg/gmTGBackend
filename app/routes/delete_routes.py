"""
Rotas para operações de deleção no banco de dados.
Gerencia endpoints para remoção segura de registros.
"""

import traceback

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_delete_schema import DeleteRequest, DeleteByIdsRequest, DeleteResponse
from app.ultils.logger import log_message
from app.services.executar_delete_e_salvar import DeleteOperationService

# Configuração do router
router = APIRouter(prefix="/delete", tags=["Data Deletion"])


@router.post("/records", response_model=DeleteResponse)
async def delete_records_by_conditions(
    delete_request: DeleteRequest,
    db_session: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    Remove registros baseado em condições específicas.
    Garante isolamento de dados por usuário.
    """
    try:
        # Validações de entrada
        if not delete_request.table:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Nome da tabela é obrigatório"
            )

        if not delete_request.conditions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Pelo menos uma condição é necessária"
            )

        # Garantir user_id na requisição
        if not delete_request.user_id:
            delete_request.user_id = str(current_user_id)

        deletion_service = DeleteOperationService()
        result = await deletion_service.execute_conditional_delete(
            delete_request=delete_request,
            db_session=db_session,
            current_user_id=current_user_id
        )

        return result

    except HTTPException:
        # Re-lança exceções HTTP existentes
        raise
        
    except ValueError as error:
        # Captura erros de validação do serviço
        log_message(f"Erro de validação: {str(error)}", "warning")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error)
        )
        
    except Exception as error:
        # Captura erros inesperados
        log_message(
            f"Erro interno na deleção por condições: {str(error)}\n{traceback.format_exc()}", 
            "error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno do servidor durante a operação de deleção"
        )


@router.post("/bulk", response_model=DeleteResponse)
async def bulk_delete_records_by_ids(
    delete_request: DeleteByIdsRequest,
    db_session: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    Remove múltiplos registros por lista de IDs.
    Aplica automaticamente restrições de segurança.
    """
    try:
        # Validação básica
        if not delete_request.ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Lista de IDs é obrigatória"
            )

        # Garantir user_id na requisição
        if not delete_request.user_id:
            delete_request.user_id = str(current_user_id)

        deletion_service = DeleteOperationService()
        result = await deletion_service.execute_bulk_delete_by_ids(
            delete_request=delete_request,
            db_session=db_session,
            current_user_id=current_user_id
        )

        return result

    except HTTPException:
        raise
        
    except ValueError as error:
        log_message(f"Erro de validação em lote: {str(error)}", "warning")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error)
        )
        
    except Exception as error:
        log_message(
            f"Erro interno na deleção em lote: {str(error)}\n{traceback.format_exc()}", 
            "error"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno do servidor durante a deleção em lote"
        )


@router.get("/health")
async def health_check():
    """
    Health check para verificar disponibilidade do serviço de deleção.
    """
    return {
        "status": "healthy",
        "service": "delete_operations",
        "timestamp": "2024-01-01T00:00:00Z"  # Você pode usar datetime.utcnow().isoformat()
    }