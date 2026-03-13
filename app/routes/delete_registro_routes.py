"""
Rotas de exclusão de registros.
"""

import traceback
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.routes.connection_routes import get_current_user_id
from app.schemas.query_delete_schema import BatchDeleteRequest, DeleteResponse, PayloadDeleteRow
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.services.executar_delete_e_salvar import DeleteOperationService
from app.ultils.logger import log_message

router = APIRouter(prefix="/delete", tags=["Data Deletion"])


def _sum_total_afetados(itens_afetados) -> int:
    return sum(item.get("afetados", 0) for item in (itens_afetados or []))


@router.delete("/records", response_model=DeleteResponse)
async def delete_multiple_records(
    delete_request: BatchDeleteRequest,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    try:
        registros = delete_request.registros or []
        if not registros:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A lista de registros para exclusão não pode estar vazia.",
            )

        log_message(
            f"Iniciando exclusão em lote de {len(registros)} registro(s) pelo usuário {current_user_id}.",
            "info",
        )

        service = DeleteOperationService()
        result = await service.execute_delete(
            registros=registros,
            payloadQuery=delete_request.payloadSelectedRow,
            db=db,
            current_user_id=current_user_id,
            delete_all=False,
        )

        total_afetados = _sum_total_afetados(result.itens_afetados)

        log_message(
            f"Exclusão em lote concluída com sucesso. Total removido(s): {total_afetados}.",
            "success",
        )

        return result

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            f"Erro interno durante exclusão em lote (User: {current_user_id}): {str(e)}\n{traceback.format_exc()}",
            "error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Não foi possível concluir a exclusão. Verifique dependências em outras tabelas ou tente novamente.",
        )


@router.delete("/delete_all", response_model=DeleteResponse)
async def delete_all_records(
    query_payload: QueryPayload,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    try:
        fake_registro = PayloadDeleteRow(
            tableForDelete=[query_payload.baseTable],
            rowDeletes={},
        )

        service = DeleteOperationService()
        result = await service.execute_delete(
            registros=[fake_registro],
            payloadQuery=query_payload,
            db=db,
            current_user_id=current_user_id,
            delete_all=True,
        )

        total_afetados = _sum_total_afetados(result.itens_afetados)

        log_message(
            f"Exclusão total concluída com sucesso. Total removido(s): {total_afetados}.",
            "success",
        )

        return result

    except HTTPException:
        raise

    except Exception as e:
        log_message(
            f"Erro durante exclusão total: {str(e)}\n{traceback.format_exc()}",
            "error",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao excluir todos os registros.",
        )