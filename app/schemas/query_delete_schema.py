from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime
from app.schemas.query_select_upAndInsert_schema import QueryPayload

class RowDelete(BaseModel):
    primaryKey: Optional[str] = Field(None, description="Nome da coluna chave primária")
    primaryKeyValue: Optional[Any] = Field(None, description="Valor da chave primária")
    keyType: Optional[Any] = Field(None, description="Tipo do dado da chave primária")
    isPrimarykeyOrUnique: Optional[bool] = Field(None, description="Indica se é chave primária ou única")
    index: int = Field(..., description="Índice do registro na lista original")


class PayloadDeleteRow(BaseModel):
    tableForDelete: List[str] = Field(..., description="Payload completo da linha selecionada")
    rowDeletes: Dict[str, RowDelete] = Field(..., description="Tabela → informações da chave primária")
    


class BatchDeleteRequest(BaseModel):
    registros: List[PayloadDeleteRow] = Field(..., description="Lista de registros a serem excluídos")
    payloadSelectedRow: Optional[QueryPayload] = Field(None, description="Payload completo da linha selecionada")
    dry_run: Optional[bool] = Field(
        False,
        title="Dry Run",
        description="Se True, simula a exclusão sem persistir alterações no banco de dados."
    )


class DeleteResponse(BaseModel):
    success: bool = True
    mensagem: str
    itens_afetados: List[Dict[str, Any]] = Field(default_factory=list)
    dry_run: Optional[bool] = Field(
        False,
        description="Indica se a operação foi executada em modo de simulação (dry-run)."
    )
    query: Optional[str] = Field(
        None,
        description="SQL gerado/simulado durante a operação (se aplicável)."
    )
    executado_em: datetime = Field(default_factory=datetime.utcnow)
