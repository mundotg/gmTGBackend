# app/schemas/db_transfer_schema.py

from typing import  Dict, Optional
from pydantic import BaseModel


class ColumnMapping(BaseModel):
    coluna_origen_name: Optional[str] = None
    coluna_distino_name: Optional[str] = None
    type_coluna_origem: Optional[str] = None
    type_coluna_destino: Optional[str] = None
    id_coluna_origem: Optional[str] = None
    id_coluna_destino: Optional[str] = None
    enabled: bool
    
    
class TableMapping(BaseModel):
  tabela_name_origem: str
  tabela_name_destino: str
  id_tabela_origen: int
  id_tabela_destino: int
  colunas_relacionados_para_transacao: Optional[list[ColumnMapping]] = None



class TransferRequest:
    id_connectio_origen: int
    id_connectio_distino: int
    tables_origen: Dict[str, Dict[str, ColumnMapping]]
    batch_size: int = 5000
