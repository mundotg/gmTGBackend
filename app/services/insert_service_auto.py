# services/insert_service.py
import random
import time
from datetime import datetime, timezone
from typing import Dict, List
from sqlalchemy.orm import Session
from sqlalchemy import Engine
from app.models.connection_models import DBConnection
from app.models.dbstructure_models import DBField
from app.schemas.dbstructure_schema import CampoDetalhado
from app.schemas.queryhistory_schemas import AutoCreateRequest, ConfiguracaoTabela, QueryHistoryCreate, QueryPayload
from app.cruds.queryhistory_crud import create_query_history
from app.services.field_info import buscar_estrutura_tabela, buscar_ou_criar_campos_tabela, processar_enum_fields
from app.services.insert_row_service import build_insert_query
from app.services.query_executor import executar_query_e_salvar
from app.ultils.buscar_enum_bd import _fetch_enum_values
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.generate_value import gerar_valor_pelo_tipo_de_dados_na_bd
from app.ultils.logger import log_message
from collections import defaultdict
def insert_row_service_auto(
    data: AutoCreateRequest,
    engine: Engine,
    user_id: int,
    connection: DBConnection,
    db: Session,
    dry_run: bool = False
):
    """
    Insere linhas automaticamente em tabelas, gerando valores fake.
    - Continua mesmo que uma linha falhe.
    - Commit parcial: não perde inserções bem-sucedidas.
    - Logs e histórico guardam os detalhes.
    - Resposta para o front é resumida (contagem de sucesso/erro/simulação por tabela).
    """
    tabela_stats = defaultdict(lambda: {"sucesso": 0, "erros": 0, "simulacao": 0})
    query_string = []
    start = time.time()

    with engine.connect() as conn:
        for config in data.configs:
            for _ in range(config.quantidade):
                try:
                    insert_dict = gerar_dict_para_insercao(
                        config=config,
                        engine=engine,
                        db=db,
                        connection=connection,
                        user_id=user_id
                    )
                    query = build_insert_query(config.tabela, connection.type, insert_dict)
                    query_string.append(f"{config.tabela}: {query}")
                    log_message(f"Tentando inserir em {config.tabela}: {insert_dict}")

                    if dry_run:
                        tabela_stats[config.tabela]["simulacao"] += 1
                    else:
                        rs = conn.execute(query)
                        conn.commit()  # commit parcial para garantir persistência
                        tabela_stats[config.tabela]["sucesso"] += rs.rowcount
                except Exception as e:
                    error_msg = _lidar_com_erro_sql(e)
                    log_message(f"Erro ao inserir em {config.tabela}: {error_msg}", "error")
                    tabela_stats[config.tabela]["erros"] += 1

    duration_ms = int((time.time() - start) * 1000)

    # Registrar histórico resumido
    historico = QueryHistoryCreate(
        user_id=user_id,
        db_connection_id=connection.id,
        query="\n".join(query_string) if query_string else "Nenhuma query gerada",
        query_type="INSERT",
        executed_at=datetime.now(timezone.utc),
        duration_ms=duration_ms,
        result_preview=str(dict(tabela_stats)),
        error_message=None,
        is_favorite=False,
        tags="insert auto"
    )
    create_query_history(db=db, data=historico)

    # Preparar resposta resumida
    resumo = [
        {"tabela": tabela, **stats}
        for tabela, stats in tabela_stats.items()
    ]

    return {
        "status": "finalizado",
        "tempo_ms": duration_ms,
        "resumo": resumo
    }



def gerar_dict_para_insercao(config : ConfiguracaoTabela, engine: Engine, db :Session, connection : DBConnection, user_id: int) -> Dict:
    """
    Gera o dicionário de valores para uma linha de INSERT,
    respeitando chaves primárias, estrangeiras e campos padronizados.
    """
    colunas = create_strutura_tabela(
        tabela_name=config.tabela,
        engine=engine,
        db=db,
        db_type=connection.type,
        db_connection_id=connection.id
    )

    # Prepara cache de FKs
    chaves_estrangeiras = {
        col.nome: obter_chaves_estrangeira(col.referenced_table, col.field_references, db, engine, connection, user_id)
        for col in colunas if col.is_foreign_key
    }

    insert_dict = {}
    for col in colunas:
        if config.camposPadronizados and any(cp.campo == col.nome for cp in config.camposPadronizados):
            valor = next(cp.valor for cp in config.camposPadronizados if cp.campo == col.nome)
        elif col.is_auto_increment and col.is_primary_key:
            continue
        elif col.is_foreign_key:
            valor = sortear_chaves_estrangeira(chaves_estrangeiras[col.nome], col)
        else:
            valor = gerar_valor_auto(col)

        insert_dict[col.nome] = {"value": valor, "type_column": col.tipo}

    return insert_dict


    
def create_strutura_tabela(tabela_name:str , engine: Engine, db: Session, db_type: str, db_connection_id:int)->List[CampoDetalhado] :
    structure = buscar_estrutura_tabela(
            db=db,
            table_name=tabela_name,
            db_connection_id=db_connection_id,
            engine=engine,
            db_type=db_type
        )
    fields_table: List[DBField] = buscar_ou_criar_campos_tabela(db, structure, engine)
    enum_map: Dict[str, List[str]] = _fetch_enum_values(db,fields_table, engine, structure, db_type)
    
    return processar_enum_fields(
            columns=fields_table,
            enum_map=enum_map
        )

def sortear_chaves_estrangeira(chaves: List, coluna: CampoDetalhado):
    if not chaves:
        return None if coluna.is_nullable else 1
    valor_fk = random.choice(chaves)
    return random.choice([valor_fk, None]) if coluna.is_nullable else valor_fk


def obter_chaves_estrangeira(
    tabela: str,
    coluna: str,
    db: Session,
    engine: Engine,
    connection: DBConnection,
    user_id: int
):
    # Query para buscar apenas 1 valor aleatório
    query = QueryPayload(
        baseTable=tabela,
        table_list=[tabela],
        select=[coluna],
        joins=[],
        where=[],
        orderBy=None,
        offset=None,  # não precisa offset se já usar random direto no SQL
        limit=1,
        distinct=None
    )

    # Executa query
    resultado = executar_query_e_salvar(
        db=db,
        user_id=user_id,
        connection=connection,
        engine=engine,
        queryrequest=query
    )
    valores = None
    if isinstance(resultado, dict):
        preview = resultado.get("preview", [])
        # se vier lista de dicts, extrai só os valores da coluna
        if isinstance(preview, list) and preview and isinstance(preview[0], dict):
            valores = [row.get(coluna) for row in preview if row.get(coluna) is not None]
        else:
            valores = preview
    else:
        valores = getattr(resultado, "preview", [])


    if not valores:
        return None  # não existe chave estrangeira disponível

    return valores


def gerar_valor_auto(
    coluna: CampoDetalhado
    ):
    """
    Gera um valor automático baseado nas regras da coluna.
    """
    # 1) Primary Key
    if coluna.is_primary_key and not coluna.is_foreign_key:
        if coluna.is_auto_increment:
            return None  # banco gera sozinho
        return gerar_valor_pelo_tipo_de_dados_na_bd(coluna)
    # 3) Enums
    opcoes = (coluna.enum_valores_encontrados or []) + (coluna.enum_valores_adicionados or [])
    if opcoes:
        return random.choice(list(set(opcoes)))

    # 4) Default
    if coluna.default:
        default_str = str(coluna.default).lower()
        if "(" in default_str and ")" in default_str:  # funções SQL
            return None
        return coluna.default

    # # 5) Nullable
    # if coluna.is_nullable:
    #     return None

    # 6) Fallback (faker baseado no tipo/nome)
    try:
        return gerar_valor_pelo_tipo_de_dados_na_bd(coluna)
    except Exception as e:
        log_message(f"Erro ao gerar valor para coluna {coluna.nome}: {e}", "error")
        return None
