# services/insert_service.py
import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union
from sqlalchemy.orm import Session
from sqlalchemy import Engine
from app.config.offset_cache_file import salvar_cache_binario
from app.models.connection_models import DBConnection
from app.models.dbstructure_models import DBField
from app.schemas.dbstructure_schema import CampoDetalhado
from app.schemas.query_select_upAndInsert_schema import AutoCreateRequest, ConfiguracaoTabela, QueryPayload
from app.schemas.queryhistory_schemas import  QueryHistoryCreate
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
    dry_run: bool = False,
    batch_size: int = 70  # ✅ tamanho máximo de lote
):
    """
    Insere linhas automaticamente em lotes otimizados.
    Evita lentidão e travamento ao inserir grandes quantidades.
    """
    tabela_stats = defaultdict(lambda: {"sucesso": 0, "erros": 0, "simulacao": 0})
    query_string = []
    start = time.time()
    total_queries = 0
    total_erros = 0

    with engine.connect() as conn:
        for config in data.configs:
            log_message(f"Iniciando geração de {config.quantidade} linhas em {config.tabela}", "info")

            buffer_queries = []  # acumula queries para commit em lote

            for i in range(config.quantidade):
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
                    total_queries += 1

                    if dry_run:
                        tabela_stats[config.tabela]["simulacao"] += 1
                    else:
                        buffer_queries.append(query)
                        # ✅ Quando atingir o limite do lote → executa tudo
                        if len(buffer_queries) >= batch_size:
                            for q in buffer_queries:
                                conn.execute(q)
                            conn.commit()
                            tabela_stats[config.tabela]["sucesso"] += len(buffer_queries)
                            exemplo = buffer_queries[0][:90]
                            log_message(f"Buffer atual contém {len(buffer_queries)} queries. Exemplo: {exemplo}", "info")
                            buffer_queries.clear()

                    # Log periódico
                    if (i + 1) % batch_size == 0:
                        log_message(f"{i + 1}/{config.quantidade} inserções processadas em {config.tabela}", "info")

                except Exception as e:
                    error_msg = _lidar_com_erro_sql(e)
                    log_message(f"Erro ao inserir em {config.tabela}: {error_msg}", "error")
                    tabela_stats[config.tabela]["erros"] += 1
                    total_erros += 1

            # ✅ Commit final do que restar no buffer
            if buffer_queries:
                for q in buffer_queries:
                    conn.execute(q)
                conn.commit()
                exemplo = buffer_queries[0][:90]
                log_message(f"Buffer atual contém {len(buffer_queries)} queries. Exemplo: {exemplo}", "info")
            log_message(f"Finalizado {config.quantidade} inserções em {config.tabela}", "success")

    duration_ms = int((time.time() - start) * 1000)

    # ✅ Monta metadados e salva histórico completo
    try:
        historico = QueryHistoryCreate(
            user_id=user_id,
            db_connection_id=connection.id,
            query="\n".join(query_string) if query_string else "Nenhuma query gerada",
            query_type="INSERT",
            executed_at=datetime.now(timezone.utc),
            duration_ms=duration_ms,
            result_preview=json.dumps(dict(tabela_stats), ensure_ascii=False),
            error_message=None if total_erros == 0 else f"{total_erros} erros durante a execução",
            is_favorite=False,
            tags="insert_auto",
            app_source="API",
            client_ip=getattr(data, "client_ip", None),
            executed_by=getattr(data, "executed_by", f"user_{user_id}"),
            modified_by=None,
            meta_info={
                "dry_run": dry_run,
                "batch_size": batch_size,
                "total_queries": total_queries,
                "total_erros": total_erros,
                "tempo_execucao_ms": duration_ms,
                "tabelas_afetadas": [cfg.tabela for cfg in data.configs],
                "connection_type": connection.type,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        create_query_history(db=db, user_id=user_id, data=historico)
        log_message("🗃️ Histórico de inserção salvo com sucesso.", "success")

    except Exception as hist_err:
        log_message(f"⚠️ Falha ao salvar histórico: {hist_err}", "warning")

    resumo = [{"tabela": tabela, **stats} for tabela, stats in tabela_stats.items()]

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
            valor = gerar_valor_auto(coluna=col,  tabela_name=config.tabela)

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


def sortear_chaves_estrangeira(chaves: List, coluna) -> Optional[Union[int, str, None]]:
    """
    Sorteia uma chave estrangeira aleatória da lista.
    Reduz a chance de retornar None quanto mais chaves existirem.

    Args:
        chaves: Lista de chaves estrangeiras disponíveis.
        coluna: Objeto com atributo 'is_nullable' indicando se pode ter valor nulo.

    Returns:
        Uma chave estrangeira aleatória ou None (se permitido e sorteado).
    """
    # Nenhuma chave disponível
    if not chaves:
        return None if getattr(coluna, "is_nullable", False) else 1

    # Escolhe uma chave aleatória
    valor_fk = random.choice(chaves)

    # Só aplica chance de None se a coluna permitir nulos
    if getattr(coluna, "is_nullable", False):
        # Base: começa com 30% de chance
        base_prob = 0.3
        
        # Reduz a chance conforme a quantidade de chaves
        # Ex: se tiver 10 chaves, chance cai para 3%, se tiver 100 chaves → 0.3%
        prob_nulo = base_prob / max(len(chaves), 1)

        # Decide se retorna None
        if random.random() < prob_nulo:
            return None

    return valor_fk

obter_chaves_estrangeira_offset_cache: Dict[str, Dict[str, int]] = {}
def obter_chaves_estrangeira(
    tabela: str,
    coluna: str,
    db: Session,
    engine: Engine,
    connection: DBConnection,
    user_id: int
):
    tabela_cache = obter_chaves_estrangeira_offset_cache.setdefault(tabela, {})
    offset_atual = tabela_cache.get(coluna, 0)

    # Define um pulo aleatório (ex: 0 a 5 blocos de 40)
    # Define um pulo aleatório (0 a 5 blocos de 40)
    pulo = random.randint(0, 5) * 40

    # Decide aleatoriamente se o pulo será positivo ou negativo
    direcao = random.choice([-1, 1])

    # Calcula o novo offset, garantindo que nunca fique abaixo de 0
    novo_offset = max(0, offset_atual + (pulo * direcao))

    # Define a query
    query = QueryPayload(
        baseTable=tabela,
        table_list=[tabela],
        select=[coluna],
        joins={},
        where=[],
        orderBy=None,
        offset=novo_offset,
        limit=40,
        distinct=None
    )

    # Executa a query
    resultado = executar_query_e_salvar(
        db=db,
        user_id=user_id,
        connection=connection,
        engine=engine,
        queryrequest=query
    )

    # Extrai os valores retornados
    valores = None
    if isinstance(resultado, dict):
        preview = resultado.get("preview", [])
        if isinstance(preview, list) and preview and isinstance(preview[0], dict):
            valores = [row.get(coluna) for row in preview if row.get(coluna) is not None]
        else:
            valores = preview
    else:
        valores = getattr(resultado, "preview", [])

    # Se não houver resultados, retorna None
    if not valores:
        return None

    # Atualiza o cache em memória e grava no ficheiro binário
    tabela_cache[coluna] = novo_offset
    salvar_cache_binario(obter_chaves_estrangeira_offset_cache)

    return valores


def gerar_valor_auto(
    coluna: CampoDetalhado, como_strategy: bool = False, tabela_name: str = ""
    ):
    """
    Gera um valor automático baseado nas regras da coluna.
    """
    # 1) Primary Key
    if coluna.is_primary_key and not coluna.is_foreign_key:
        if coluna.is_auto_increment:
            return None  # banco gera sozinho
        return gerar_valor_pelo_tipo_de_dados_na_bd(coluna, como_strategy, tabela_name)
    # 3) Enums
    opcoes = (coluna.enum_valores_encontrados or []) 
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
        return gerar_valor_pelo_tipo_de_dados_na_bd(coluna , como_strategy, tabela_name)
    except Exception as e:
        log_message(f"Erro ao gerar valor para coluna {coluna.nome}: {e}", "error")
        return None
