from datetime import datetime, timezone
import traceback
from typing import Dict, List, Optional
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
# from app.config.cache_manager import cache_result
from app.config.cache_manager import cache_result
from app.cruds.dbstructure_crud import create_db_field, create_db_structure, delete_structure_by_name, get_db_structures_by_conn_id_and_table, get_fields_by_structure
from app.models.dbstructure_models import   DBField,  DBStructure
from app.schemas.dbstructure_schema import  CampoDetalhado, DBFieldCreate, MetadataTableResponse
from app.services.fields_estruture import  _is_system_field_from_column, obter_schema_do_engine
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.buscar_enum_bd import _fetch_enum_values
from app.ultils.logger import log_message
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
from sqlalchemy.exc import OperationalError
 
def safe_get_columns(engine: Engine, table_name: str, schema: str):
    inspector = inspect(engine)
    try:
        return inspector.get_columns(table_name, schema=schema),inspector
    except OperationalError as e:
        if "SSL connection has been closed" in str(e):
            # 🔄 recria o inspector e tenta de novo
            inspector = inspect(engine)
            return inspector.get_columns(table_name, schema=schema),inspector
        raise

def sincronizar_metadados_da_tabela(
    db: Session, table_name: str, user_id: int
) -> dict:
    try:
        # 1. Obtém conexão ativa do usuário
        engine,connection = ConnectionManager.ensure_connection(db, user_id)

        # 2. Obtém o tipo do banco de dados
        db_type = connection.type if connection else "postgresql"  # fallback seguro

        # 3. Busca ou cria a estrutura da tabela
        structure = buscar_estrutura_tabela(
            db=db,
            table_name=table_name,
            db_connection_id=connection.id,
            engine=engine,
            db_type=db_type
        )
        
        # 4. Busca ou cria os campos
        fields_table: List[DBField] = buscar_ou_criar_campos_tabela(db, structure, engine, str(db_type))

        # 5. Busca valores ENUM por coluna
        enum_map: Dict[str, List[str]] = _fetch_enum_values(db,fields_table, engine, structure, db_type)

        # 6. Processa e sincroniza campos ENUM
        resposta_colunas = processar_enum_fields(
            columns=fields_table,
            enum_map=enum_map
        )

        # 7. Monta e retorna a resposta final
        return montar_resposta_sincronizacao(
            db_connection_id=connection.id,
            schema_name=structure.schema_name,
            table_name=table_name,
            resposta_colunas=resposta_colunas,
            total_adicionado=len(resposta_colunas)
        )

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro de banco de dados ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}", "error")
        raise RuntimeError(f"❌ Erro de banco de dados ao sincronizar a tabela '{table_name}': {str(e)}")

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro inesperado ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}", "error")
        raise RuntimeError(f"❌ Erro inesperado ao sincronizar a tabela '{table_name}': {str(e)}")
    
def get_fields_of_table(
    db: Session, table_name: str, user_id: int,connection_id:int
) -> List[DBField]:
    """
    Obtém os campos de uma tabela específica para um usuário e conexão.
    """
    # 1. Obtém conexão ativa do usuário
    engine,connection = ConnectionManager.ensure_idConn_connection(db, user_id,connection_id)

    # 2. Obtém o tipo do banco de dados
    db_type = connection.type if connection else "postgresql"  # fallback seguro

    # 3. Busca ou cria a estrutura da tabela
    structure = buscar_estrutura_tabela(
        db=db,
        table_name=table_name,
        db_connection_id=connection.id,
        engine=engine,
        db_type=db_type
    )
    
    # 4. Busca ou cria os campos
    fields_table: List[DBField] = buscar_ou_criar_campos_tabela(db, structure, engine,str(db_type))

    return fields_table


def get_fields_of_tables_bulk(
    db: Session,
    table_names: List[str],
    user_id: int,
    connection_id: int,
) -> Dict[str, List[DBField]]:
    """
    Obtém os campos de várias tabelas (bulk) para um usuário e conexão.
    Retorna um dicionário: { table_name: [DBField, ...] }
    """

    if not table_names:
        return {}

    # 1️⃣ Obtém engine e conexão UMA ÚNICA VEZ
    engine, connection = ConnectionManager.ensure_idConn_connection(
        db=db, user_id=user_id, id_connection=connection_id
    )

    db_type = connection.type if connection else "postgresql"

    result: Dict[str, List[DBField]] = {}

    # 2️⃣ Processa tabela por tabela, reaproveitando engine
    for table_name in table_names:
        try:
            # 3️⃣ Busca ou cria estrutura da tabela
            structure = buscar_estrutura_tabela(
                db=db,
                table_name=table_name,
                db_connection_id=connection.id,
                engine=engine,
                db_type=db_type,
            )

            if not structure:
                result[table_name] = []
                continue

            # 4️⃣ Busca ou cria campos da tabela
            fields_table: List[DBField] = buscar_ou_criar_campos_tabela(
                db=db,
                structure=structure,
                engine=engine,db_type=str(db_type)
            )

            result[table_name] = fields_table or []

        except Exception as e:
            # ⚠️ erro isolado não quebra o batch inteiro
            log_message(
                f"❌ Erro ao obter campos da tabela '{table_name}': {str(e)}{traceback.format_exc()}",
                "error"
            )
            result[table_name] = []

    return result

    
    
@cache_result(ttl=150800, user_id="user_metadados_da_tabela_{user_id}")    
def sincronizar_metadados_da_tabela_simple(
    db: Session, table_name: str, user_id: int,connection_id:int
) -> dict:
    try:
        # 1. Obtém conexão ativa do usuário
        engine,connection = ConnectionManager.ensure_idConn_connection(db, user_id,connection_id)

        # 2. Obtém o tipo do banco de dados
        db_type = connection.type if connection else "postgresql"  # fallback seguro

        # 3. Busca ou cria a estrutura da tabela
        structure = buscar_estrutura_tabela(
            db=db,
            table_name=table_name,
            db_connection_id=connection.id,
            engine=engine,
            db_type=str(db_type)
        )
        
        # 4. Busca ou cria os campos
        fields_table: List[DBField] = buscar_ou_criar_campos_tabela(db, structure, engine,str(db_type))

        # 5. Busca valores ENUM por coluna
        enum_map: Dict[str, List[str]] = _fetch_enum_values(db,fields_table, engine, structure, db_type)

        # 6. Processa e sincroniza campos ENUM
        resposta_colunas = processar_enum_fields(
            columns=fields_table,
            enum_map=enum_map
        )

        # 7. Monta e retorna a resposta final
        return montar_resposta_sincronizacao(
            db_connection_id=connection.id,
            schema_name=structure.schema_name,
            table_name=table_name,
            resposta_colunas=resposta_colunas,
            total_adicionado=len(resposta_colunas)
        )

    except SQLAlchemyError as e:
        db.rollback()
        log_message(f"❌ Erro de banco de dados ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}", "error")
        raise RuntimeError(f"❌ Erro de banco de dados ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}")

    except Exception as e:
        db.rollback()
        log_message(f"❌ Erro inesperado ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}", "error")
        raise RuntimeError(f"❌ Erro inesperado ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}")


def montar_resposta_sincronizacao(
    db_connection_id: int,
    schema_name: str,
    table_name: str,
    resposta_colunas: List[CampoDetalhado],
    total_adicionado: int
) -> dict:
 
    return MetadataTableResponse(
        message="Metadados obtidos com sucesso",
        executado_em=datetime.now(timezone.utc),
        connection_id=db_connection_id,
        schema_name=schema_name,
        table_name=table_name,
        total_colunas=total_adicionado,
        colunas=resposta_colunas
    ).model_dump()
    

def processar_enum_fields(
    columns: List[DBField],
    enum_map: Dict[str, List[str]]
) -> List[CampoDetalhado]:
    """
    Processa colunas ENUM da estrutura de tabela, sincronizando novos valores
    no banco local e retornando os metadados detalhados.
    
    Retorna:
        - Lista de campos detalhados (`CampoDetalhado`)
        - Total de valores ENUM adicionados
    """
    resposta_colunas: List[CampoDetalhado] = []

    for column in columns:
        col_name = column.name
        col_type = column.type.lower()

        valores_encontrados = enum_map.get(col_name, []) 
        valores_adicionados = []

        
        for valor in valores_encontrados:
            valores_adicionados.append(valor)
        valor_editado =CampoDetalhado(
            nome=col_name,
            tipo=col_type,
            is_nullable=column.is_nullable,
            is_unique=column.is_unique,
            is_primary_key=column.is_primary_key,
            is_foreign_key=column.is_foreign_key,
            referenced_table=column.referenced_table,
            field_references=column.referenced_field,
            on_delete_action=column.fk_on_delete,
            on_update_action=column.fk_on_update,
            is_auto_increment=column.is_auto_increment,
            default=column.default_value,
            comentario=column.comment,
            length=column.length,
            enum_valores_encontrados=valores_encontrados,
        )
        # Adiciona metadado da coluna para resposta
        resposta_colunas.append(valor_editado)

    return resposta_colunas

def buscar_estrutura_tabela(
    db: Session, 
    table_name: str, 
    db_connection_id: int, 
    engine: Engine, 
    db_type: str
) -> DBStructure:
    """
    Busca ou cria a estrutura da tabela na base local.
    """
    structure = get_db_structures_by_conn_id_and_table(db, db_connection_id, table_name)
    
    if not structure:
        # Usa o schema da conexão, se possível
        schema_name = obter_schema_do_engine(engine, db_type)
        
        # Opcional: Se 'create_db_structure' já lida com reativação de deletados, 
        # essa linha abaixo pode ser redundante, mas mantive conforme seu código original.
        delete_structure_by_name(db, table_name, db_connection_id)
        
        # --- CORREÇÃO AQUI ---
        # Removemos o DBStructureCreate(...) e passamos os argumentos nomeados diretamente.
        structure = create_db_structure(
            db=db,
            db_connection_id=db_connection_id,
            table_name=table_name,
            schema_name=schema_name
            # Se sua função create_db_structure tiver outros campos obrigatórios 
            # (como description, charset, etc), adicione-os aqui.
        )
        
    return structure

def map_column_type(col_type: str, db_type: str) -> str:
    """
    Converte o tipo de coluna da base para um tipo genérico.
    Para SQL Server, converte 'integer' em 'int'.
    """
    # normaliza para evitar problema de maiúscula/minúscula
    col_typelower = col_type.lower()
    db_typelower = db_type.lower()

  

    # regra especial para SQL Server
    if db_typelower in ("sqlserver", "mssql"):
        if col_typelower == "integer":
            return "int"
    # retorna mapeado ou o próprio tipo se não achar
    return col_type.strip('"')


def buscar_ou_criar_campos_tabela(
    db: Session,
    structure: DBStructure,
    engine: Engine,
    db_type: Optional[str]
) -> List[DBField]:
    """
    Busca os campos de uma tabela no banco. Se não existirem localmente,
    insere todos os campos com base nos metadados do banco de dados.
    """
    # Verifica se já existem campos locais
    campos_existentes = get_fields_by_structure(db, structure.id)
    if campos_existentes:
        return campos_existentes

    try:
        columns,inspector = safe_get_columns(engine,structure.table_name, schema=structure.schema_name)
    except NoSuchTableError as ex:
        raise ValueError(
            f"Tabela '{structure.table_name}' não encontrada no banco.  {ex}{traceback.format_exc()}"
        )
    except SQLAlchemyError as e:
        raise RuntimeError(
            f"Erro ao obter colunas da tabela '{structure.table_name}': {e}{traceback.format_exc()}"
        )

    campos_resultantes: List[DBField] = []

    # Chaves primárias
    pk_constraint = inspector.get_pk_constraint(structure.table_name, schema=structure.schema_name)
    primary_keys = pk_constraint.get("constrained_columns", []) or []

    # Chaves únicas
    try:
        unique_constraints = inspector.get_unique_constraints(
            structure.table_name, schema=structure.schema_name
        )
    except NotImplementedError:
        unique_constraints = []
        log_message("⚠️ Este dialeto não implementa 'get_unique_constraints'. Continuando sem verificar unicidade...", level="warning")

    unique_columns = set()
    for constraint in unique_constraints:
        for col in constraint.get("column_names", []):
            unique_columns.add(col)

    # Chaves estrangeiras (fallback extra, já usamos `_get_foreign_keys`)
    foreign_keys_info = inspector.get_foreign_keys(structure.table_name, schema=structure.schema_name)
    relations_map = {}  # mapa de relações encontradas

    for fk in foreign_keys_info:
        tabela_relacao = fk["referred_table"]
        for col_origem, col_relacao in zip(
            fk.get("constrained_columns", []),
            fk.get("referred_columns", []),
            
            
        ):
            relation = {
                "fieldTabelaOrigem": col_origem,
                "tabelaRelacao": tabela_relacao,
                "fieldTabelaRelacao": col_relacao,
                "ondelete": fk.get("options", {}).get("ondelete"),
                "onupdate": fk.get("options", {}).get("onupdate"),
            }
            relations_map[col_origem] = relation
    
    
    # Processa cada coluna
    for column in columns:
        col_name = column["name"]
        column_type = column["type"]
        length = getattr(column_type, "length", None)
        precision = getattr(column_type, "precision", None)
        scale = getattr(column_type, "scale", None)
        is_primary_key = col_name in primary_keys
        is_unique = col_name in unique_columns
        relation = relations_map.get(col_name)
        is_foreign_key = True if relation else False
        tipo = column_type.compile(dialect=engine.dialect)
        default_value =column.get("default")
        is_auto_incremento = _is_system_field_from_column(tipo, column,is_foreign_key)
        # Cria o novo campo
        field_in = DBFieldCreate(
            name=col_name,
            type=map_column_type(tipo, db_type or structure.connection.type),
            is_nullable=column.get("nullable", True),
            default_value=default_value,
            is_primary_key=is_primary_key,
            comment=column.get("comment", "") or "",
            referenced_table=relation.get("tabelaRelacao") if is_foreign_key else None,
            referenced_field=relation.get("fieldTabelaRelacao") if is_foreign_key else None,
            fk_on_delete=relation.get("ondelete") if is_foreign_key else None,
            fk_on_update=relation.get("onupdate") if is_foreign_key else None,
            is_unique=is_unique,
            is_foreign_key=bool(is_foreign_key),
            is_auto_increment=is_auto_incremento,
            length=length,
            precision=precision,
            scale=scale
        )

        field_create =create_db_field(db=db, field_in=field_in, structure_id=structure.id)
        campos_resultantes.append(field_create)
    return campos_resultantes

