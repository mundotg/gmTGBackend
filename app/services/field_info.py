from datetime import datetime, timezone
import traceback
from typing import Tuple
from typing import Dict, List
from fastapi import HTTPException
from sqlalchemy import  inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.engine.reflection import Inspector
from app.config.dependencies import EngineManager
from app.cruds.connection_cruds import get_active_connection_by_userid, get_db_connection_by_id
from app.cruds.dbstructure_crud import create_db_field, create_db_structure, create_enum_field, delete_field_name, delete_structure_by_name, get_db_structures_by_conn_id_and_table, get_fields_by_structure, list_enum_fields_by_field
from app.models.dbstructure_models import   DBEnum_field, DBField,  DBStructure
from app.schemas.dbstructure_schema import  CampoDetalhado, DBFieldCreate, DBStructureCreate, MetadataTableResponse
from app.services.fields_estruture import _is_system_field, obter_schema_do_engine
from app.services.pesquizar_index_linha_in_bd import _get_foreign_keys
from app.ultils.build_query import _build_enum_query, _parse_enum_result
from app.ultils.logger import log_message
from app.ultils.ativar_session_bd import reativar_connection
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError

def sincronizar_metadados_da_tabela(
    db: Session, table_name: str, user_id: int
) -> dict:
    try:
        # 1. Obtém conexão ativa do usuário
        active_connection = get_active_connection_by_userid(db, user_id)
        db_connection_id = active_connection.connection_id
        engine: Engine = EngineManager.get(user_id)

        if not engine:
            sucesso = reativar_connection(user_id, db)
            if sucesso.get("success"):
                engine = EngineManager.get(user_id)
            else:
                raise RuntimeError("❌ Não foi possível reativar a conexão com o banco de dados.")

        # 2. Obtém o tipo do banco de dados
        connection = get_db_connection_by_id(db, db_connection_id)
        db_type = connection.type if connection else "postgresql"  # fallback seguro

        # 3. Busca ou cria a estrutura da tabela
        structure = buscar_estrutura_tabela(
            db=db,
            table_name=table_name,
            db_connection_id=db_connection_id,
            engine=engine,
            db_type=db_type
        )
        

        # 4. Busca ou cria os campos
        fields_table: List[DBField] = buscar_ou_criar_campos_tabela(db, structure, engine)

        # 5. Busca valores ENUM por coluna
        enum_map: Dict[str, List[str]] = _fetch_enum_values(db, fields_table, engine, structure, db_type)

        # 6. Processa e sincroniza campos ENUM
        resposta_colunas = processar_enum_fields(
            columns=fields_table,
            enum_map=enum_map
        )

        # 7. Monta e retorna a resposta final
        return montar_resposta_sincronizacao(
            db_connection_id=db_connection_id,
            schema_name=structure.schema_name,
            table_name=table_name,
            resposta_colunas=resposta_colunas,
            total_adicionado=len(resposta_colunas)
        )

    except SQLAlchemyError as e:
        # db.rollback()
        # traceback.print_exc()
        log_message(f"❌ Erro de banco de dados ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}", "error")
        raise RuntimeError(f"❌ Erro de banco de dados ao sincronizar a tabela '{table_name}': {str(e)}")

    except Exception as e:
        # traceback.print_exc()
        log_message(f"❌ Erro inesperado ao sincronizar a tabela '{table_name}': {str(e)}{traceback.format_exc()}", "error")
        raise RuntimeError(f"❌ Erro inesperado ao sincronizar a tabela '{table_name}': {str(e)}")


def montar_resposta_sincronizacao(
    db_connection_id: int,
    schema_name: str,
    table_name: str,
    resposta_colunas: List[CampoDetalhado],
    total_adicionado: int
) -> dict:
    nomes = [col.nome for col in resposta_colunas]  # Correção aqui
    duplicados = {nome for nome in nomes if nomes.count(nome) > 1}

    # if duplicados:
    #     print(f"Colunas duplicadas detectadas: {', '.join(duplicados)}")
    
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
        is_enum = "enum" in col_type

        valores_encontrados = enum_map.get(col_name, []) if is_enum else []
        valores_adicionados = []

        if is_enum:
            # Insere os novos valores no banco local
            for valor in valores_encontrados:
                valores_adicionados.append(valor)

        # Adiciona metadado da coluna para resposta
        resposta_colunas.append(CampoDetalhado(
            nome=col_name,
            tipo=col_type,
            is_nullable=column.is_nullable,
            is_unique=column.is_unique,
            is_primary_key=column.is_primary_key,
            default=column.default_value,
            comentario=column.comment,
            length=column.length,
            enum_valores_encontrados=valores_encontrados,
            enum_valores_adicionados=valores_adicionados,
        ))

    return resposta_colunas


def buscar_estrutura_tabela(
    db: Session, table_name: str, db_connection_id: int,engine:Engine,db_type: str
) -> DBStructure:
    """
    Busca ou cria a estrutura da tabela na base local.
    """
    structure = get_db_structures_by_conn_id_and_table(db, db_connection_id, table_name)
    
    if not structure:
        # Usa o schema da conexão, se possível
        schema_name = obter_schema_do_engine(engine, db_type)
        # Cria a estrutura da tabela localmente
        delete_structure_by_name(db, table_name, db_connection_id)
        structure = create_db_structure(
            db,
            DBStructureCreate( table_name=table_name, schema_name=schema_name, db_connection_id=db_connection_id)
        )
    return structure

def buscar_ou_criar_campos_tabela(
    db: Session,
    structure: DBStructure,
    engine: Engine
) -> List[DBField]:
    """
    Busca os campos de uma tabela no banco. Se não existirem localmente,
    insere todos os campos com base nos metadados do banco de dados.
    """

    # Verifica se já existem campos locais
    campos_existentes = get_fields_by_structure(db, structure.id)
    # if campos_existentes:
    #     return campos_existentes

    # 🔹 Buscar chaves estrangeiras já organizadas: { "coluna": "tabela_referenciada" }
    foreign_keys_map = _get_foreign_keys(engine, structure.table_name)
    foreign_keys = foreign_keys_map.get(structure.table_name, {})
    print(f"🔍 Chaves estrangeiras encontradas: {foreign_keys}")

    try:
        inspector: Inspector = inspect(engine)
        columns = inspector.get_columns(structure.table_name, schema=structure.schema_name)
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
    fk_columns = set()
    for fk in foreign_keys_info:
        for col in fk.get("constrained_columns", []):
            fk_columns.add(col)

    # Processa cada coluna
    for column in columns:
        col_name = column["name"]
        column_type = column["type"]
        length = getattr(column_type, "length", None)
        precision = getattr(column_type, "precision", None)
        scale = getattr(column_type, "scale", None)
        is_primary_key = col_name in primary_keys
        is_unique = col_name in unique_columns
        is_ForeignKey = col_name in fk_columns or col_name in foreign_keys

        is_auto_incremento = _is_system_field(
            col_name,
            str(column_type),
            structure.connection.type,
            structure.table_name,
            engine
        )

        # Apaga o campo se já existir com mesmo nome
        delete_field_name(db, col_name, structure.id)

        # Cria o novo campo
        field_in = DBFieldCreate(
            name=col_name,
            type=str(column_type),
            is_nullable=column.get("nullable", True),
            default_value=column.get("default"),
            is_primary_key=is_primary_key,
            comment=column.get("comment", "") or "",
            referenced_table=foreign_keys.get(col_name),
            is_unique=is_unique,
            is_ForeignKey=is_ForeignKey,
            is_auto_increment=is_auto_incremento,
            length=length,
            precision=precision,
            scale=scale
        )

        field = create_db_field(db=db, field_in=field_in, structure_id=structure.id)
        campos_resultantes.append(field_in)

        print(f"🟢 Campo criado: {col_name} (FK: {is_ForeignKey}", end="")
        if is_ForeignKey and col_name in foreign_keys:
            print(f" → {foreign_keys[col_name]})")
        else:
            print(")")

    return campos_resultantes




def _fetch_enum_values(
    db: Session, columns: List[DBField],
    engine: Engine, structure: DBStructure,  db_type: str
) -> Dict[str, List[str]]:
    """
    Obtém os valores ENUM de cada coluna ENUM da tabela.
    Se já existirem valores no banco local, retorna esses.
    Caso contrário, busca diretamente no banco de dados.
    """
    enum_values: Dict[str, List[str]] = {}

    try:
        for col in columns:
            if "enum" not in col.type.lower():
                continue

            # 1. Busca valores já salvos localmente
            enums_local = list_enum_fields_by_field(db, col.id)
            if enums_local:
                enum_values[col.name] = [e.valor for e in enums_local]
                continue

            # 2. Caso não existam localmente, gera a query
            query = _build_enum_query(col.name, structure.table_name, db_type)
            if not query:
                continue

            with engine.connect() as conn:
                result = conn.execute(query).fetchall()

                if result:
                    valores_enum = _parse_enum_result(db_type, result, col.name)
                    enum_values[col.name] = []

                    for valor in valores_enum:
                        # Insere valor no banco local
                        enum_model = DBEnum_field(field_id=col.id, valor=valor)
                        create_enum_field(db, enum_model)
                        enum_values[col.name].append(valor)

        log_message(f"🟢 Valores ENUM sincronizados: {enum_values}", "info")
        return enum_values

    except Exception as e:
        log_message(f"❌ Erro ao buscar ENUMs: {e}\n{traceback.format_exc()}", "warning")
        return {}
