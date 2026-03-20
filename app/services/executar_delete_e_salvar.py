import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import DefaultDict, List, Dict, Any, Optional, Tuple

from sqlalchemy import text, bindparam
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.schemas.query_delete_schema import PayloadDeleteRow, DeleteResponse
from app.schemas.query_select_upAndInsert_schema import QueryPayload
from app.schemas.queryhistory_schemas import QueryHistoryCreate, QueryType
from app.cruds.queryhistory_crud import create_query_history
from app.services.cloudeAi_execute_query import QueryFilterBuilder
from app.services.editar_linha import quote_identifier
from app.services.query_executor import is_safe_identifier
from app.ultils.ativar_engine import ConnectionManager
from app.ultils.build_query import get_query_string_advance
from app.ultils.errorSQL_Logger import _lidar_com_erro_sql
from app.ultils.logger import log_message

BOOL_MAP = {
    "true": True,
    "1": True,
    "yes": True,
    "sim": True,
    "false": False,
    "0": False,
    "no": False,
    "nao": False,
    "não": False,
}


class DeleteOperationService:
    def __init__(self) -> None:
        self.connection_manager = ConnectionManager()

    def _is_safe_with_dots(self, identifier: str) -> bool:
        """Permite identificadores como 'public.users' validando cada parte."""
        if not identifier:
            return False
        return all(is_safe_identifier(part) for part in str(identifier).split("."))

    def _cast_value(self, value: Any, key_type: Optional[str]) -> Any:
        if value is None:
            return None

        normalized_type = (key_type or "").strip().lower()

        if normalized_type in ("integer", "int", "bigint", "smallint"):
            return int(value)

        if normalized_type in ("float", "double", "decimal", "numeric", "real"):
            return float(value)

        normalized_value = str(value).strip().lower()
        if normalized_value in BOOL_MAP:
            return BOOL_MAP[normalized_value]

        return str(value)

    def _read_value(self, obj: Any, field: str, default: Any = None) -> Any:
        if isinstance(obj, dict):
            return obj.get(field, default)
        return getattr(obj, field, default)

    def _normalize_row_delete(self, dados: Any) -> Dict[str, Any]:
        return {
            "primaryKey": self._read_value(dados, "primaryKey"),
            "primaryKeyValue": self._read_value(dados, "primaryKeyValue"),
            "keyType": self._read_value(dados, "keyType"),
            "index": self._read_value(dados, "index"),
            "isPrimarykeyOrUnique": self._read_value(dados, "isPrimarykeyOrUnique"),
        }

    async def _build_where_clause(
        self,
        payload: QueryPayload,
        connection_type: str,
    ) -> Tuple[str, Dict[str, Any]]:
        filter_builder = QueryFilterBuilder()
        filters, params = await filter_builder.build_where_clause(
            payload.where or [],
            connection_type,
        )
        return filters, params

    async def _delete_all_by_payload(
        self,
        conn,
        payload: QueryPayload,
        connection_type: str,
    ) -> int:
        tabela = payload.baseTable

        if not self._is_safe_with_dots(tabela):
            raise ValueError(f"Tabela base inválida para exclusão total: {tabela}")

        filters, params = await self._build_where_clause(payload, connection_type)

        if not filters:
            raise ValueError("Exclusão total bloqueada: payload sem filtros.")

        sql = f"""
            DELETE FROM {quote_identifier(connection_type, tabela)}
            {filters}
        """

        log_message(f"DELETE ALL => {sql} | params={params}", "debug")
        result = conn.execute(text(sql), params)
        conn.commit()
        return result.rowcount or 0

    async def _get_pk_by_index(
        self,
        conn,
        payload: QueryPayload,
        tabela_name: str,
        pk_coluna: str,
        index: int,
        connection_type: str,
    ) -> Any:
        filters, params = await self._build_where_clause(payload, connection_type)

        query_select = get_query_string_advance(
            base_table=payload.baseTable,
            select=[pk_coluna],
            joins=payload.joins,
            aliases=payload.aliaisTables,
            filters=filters,
            table_list=payload.table_list,
            order_by=payload.orderBy,
            max_rows=1,
            offset=index,
            db_type=connection_type,
            distinct=payload.distinct,
        )

        log_message(f"SELECT PK BY INDEX => {query_select} | params={params}", "debug")
        result = conn.execute(text(query_select), params).fetchone()

        if not result:
            return None

        row_data = result._mapping
        return row_data.get(pk_coluna) or row_data.get(f"{tabela_name}.{pk_coluna}")

    async def _delete_by_index_direct(
        self,
        conn,
        payload: QueryPayload,
        primaryKey: str,
        is_pk_or_unique: bool,
        keyType: str,
        tabela_name: str,
        index: int,
        connection_type: str,
    ) -> int:
        if not self._is_safe_with_dots(tabela_name):
            raise ValueError(f"Tabela inválida/insegura: {tabela_name}")

        if not self._is_safe_with_dots(primaryKey):
            raise ValueError(f"Chave primária inválida/insegura: {primaryKey}")

        filters, params = await self._build_where_clause(payload, connection_type)

        # Se for PK ou UNIQUE, usa o método otimizado
        if is_pk_or_unique:
            return await self._delete_by_pk_or_unique(
                conn, payload, primaryKey, keyType, tabela_name, index, 
                connection_type, filters, params
            )
        else:
            # Se não for PK/UNIQUE, usa método baseado em rowid ou todas as colunas
            return await self._delete_by_rowid_or_columns(
                conn, payload, tabela_name, index, connection_type, filters, params
            )

    async def _delete_by_pk_or_unique(
        self,
        conn,
        payload: QueryPayload,
        primaryKey: str,
        keyType: str,
        tabela_name: str,
        index: int,
        connection_type: str,
        filters: str,
        params: dict,
    ) -> int:
        """Deleção otimizada usando PK ou Unique Key"""
        pk_select_options = [
            primaryKey,
            f"{tabela_name}.{primaryKey}",
            f'{tabela_name.split(".")[-1]}.{primaryKey}',
        ]

        query_select = get_query_string_advance(
            base_table=payload.baseTable,
            select=[primaryKey],
            joins=payload.joins,
            aliases=payload.aliaisTables,
            filters=filters,
            table_list=payload.table_list,
            order_by=payload.orderBy,
            max_rows=1,
            offset=index,
            db_type=connection_type,
            distinct=payload.distinct,
        )

        log_message(
            f"SELECT PK BY INDEX DIRECT => {query_select} | params={params}", "debug"
        )

        result = conn.execute(text(query_select), params).fetchone()
        if not result:
            return 0

        row_data = result._mapping

        pk_value = None
        for key in pk_select_options:
            if key in row_data and row_data[key] is not None:
                pk_value = row_data[key]
                break

        if pk_value is None:
            # fallback extra: tenta achar pela última parte do nome da coluna
            for col, value in row_data.items():
                if col.split(".")[-1] == primaryKey and value is not None:
                    pk_value = value
                    break

        if pk_value is None:
            log_message(
                f"⚠️ Não foi possível obter valor da PK '{primaryKey}' na linha do índice {index}",
                "warning",
            )
            return 0

        casted_value = self._cast_value(pk_value, keyType)

        sql = f"""
            DELETE FROM {quote_identifier(connection_type, tabela_name)}
            WHERE {quote_identifier(connection_type, primaryKey)} = :pk_value
        """

        delete_params = {"pk_value": casted_value}

        log_message(
            f"DELETE DIRECT BY INDEX USING PK => {sql} | params={delete_params}",
            "debug",
        )

        result = conn.execute(text(sql), delete_params)
        return result.rowcount or 0

    async def _delete_by_rowid_or_columns(
        self,
        conn,
        payload: QueryPayload,
        tabela_name: str,
        index: int,
        connection_type: str,
        filters: str,
        params: dict,
    ) -> int:
        """Deleção baseada em rowid (SQLite/PostgreSQL) ou todas as colunas"""
        
        # Primeiro, busca todas as colunas da linha específica
        query_select_all = get_query_string_advance(
            base_table=payload.baseTable,
            select=[],  # Seleciona todas as colunas
            joins=payload.joins,
            aliases=payload.aliaisTables,
            filters=filters,
            table_list=payload.table_list,
            order_by=payload.orderBy,
            max_rows=1,
            offset=index,
            db_type=connection_type,
            distinct=payload.distinct,
        )

        log_message(
            f"SELECT ALL COLUMNS BY INDEX => {query_select_all} | params={params}", 
            "debug"
        )

        result = conn.execute(text(query_select_all), params).fetchone()
        if not result:
            return 0

        row_data = result._mapping
        
        # Tenta usar rowid se disponível (SQLite/PostgreSQL)
        if connection_type.lower() in ['sqlite', 'postgresql']:
            # Para SQLite, tenta rowid
            if connection_type.lower() == 'sqlite':
                # Busca o rowid da linha
                query_rowid = f"""
                    SELECT rowid FROM {quote_identifier(connection_type, tabela_name)}
                    WHERE rowid IN (
                        SELECT rowid FROM {quote_identifier(connection_type, tabela_name)}
                        {filters}
                        ORDER BY {payload.orderBy or '1'}
                        LIMIT 1 OFFSET {index}
                    )
                """
                
                rowid_result = conn.execute(text(query_rowid), params).fetchone()
                if rowid_result and rowid_result[0] is not None:
                    # Deleta por rowid
                    sql = f"""
                        DELETE FROM {quote_identifier(connection_type, tabela_name)}
                        WHERE rowid = :rowid_value
                    """
                    delete_params = {"rowid_value": rowid_result[0]}
                    
                    log_message(
                        f"DELETE BY ROWID => {sql} | params={delete_params}",
                        "debug",
                    )
                    
                    result = conn.execute(text(sql), delete_params)
                    return result.rowcount or 0
        
        # Se não conseguir usar rowid, constrói WHERE clause com todas as colunas
        where_conditions = []
        delete_params = {}
        
        for i, (col, value) in enumerate(row_data.items()):
            # Ignora colunas com alias (que tenham ponto)
            if '.' in col:
                col_name = col.split('.')[-1]
            else:
                col_name = col
                
            # Para valores None, usa IS NULL
            if value is None:
                where_conditions.append(
                    f"{quote_identifier(connection_type, col_name)} IS NULL"
                )
            else:
                param_name = f"p_{i}"
                where_conditions.append(
                    f"{quote_identifier(connection_type, col_name)} = :{param_name}"
                )
                delete_params[param_name] = value
        
        if not where_conditions:
            log_message("⚠️ Nenhuma condição WHERE gerada para deleção", "warning")
            return 0
        
        sql = f"""
            DELETE FROM {quote_identifier(connection_type, tabela_name)}
            WHERE {' AND '.join(where_conditions)}
        """
        
        log_message(
            f"DELETE BY ALL COLUMNS => {sql} | params={delete_params}",
            "debug",
        )
        
        result = conn.execute(text(sql), delete_params)
        return result.rowcount or 0

   

    async def _execute_bulk_delete_by_pks(
        self,
        conn,
        tabela_name: str,
        pk_coluna: str,
        valores: List[Any],
        connection_type: str,
    ) -> int:
        if not valores:
            return 0

        sql = text(
            f"DELETE FROM {quote_identifier(connection_type, tabela_name)} "
            f"WHERE {quote_identifier(connection_type, pk_coluna)} IN :pks"
        ).bindparams(bindparam("pks", expanding=True))

        log_message(
            f"DELETE BULK => tabela={tabela_name}, pk={pk_coluna}, total={len(valores)}",
            "debug",
        )

        result = conn.execute(sql, {"pks": list(valores)})
        return result.rowcount or 0

    async def execute_delete(
        self,
        registros: List[PayloadDeleteRow],
        payloadQuery: Optional[QueryPayload],
        db: Session,
        current_user_id: int,
        delete_all: bool = False,
        app_source: str = "API",
        executed_by: str = "sistema",
        modified_by: Optional[str] = None,
        client_ip: Optional[str] = None,
    ) -> DeleteResponse:
        itens_afetados: List[Dict[str, Any]] = []
        erros: List[str] = []

        engine, connection = self.connection_manager.ensure_connection(
            db, current_user_id
        )
        connection_type = str(connection.type)
        connection_id = getattr(connection, "id", None)

        start = datetime.now(timezone.utc)

        try:
            # Se for async engine, o ideal é async with.
            # Estou assumindo que o teu engine.begin() é compatível com await nas chamadas internas.
            with engine.begin() as conn:
                if delete_all:
                    if not payloadQuery:
                        raise ValueError(
                            "Payload da query não informado para exclusão total."
                        )

                    afetados = await self._delete_all_by_payload(
                        conn=conn,
                        payload=payloadQuery,
                        connection_type=connection_type,
                    )

                    itens_afetados.append(
                        {
                            "tabela": payloadQuery.baseTable,
                            "chave": "query",
                            "valor": "delete_all",
                            "afetados": afetados,
                        }
                    )

                else:
                    grouped_pks, fallbacks, erros_extra = self._prepare_delete_groups(
                        registros=registros,
                        payload_query=payloadQuery,
                    )
                    erros.extend(erros_extra)

                    # 1) delete em lote por PKs já recebidas diretamente
                    for tabela_name, colunas in grouped_pks.items():
                        for pk_coluna, valores in colunas.items():
                            if not valores:
                                continue

                            afetados = await self._execute_bulk_delete_by_pks(
                                conn=conn,
                                tabela_name=tabela_name,
                                pk_coluna=pk_coluna,
                                valores=valores,
                                connection_type=connection_type,
                            )

                            itens_afetados.append(
                                {
                                    "tabela": tabela_name,
                                    "chave": pk_coluna,
                                    "valor": f"{len(valores)} IDs",
                                    "afetados": afetados,
                                }
                            )

                    # 2) resolver fallbacks:
                    #    primeiro tenta obter PK por index quando possível
                    fallback_pks: DefaultDict[Tuple[str, str], List[Any]] = defaultdict(
                        list
                    )
                    direct_fallbacks: List[
                        Tuple[str, Dict[str, Any], QueryPayload, int]
                    ] = []

                    for tabela_name, dados, payload, index in fallbacks:
                        pk_coluna = dados.get("primaryKey")
                        key_type = dados.get("keyType")
                        is_pk_or_unique = bool(dados.get("isPrimarykeyOrUnique"))

                        # se temos PK válida e a linha pode ser resolvida por index -> buscar valor real da PK
                        if (
                            pk_coluna
                            and is_pk_or_unique
                            and self._is_safe_with_dots(pk_coluna)
                        ):
                            try:
                                pk_valor = await self._get_pk_by_index(
                                    conn=conn,
                                    payload=payload,
                                    tabela_name=tabela_name,
                                    pk_coluna=pk_coluna,
                                    index=index,
                                    connection_type=connection_type,
                                )
                            except Exception as exc:
                                erros.append(
                                    f"Erro ao obter PK por índice em {tabela_name}.{pk_coluna} [index={index}]: {exc}"
                                )
                                pk_valor = None

                            if pk_valor is not None:
                                try:
                                    fallback_pks[(tabela_name, pk_coluna)].append(
                                        self._cast_value(pk_valor, key_type)
                                    )
                                except Exception as exc:
                                    erros.append(
                                        f"Erro ao converter PK de fallback em {tabela_name}.{pk_coluna}: {exc}"
                                    )
                                continue

                        # se não der para resolver por PK, vai para delete direto por índice
                        direct_fallbacks.append((tabela_name, dados, payload, index))

                    # 3) delete em lote das PKs resolvidas via fallback
                    for (tabela_name, pk_coluna), valores in fallback_pks.items():
                        if not valores:
                            continue

                        afetados = await self._execute_bulk_delete_by_pks(
                            conn=conn,
                            tabela_name=tabela_name,
                            pk_coluna=pk_coluna,
                            valores=valores,
                            connection_type=connection_type,
                        )

                        itens_afetados.append(
                            {
                                "tabela": tabela_name,
                                "chave": pk_coluna,
                                "valor": f"{len(valores)} IDs (fallback)",
                                "afetados": afetados,
                            }
                        )

                    # 4) delete direto por índice quando não foi possível resolver PK
                    for tabela_name, dados, payload, index in direct_fallbacks:
                        pk_coluna = dados.get("primaryKey")
                        key_type = dados.get("keyType")

                        if not pk_coluna:
                            erros.append(
                                f"Fallback por índice sem primaryKey informado para a tabela {tabela_name}"
                            )
                            itens_afetados.append(
                                {
                                    "tabela": tabela_name,
                                    "chave": "index",
                                    "valor": index,
                                    "afetados": 0,
                                }
                            )
                            continue

                        if not self._is_safe_with_dots(pk_coluna):
                            erros.append(
                                f"PrimaryKey inválida/insegura no fallback: {pk_coluna}"
                            )
                            itens_afetados.append(
                                {
                                    "tabela": tabela_name,
                                    "chave": pk_coluna,
                                    "valor": index,
                                    "afetados": 0,
                                }
                            )
                            continue

                        afetados = await self._delete_by_index_direct(
                            conn=conn,
                            payload=payload,
                            primaryKey=pk_coluna,
                            keyType=key_type or "",
                            is_pk_or_unique=False,
                            tabela_name=tabela_name,
                            index=index,
                            connection_type=connection_type,
                        )

                        itens_afetados.append(
                            {
                                "tabela": tabela_name,
                                "chave": pk_coluna,
                                "valor": f"fallback_index={index}",
                                "afetados": afetados,
                            }
                        )

            duration_ms = int(
                (datetime.now(timezone.utc) - start).total_seconds() * 1000
            )
            total_deletados = sum(
                int(item.get("afetados", 0)) for item in itens_afetados
            )
            erros_unicos = self._unique_preserve_order(erros)

            if connection_id is not None:
                historico = QueryHistoryCreate(
                    user_id=current_user_id,
                    db_connection_id=connection_id,
                    query="DELETE simple",
                    query_type=QueryType.DELETE,
                    executed_at=start,
                    updated_at=datetime.now(timezone.utc),
                    duration_ms=duration_ms,
                    result_preview=json.dumps(
                        itens_afetados,
                        default=str,
                        ensure_ascii=False,
                    ),
                    error_message=(
                        json.dumps(erros_unicos, ensure_ascii=False)
                        if erros_unicos
                        else None
                    ),
                    is_favorite=False,
                    tags="delete_simple",
                    app_source=app_source,
                    client_ip=client_ip,
                    executed_by=executed_by,
                    modified_by=modified_by or executed_by,
                    meta_info={
                        "delete_all": delete_all,
                        "erros": erros_unicos,
                        "total_deletados": total_deletados,
                    },
                )
                create_query_history(db=db, user_id=current_user_id, data=historico)

            mensagem = (
                f"{total_deletados} registro(s) excluído(s) com sucesso."
                if total_deletados > 0
                else "Nenhum registro foi excluído."
            )

            if erros_unicos:
                mensagem += f" Avisos: {', '.join(erros_unicos)}."

            return DeleteResponse(
                success=total_deletados > 0,
                mensagem=mensagem,
                itens_afetados=itens_afetados,
                executado_em=datetime.now(timezone.utc),
            )

        except SQLAlchemyError as sa_err:
            error_msg = _lidar_com_erro_sql(sa_err)
            log_message(f"Erro SQL no delete: {error_msg}", "error")
            raise RuntimeError(f"Erro SQL: {error_msg}") from sa_err

        except Exception as exc:
            log_message(f"Erro inesperado no delete: {exc}", "error")
            raise RuntimeError(str(exc)) from exc

    def _prepare_delete_groups(
        self,
        registros: List[PayloadDeleteRow],
        payload_query: Optional[QueryPayload],
    ) -> tuple[
        DefaultDict[str, DefaultDict[str, List[Any]]],
        List[Tuple[str, Dict[str, Any], QueryPayload, int]],
        List[str],
    ]:
        """
        Organiza os deletes em:
        - grouped_pks: deletes em lote por PK
        - fallbacks: deletes que dependem de index/payload
        - erros: avisos/erros coletados
        """
        grouped_pks: DefaultDict[str, DefaultDict[str, List[Any]]] = defaultdict(
            lambda: defaultdict(list)
        )
        fallbacks: List[Tuple[str, Dict[str, Any], QueryPayload, int]] = []
        erros: List[str] = []

        for registro in registros:
            payload = payload_query

            row_deletes = self._read_value(registro, "rowDeletes", {}) or {}
            tables_to_delete = self._read_value(registro, "tableForDelete", []) or []

            if row_deletes:
                for tabela_name, dados in row_deletes.items():
                    row_delete = self._normalize_row_delete(dados)

                    pk_coluna = row_delete["primaryKey"]
                    pk_valor = row_delete["primaryKeyValue"]
                    key_type = row_delete["keyType"]
                    index = row_delete["index"]
                    is_pk_or_unique = bool(row_delete["isPrimarykeyOrUnique"])

                    if not self._is_safe_with_dots(tabela_name):
                        erros.append(f"Tabela inválida/insegura: {tabela_name}")
                        continue

                    if pk_coluna and pk_valor is not None and is_pk_or_unique:
                        if not self._is_safe_with_dots(pk_coluna):
                            erros.append(f"PK inválida/insegura: {pk_coluna}")
                            continue

                        try:
                            grouped_pks[tabela_name][pk_coluna].append(
                                self._cast_value(pk_valor, key_type)
                            )
                        except Exception as exc:
                            erros.append(
                                f"Erro ao converter PK em {tabela_name}.{pk_coluna}: {exc}"
                            )
                        continue

                    if pk_coluna and pk_valor is not None and not is_pk_or_unique:
                        erros.append(
                            f"Campo {tabela_name}.{pk_coluna} não é PK/Unique. Usando fallback por index."
                        )

                    if payload is None or index is None:
                        erros.append(
                            f"Faltando index ou payload para fallback na tabela {tabela_name}"
                        )
                        continue

                    fallbacks.append((tabela_name, row_delete, payload, index))

            else:
                for tabela_name in tables_to_delete:
                    if not self._is_safe_with_dots(tabela_name):
                        erros.append(f"Tabela inválida para deleção: {tabela_name}")
                        continue

                    if payload is None:
                        erros.append(
                            f"Sem payload para consultar na tabela {tabela_name}"
                        )
                        continue

                    fallback_index = self._read_value(registro, "index")
                    if fallback_index is None:
                        erros.append(f"Sem index (posição) para a tabela {tabela_name}")
                        continue

                    fallbacks.append(
                        (
                            tabela_name,
                            {
                                "primaryKey": None,
                                "primaryKeyValue": None,
                                "keyType": None,
                                "index": fallback_index,
                                "isPrimarykeyOrUnique": False,
                            },
                            payload,
                            fallback_index,
                        )
                    )

        return grouped_pks, fallbacks, erros

    def _unique_preserve_order(self, values: List[str]) -> List[str]:
        """
        Remove duplicados preservando a ordem original.
        """
        seen = set()
        result = []

        for value in values:
            if value not in seen:
                seen.add(value)
                result.append(value)

        return result
