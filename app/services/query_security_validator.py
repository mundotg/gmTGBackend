"""
executa secury
"""

import re
from typing import Any

from app.schemas.query_select_upAndInsert_schema import QueryPayload


class QuerySecurityValidator:
    """Validador de segurança para queries SQL."""

    # Palavras reservadas perigosas
    FORBIDDEN_KEYWORDS = {
        "drop",
        "delete",
        "update",
        "insert",
        "alter",
        "truncate",
        "create",
        "grant",
        "revoke",
        "execute",
        "exec",
        "xp_",
    }

    # Padrão para identificadores válidos
    IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    @staticmethod
    def is_safe_value(value: Any, column_type: str) -> bool:
        """
        Valida se o valor fornecido é seguro para uso em SQL, baseado no tipo da coluna.
        Suporta valores como "1,3" para listas de inteiros.
        """
        # Permite None
        if value is None:
            return True
        # Tipos básicos
        if column_type in ("int", "integer", "bigint", "smallint"):
            # Suporta listas de inteiros em string separada por vírgula
            if isinstance(value, str) and "," in value:
                try:
                    return all(
                        int(v.strip()) or v.strip() == "0" for v in value.split(",")
                    )
                except Exception:
                    return False
            try:
                int(value)
                return True
            except Exception:
                return False
        if column_type in ("float", "double", "real", "numeric", "decimal"):
            # Suporta listas de floats em string separada por vírgula
            if isinstance(value, str) and "," in value:
                try:
                    return all(float(v.strip()) for v in value.split(","))
                except Exception:
                    return False
            try:
                float(value)
                return True
            except Exception:
                return False
        if column_type in ("bool", "boolean"):
            return isinstance(value, bool) or value in (
                0,
                1,
                "0",
                "1",
                "true",
                "false",
                "True",
                "False",
            )
        # Para strings, limita tamanho e caracteres perigosos
        if column_type in ("str", "string", "varchar", "text", "char"):
            if not isinstance(value, str):
                return False
            if len(value) > 1000:
                return False
            # Não permite ; ou comentários SQL
            if ";" in value or "--" in value or "/*" in value or "*/" in value:
                return False
            return True
        # Para outros tipos, aceita se não for objeto complexo
        if isinstance(value, (dict, list, set, tuple)):
            return False
        return True

    @classmethod
    def is_safe_identifier(cls, identifier: str) -> bool:
        """Valida se um identificador SQL é seguro."""
        if not identifier or not isinstance(identifier, str):
            return False

        # Verifica padrão de identificador válido
        if not cls.IDENTIFIER_PATTERN.match(identifier):
            return False

        # Verifica palavras reservadas perigosas
        return identifier.lower() not in cls.FORBIDDEN_KEYWORDS

    @classmethod
    def is_safe_qualified_identifier(cls, identifier: str, max_parts: int = 3) -> bool:
        """
        Aceita:
        - table
        - schema.table
        - schema.table.column (ou table.column)
        Rejeita qualquer coisa com espaços, aspas, SQL, etc.
        """
        if not identifier or not isinstance(identifier, str):
            return False

        parts = [p for p in identifier.split(".") if p]
        if not parts or len(parts) > max_parts:
            return False

        for part in parts:
            if not cls.is_safe_identifier(part):
                return False

        return True

    @classmethod
    def ensure_base_table_in_query(cls, payload: QueryPayload) -> QueryPayload:
        """
        Garante que payload.baseTable esteja referenciada em SELECT/WHERE.
        Se não estiver, troca pela primeira tabela válida encontrada.
        Mantém schemas intactos (ex: public.users).
        """

        def extract_table_ref(token: str) -> str | None:
            """
            Recebe coisas tipo:
            - "users" -> "users"
            - "public.users" -> "public.users"
            Se por acaso vier com coluna ("public.users.name"), remove só a coluna.
            """
            if not token:
                return None

            parts = [p for p in token.split(".") if p]

            # Se tem mais de 2 partes, assumimos que a última é a coluna.
            # Caso contrário, assumimos que é "schema.tabela" ou "tabela" e retornamos intacto.
            if len(parts) > 2:
                return ".".join(parts[:-1])
            return token

        def extract_table_from_alias_key(alias_key: str) -> str | None:
            """
            Extrai a tabela de chaves como "schema.table.column" -> "schema.table"
            ou "table.column" -> "table".
            """
            parts = [p for p in (alias_key or "").split(".") if p]
            if len(parts) >= 2:
                return ".".join(parts[:-1])
            return None

        def is_same_table(ref1: str | None, ref2: str | None) -> bool:
            """
            Compara duas tabelas ignorando o schema e aspas.
            Exemplo: 'public.usuarios' e 'usuarios' retornam True.
            """
            if not ref1 or not ref2:
                return False
            # Pega apenas a última parte (nome da tabela real) e ignora aspas/brackets
            name1 = ref1.strip('"`[]').split(".")[-1].lower()
            name2 = ref2.strip('"`[]').split(".")[-1].lower()
            return name1 == name2

        base = payload.baseTable
        base_table_in_use = False

        # ---------- check SELECT (aliases) ----------
        if payload.aliaisTables:
            for alias_key in payload.aliaisTables.keys():
                t = extract_table_from_alias_key(alias_key)
                if is_same_table(t, base):
                    base_table_in_use = True
                    break

        # ---------- check SELECT (lista simples) ----------
        if not base_table_in_use and getattr(payload, "select", None):
            for col in payload.select:
                t = extract_table_from_alias_key(col)
                if is_same_table(t, base):
                    base_table_in_use = True
                    break

        # ---------- check WHERE ----------
        if not base_table_in_use and payload.where:
            for condition in payload.where:
                if is_same_table(condition.table_name_fil, base):
                    base_table_in_use = True
                    break

        # ---------- check TABLE LIST direta ----------
        if not base_table_in_use and payload.table_list:
            for tb in payload.table_list:
                if is_same_table(tb, base):
                    base_table_in_use = True
                    break

        # ---------- if missing -> pick another ----------
        if not base_table_in_use:
            available_tables: list[str] = []

            def add_available(t_name: str | None):
                # Só adiciona se não for a tabela base e se já não existir na lista
                if t_name and not is_same_table(t_name, base):
                    if not any(
                        is_same_table(t_name, exist_t) for exist_t in available_tables
                    ):
                        available_tables.append(t_name)

            # Coleta de todas as fontes possíveis
            if payload.aliaisTables:
                for alias_key in payload.aliaisTables.keys():
                    add_available(extract_table_from_alias_key(alias_key))

            if getattr(payload, "select", None):
                for col in payload.select:
                    add_available(extract_table_from_alias_key(col))

            if payload.where:
                for condition in payload.where:
                    add_available(condition.table_name_fil)

            if payload.joins:
                for join_table in payload.joins.keys():
                    # Assegure-se de que extract_table_ref está definida no seu código
                    add_available(extract_table_ref(join_table))

            if payload.table_list:
                for table in payload.table_list:
                    add_available(extract_table_ref(table))

            if available_tables:
                # Opcional: Se quiser garantir que a nova baseTable também mantém
                # o schema se estiver disponível na table_list original, pode fazer aqui.
                payload.baseTable = available_tables[0]

        return payload

    @classmethod
    def validate_query_payload(cls, payload) -> None:
        """Valida apenas os campos críticos do payload para segurança."""
        try:
            # 1) Tabelas (aceita schema.table)
            if not cls.is_safe_qualified_identifier(payload.baseTable, max_parts=2):
                raise ValueError(f"Nome da tabela base inválido: {payload.baseTable}")

            if payload.table_list:
                for table in payload.table_list:
                    if not cls.is_safe_qualified_identifier(table, max_parts=2):
                        raise ValueError(f"Nome da tabela na lista inválido: {table}")

            # 2) Joins (estrutura básica)
            if payload.joins:
                for table_name, join_option in payload.joins.items():
                    if not cls.is_safe_qualified_identifier(table_name, max_parts=2):
                        raise ValueError(
                            f"Nome da tabela de join inválido: {table_name}"
                        )

                    for condition in join_option.conditions:
                        # leftColumn (table.column OU schema.table.column)
                        if condition.leftColumn:
                            if not cls.is_safe_qualified_identifier(
                                condition.leftColumn, max_parts=3
                            ):
                                raise ValueError(
                                    f"Coluna inválida em leftColumn: {condition.leftColumn}"
                                )

                        # rightColumn (quando não usa value)
                        if not condition.useValue and condition.rightColumn:
                            if not cls.is_safe_qualified_identifier(
                                condition.rightColumn, max_parts=3
                            ):
                                raise ValueError(
                                    f"Coluna inválida em rightColumn: {condition.rightColumn}"
                                )

            # 3) WHERE
            if payload.where:
                for condition in payload.where:
                    # table_name_fil pode vir schema.table
                    if not cls.is_safe_qualified_identifier(
                        condition.table_name_fil, max_parts=2
                    ):
                        raise ValueError(
                            f"Nome da tabela no filtro inválido: {condition.table_name_fil}"
                        )

                    # column normalmente é só "nome", "email"... (1 parte)
                    # mas se em algum cenário vier qualificado, aceitamos 1..3 pra compatibilidade
                    if not cls.is_safe_qualified_identifier(
                        condition.column, max_parts=3
                    ):
                        raise ValueError(
                            f"Nome da coluna no filtro inválido: {condition.column}"
                        )

                    # valores em operadores de risco
                    if condition.operator in ["IN", "NOT IN"] and condition.value:
                        if isinstance(condition.value, list):
                            for val in condition.value:
                                if not cls.is_safe_value(val, condition.column_type):
                                    raise ValueError(
                                        f"Valor inválido na condição IN: {val}"
                                    )
                        else:
                            if not cls.is_safe_value(
                                condition.value, condition.column_type
                            ):
                                raise ValueError(f"Valor inválido: {condition.value}")

            # 4) Aliases/colunas no SELECT (aliaisTables)
            if payload.aliaisTables:
                for alias, original in payload.aliaisTables.items():
                    # aqui alias está a ser usado como "coluna selecionada" (ex: public.users.nome)
                    if not cls.is_safe_qualified_identifier(alias, max_parts=3):
                        raise ValueError(f"Alias/coluna inválido: {alias}")

                    # opcional: validar também o "original" se tu realmente usas isso no SQL
                    if original and isinstance(original, str):
                        if not cls.is_safe_qualified_identifier(original, max_parts=3):
                            raise ValueError(
                                f"Original inválido em aliaisTables: {original}"
                            )

        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Erro na validação do payload: {e}")
