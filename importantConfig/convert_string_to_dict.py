from __future__ import annotations

import json
from typing import Dict, Tuple, List, Any, Optional

from app.schemas.db_transfer_schema import ColumnMapping, TableMapping

# supondo que existem:
# class ColumnMapping(...)
# class TableMapping(...)

class PayloadError(ValueError):
    """Erro de payload/validação para requests de transferência."""
    pass


def _as_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def converter_tables_origen(
    tables_origen_str: str,
    *,
    strict: bool = False,   # strict=False => tenta continuar; strict=True => falha no primeiro erro sério
) -> Tuple[Dict[str, TableMapping], List[str]]:
    """
    Converte JSON do payload para Dict[id_tabela_origem, TableMapping].

    Retorna: (result, warnings)

    - strict=False: ignora partes inválidas e segue (modo "só funcionar")
    - strict=True: lança PayloadError ao encontrar inconsistências importantes
    """
    warnings: List[str] = []

    if tables_origen_str is None or not str(tables_origen_str).strip():
        msg = "Campo 'tabelas' vazio ou ausente."
        if strict:
            raise PayloadError(msg)
        return {}, [msg]

    # 1) Parse JSON
    try:
        data = json.loads(tables_origen_str)
    except json.JSONDecodeError as e:
        raise PayloadError(f"JSON inválido em 'tabelas': {e.msg} (linha {e.lineno}, coluna {e.colno})") from e

    # 2) Estrutura raiz precisa ser dict
    if not isinstance(data, dict):
        raise PayloadError("Estrutura inválida: 'tabelas' deve ser um objeto JSON (dict).")

    result: Dict[str, TableMapping] = {}

    for table_id_raw, table_data in data.items():
        table_id = _as_str(table_id_raw) or ""
        if not table_id:
            msg = "Encontrado table_id vazio/inválido no payload."
            if strict:
                raise PayloadError(msg)
            warnings.append(msg)
            continue

        if not isinstance(table_data, dict):
            msg = f"Tabela '{table_id}': esperado objeto/dict, recebido {type(table_data).__name__}."
            if strict:
                raise PayloadError(msg)
            warnings.append(msg)
            continue

        # 3) Ignorar tabelas sem destino definido (sua regra)
        tabela_name_destino = _as_str(table_data.get("tabela_name_destino"))
        if not tabela_name_destino:
            warnings.append(f"Tabela '{table_id}': sem 'tabela_name_destino' — ignorada.")
            continue

        tabela_name_origem = _as_str(table_data.get("tabela_name_origem")) or ""
        if not tabela_name_origem:
            msg = f"Tabela '{table_id}': sem 'tabela_name_origem'."
            if strict:
                raise PayloadError(msg)
            warnings.append(msg)

        # ids das tabelas (aceita int/str)
        id_tabela_origen = table_data.get("id_tabela_origen")
        id_tabela_destino = table_data.get("id_tabela_destino")

        cols_raw = table_data.get("colunas_relacionados_para_transacao", [])
        if cols_raw is None:
            cols_raw = []
        if not isinstance(cols_raw, list):
            msg = f"Tabela '{table_id}': 'colunas_relacionados_para_transacao' deve ser lista."
            if strict:
                raise PayloadError(msg)
            warnings.append(msg)
            cols_raw = []

        colunas: List[ColumnMapping] = []

        for idx, c in enumerate(cols_raw):
            if not isinstance(c, dict):
                msg = f"Tabela '{table_id}', coluna[{idx}]: esperado dict, recebido {type(c).__name__} — ignorada."
                if strict:
                    raise PayloadError(msg)
                warnings.append(msg)
                continue

            coluna_origem = _as_str(c.get("coluna_origen_name"))
            coluna_destino = _as_str(c.get("coluna_distino_name"))
            enabled = bool(c.get("enabled", False))

            # Modo "funcionar": se enabled e falta destino, desabilita e avisa
            id_dest = _as_str(c.get("id_coluna_destino"))
            if enabled and (not coluna_destino or not id_dest or id_dest == "0"):
                warnings.append(
                    f"Tabela '{table_id}', coluna '{coluna_origem or f'[{idx}]'}': enabled=true mas sem destino válido — desabilitada."
                )
                enabled = False

            colunas.append(
                ColumnMapping(
                    coluna_origen_name=coluna_origem,
                    coluna_distino_name=coluna_destino,
                    type_coluna_origem=_as_str(c.get("type_coluna_origem")),
                    type_coluna_destino=_as_str(c.get("type_coluna_destino")),
                    id_coluna_origem=_as_str(c.get("id_coluna_origem")),
                    id_coluna_destino=id_dest,
                    enabled=enabled,
                )
            )

        # 4) Opcional: se nenhuma coluna ficou enabled, você decide: ignora ou mantém
        if not any(col.enabled for col in colunas):
            msg = f"Tabela '{table_id}': nenhuma coluna ativa após validação."
            # modo "só funcionar": mantém a tabela (talvez o usuário queira ver), mas avisa.
            warnings.append(msg)
            # Se quiser ignorar, descomenta:
            # continue

        result[table_id] = TableMapping(
            tabela_name_origem=tabela_name_origem,
            tabela_name_destino=tabela_name_destino,
            id_tabela_origen=id_tabela_origen or 0,
            id_tabela_destino=id_tabela_destino or 0,
            colunas_relacionados_para_transacao=colunas,
        )

    # 5) Se ficou tudo vazio, pode ser erro do payload
    if not result:
        msg = "Nenhuma tabela válida foi encontrada em 'tabelas'."
        if strict:
            raise PayloadError(msg)
        warnings.append(msg)

    return result, warnings
