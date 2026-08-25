"""
Motor de VARIÁVEIS para templates de relatório.

Permite que um template criado no construtor (page `createtamplete`) se ajuste
aos dados reais do relatório usando placeholders `{{caminho.para.valor}}` em
qualquer campo de texto (títulos, textos, células, itens de lista, rodapé…) e
que uma TABELA seja preenchida automaticamente a partir de uma lista de dados.

Como usar (no gerador):
    from app.relatorio.template_variaveis import preparar_estrutura
    estrutura = preparar_estrutura(estrutura_do_template, contexto)

Contexto = dicionário com os dados disponíveis, ex.:
    {
      "data": {"hoje": "2026-08-09", "hora": "14:03"},
      "empresa": {"nome": "ACME"},
      "usuario": {"nome": "Ana"},
      "query": {"total": 128, "colunas": [...], "linhas": [ {..}, {..} ]},
    }

Binding de tabela (no `table.data`):
    "rows_from":   caminho para uma LISTA (de dicts ou de listas) → vira as linhas
    "columns_from": caminho para uma lista de nomes de coluna (opcional)
    Quando as linhas são dicts, cada `columns[i]` é usada como CHAVE do dict.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# {{ caminho.para.valor }}  |  {{ query.linhas[0].nome }}
_TOKEN = re.compile(r"\{\{\s*([\w\.\[\]]+)\s*\}\}")
_INDEX = re.compile(r"^(\w+)\[(\d+)\]$")


def resolve_path(context: Any, path: str) -> Any:
    """Resolve um caminho tipo `a.b[0].c` no contexto. None se não existir."""
    cur = context
    for raw in path.split("."):
        if cur is None:
            return None
        m = _INDEX.match(raw)
        key, idx = (m.group(1), int(m.group(2))) if m else (raw, None)

        if key:
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                cur = getattr(cur, key, None)
        if idx is not None:
            if isinstance(cur, (list, tuple)) and 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return None
    return cur


def _fmt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Sim" if value else "Não"
    if isinstance(value, float):
        # evita "3.0" desnecessário
        return str(int(value)) if value.is_integer() else f"{value:g}"
    return str(value)


def substituir_string(s: str, context: Dict[str, Any]) -> str:
    def repl(m: "re.Match[str]") -> str:
        return _fmt(resolve_path(context, m.group(1)))

    return _TOKEN.sub(repl, s)


def substituir_variaveis(node: Any, context: Dict[str, Any]) -> Any:
    """Substitui placeholders recursivamente em strings/listas/dicts."""
    if isinstance(node, str):
        return substituir_string(node, context)
    if isinstance(node, list):
        return [substituir_variaveis(x, context) for x in node]
    if isinstance(node, dict):
        return {k: substituir_variaveis(v, context) for k, v in node.items()}
    return node


def _expandir_tabela(table: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Preenche `columns`/`rows` a partir de `columns_from`/`rows_from`."""
    t = dict(table)
    cols_from = t.pop("columns_from", None)
    rows_from = t.pop("rows_from", None) or t.pop("bind", None)

    if cols_from:
        cols = resolve_path(context, cols_from)
        if isinstance(cols, list):
            t["columns"] = [_fmt(c) for c in cols]

    if rows_from:
        data_list = resolve_path(context, rows_from)
        if isinstance(data_list, list):
            columns = t.get("columns") or []
            new_rows: List[List[str]] = []
            for item in data_list:
                if isinstance(item, dict):
                    # usa as colunas como chaves; se não houver colunas, usa as chaves do 1º item
                    keys = columns or list(item.keys())
                    if not columns:
                        t["columns"] = [_fmt(k) for k in keys]
                        columns = t["columns"]
                    new_rows.append([_fmt(item.get(k, "")) for k in keys])
                elif isinstance(item, (list, tuple)):
                    new_rows.append([_fmt(x) for x in item])
                else:
                    new_rows.append([_fmt(item)])
            t["rows"] = new_rows
    return t


def preparar_estrutura(
    structure: List[Dict[str, Any]], context: Optional[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Aplica variáveis + binding de tabelas a uma estrutura de template.
    Se `context` for vazio, devolve a estrutura tal como está.
    """
    if not context:
        return structure

    out: List[Dict[str, Any]] = []
    for section in structure:
        if isinstance(section, dict) and "table" in section and isinstance(section["table"], dict):
            section = {**section, "table": _expandir_tabela(section["table"], context)}
        out.append(substituir_variaveis(section, context))
    return out
