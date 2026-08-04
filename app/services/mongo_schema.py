"""
Inferência de schema para coleções MongoDB.

O MongoDB não declara schema: cada documento pode ter campos diferentes.
Como o resto da aplicação assume tabelas com colunas, é preciso derivar
uma descrição equivalente lendo uma amostra de documentos.

A amostra torna o resultado uma aproximação, e isso é intencional — ler
a coleção inteira para listar campos seria inviável. As consequências
estão explicitadas no resultado:

- `is_nullable` fica True se o campo faltar em algum documento da amostra
- `type` fica "mixed" quando o mesmo campo aparece com tipos diferentes
- campos que só existem fora da amostra não são detetados

A profundidade é deliberadamente 1: subdocumentos são descritos como
"object" e arrays como "array", em vez de serem achatados em caminhos com
pontos. Nomes com ponto não são representáveis como colunas no resto do
sistema, e achatá-los produziria "colunas" que nenhuma query conseguiria
referenciar.
"""

from __future__ import annotations

from typing import Any

from app.ultils.logger import log_message

# Documentos lidos por coleção. Suficiente para estabilizar os campos
# comuns sem pesar num cluster de produção.
DEFAULT_SAMPLE_SIZE = 200

# Campo que o MongoDB cria e indexa automaticamente em toda a coleção.
MONGO_ID_FIELD = "_id"


def _bson_type_name(value: Any) -> str:
    """
    Nome do tipo BSON de um valor, na terminologia do MongoDB.

    Usa os nomes que aparecem no `$type` do próprio Mongo, para que o que
    é mostrado ao utilizador coincida com o que ele escreveria numa query.
    """
    if value is None:
        return "null"

    # bool antes de int: em Python, bool é subclasse de int.
    if isinstance(value, bool):
        return "bool"

    if isinstance(value, int):
        return "long" if abs(value) > 2_147_483_647 else "int"

    if isinstance(value, float):
        return "double"

    if isinstance(value, str):
        return "string"

    if isinstance(value, (list, tuple)):
        return "array"

    if isinstance(value, dict):
        return "object"

    if isinstance(value, (bytes, bytearray)):
        return "binData"

    # Tipos do pymongo/bson (ObjectId, Decimal128, datetime, Binary…).
    nome = type(value).__name__

    return {
        "ObjectId": "objectId",
        "datetime": "date",
        "Decimal128": "decimal",
        "Binary": "binData",
        "Regex": "regex",
        "Timestamp": "timestamp",
        "Code": "javascript",
        "DBRef": "dbPointer",
    }.get(nome, nome)


def _unique_index_fields(collection: Any) -> set[str]:
    """
    Campos cobertos por um índice único de campo simples.

    Índices compostos são ignorados de propósito: a unicidade aí é da
    combinação, não de cada campo, e marcá-los como únicos daria uma
    garantia que a base não faz.
    """
    try:
        indices = collection.index_information()
    except Exception as e:
        log_message(f"⚠️ Sem acesso aos índices: {e}", "warning")
        return set()

    unicos: set[str] = set()

    for info in indices.values():
        if not info.get("unique"):
            continue

        chaves = info.get("key") or []

        if len(chaves) == 1:
            unicos.add(chaves[0][0])

    return unicos


def infer_collection_fields(
    database: Any,
    collection_name: str,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> list[dict[str, Any]]:
    """
    Deriva a lista de campos de uma coleção a partir de uma amostra.

    Devolve dicionários com a forma que `buscar_ou_criar_campos_tabela`
    espera para construir os DBField.
    """
    collection = database[collection_name]

    try:
        # find().limit() em vez de $sample: não exige permissão de
        # aggregate e é barato porque lê documentos consecutivos.
        documentos = list(collection.find().limit(sample_size))
    except Exception as e:
        log_message(
            f"⚠️ Não foi possível ler documentos de '{collection_name}': {e}",
            "warning",
        )
        return []

    if not documentos:
        # Coleção vazia: só se pode afirmar o _id, que o Mongo garante.
        log_message(
            f"ℹ️ Coleção '{collection_name}' vazia; apenas {MONGO_ID_FIELD} inferido.",
            "info",
        )
        return [_campo_id()]

    total = len(documentos)
    ocorrencias: dict[str, int] = {}
    tipos: dict[str, set[str]] = {}
    tem_nulo: dict[str, bool] = {}
    ordem: list[str] = []

    for documento in documentos:
        for nome, valor in documento.items():
            if nome not in ocorrencias:
                ocorrencias[nome] = 0
                tipos[nome] = set()
                tem_nulo[nome] = False
                ordem.append(nome)

            ocorrencias[nome] += 1

            tipo = _bson_type_name(valor)

            # "null" não é um tipo em si — indica que o campo aceita nulos.
            # Regista-se à parte: um campo presente em todos os documentos
            # mas nulo nalguns continua a ser nullable.
            if tipo == "null":
                tem_nulo[nome] = True
            else:
                tipos[nome].add(tipo)

    unicos = _unique_index_fields(collection)

    campos: list[dict[str, Any]] = []

    for nome in ordem:
        tipos_vistos = tipos[nome]

        if not tipos_vistos:
            tipo = "null"
        elif len(tipos_vistos) == 1:
            tipo = next(iter(tipos_vistos))
        else:
            # Sinaliza heterogeneidade em vez de escolher um tipo à sorte.
            tipo = "mixed"

        ausente_nalgum = ocorrencias[nome] < total

        if nome == MONGO_ID_FIELD:
            campos.append(_campo_id(tipo=tipo))
            continue

        campos.append(
            {
                "name": nome,
                "type": tipo,
                "is_nullable": ausente_nalgum or tem_nulo[nome],
                "is_primary_key": False,
                "is_unique": nome in unicos,
                "is_auto_increment": False,
                "comment": _descrever_cobertura(ocorrencias[nome], total, tipos_vistos),
            }
        )

    return campos


def _campo_id(tipo: str = "objectId") -> dict[str, Any]:
    """Descritor do _id, que o MongoDB garante único e sempre presente."""
    return {
        "name": MONGO_ID_FIELD,
        "type": tipo,
        "is_nullable": False,
        "is_primary_key": True,
        "is_unique": True,
        # O Mongo gera o ObjectId quando o cliente não o fornece.
        "is_auto_increment": tipo == "objectId",
        "comment": "Chave primária gerada pelo MongoDB",
    }


def _descrever_cobertura(
    presencas: int, total: int, tipos_vistos: set[str]
) -> str:
    """
    Comentário que torna explícito o que a amostra suporta.

    Sem isto, um campo presente em 3 de 200 documentos seria indistinguível
    de um campo obrigatório, e o utilizador não teria como o saber.
    """
    partes = [f"presente em {presencas}/{total} documentos amostrados"]

    if len(tipos_vistos) > 1:
        partes.append(f"tipos observados: {', '.join(sorted(tipos_vistos))}")

    return "; ".join(partes)
