"""
Inferência de schema de coleções MongoDB.

Como o MongoDB não declara schema, os campos são derivados de uma amostra
de documentos. Estes testes fixam o que a inferência garante — e, tão
importante, o que apenas aproxima.
"""

from datetime import datetime

import pytest
from bson import ObjectId
from bson.dbref import DBRef

from app.services.mongo_schema import _bson_type_name, infer_collection_fields


class ColecaoFalsa:
    def __init__(self, documentos, indices=None):
        self._documentos = documentos
        self._indices = indices or {}

    def find(self):
        return self

    def limit(self, n):
        return iter(self._documentos[:n])

    def index_information(self):
        return self._indices


class BaseFalsa:
    name = "loja"

    def __init__(self, colecao):
        self._colecao = colecao

    def __getitem__(self, _nome):
        return self._colecao


def inferir(documentos, indices=None, **kwargs):
    base = BaseFalsa(ColecaoFalsa(documentos, indices))
    return {c["name"]: c for c in infer_collection_fields(base, "produtos", **kwargs)}


class TestTipos:
    @pytest.mark.parametrize(
        "valor,esperado",
        [
            ("texto", "string"),
            (42, "int"),
            (9_999_999_999, "long"),
            (3.14, "double"),
            (True, "bool"),
            ([1, 2], "array"),
            ({"a": 1}, "object"),
            (b"bytes", "binData"),
            (None, "null"),
            (ObjectId(), "objectId"),
            (datetime(2026, 1, 1), "date"),
        ],
    )
    def test_nomes_bson(self, valor, esperado):
        assert _bson_type_name(valor) == esperado

    def test_bool_nao_e_confundido_com_int(self):
        # Em Python bool é subclasse de int; a ordem da verificação importa.
        assert _bson_type_name(True) == "bool"
        assert _bson_type_name(1) == "int"


class TestInferencia:
    def test_campos_e_tipos_basicos(self):
        campos = inferir(
            [
                {"_id": ObjectId(), "nome": "cadeira", "preco": 49.9, "stock": 12},
                {"_id": ObjectId(), "nome": "mesa", "preco": 120.0, "stock": 3},
            ]
        )

        assert campos["nome"]["type"] == "string"
        assert campos["preco"]["type"] == "double"
        assert campos["stock"]["type"] == "int"

    def test_id_e_chave_primaria(self):
        campos = inferir([{"_id": ObjectId(), "x": 1}])

        assert campos["_id"]["is_primary_key"]
        assert campos["_id"]["is_unique"]
        assert not campos["_id"]["is_nullable"]
        # O Mongo gera o ObjectId quando o cliente não o envia.
        assert campos["_id"]["is_auto_increment"]

    def test_campo_ausente_nalguns_documentos_e_nullable(self):
        campos = inferir(
            [
                {"_id": 1, "nome": "a", "desconto": 10},
                {"_id": 2, "nome": "b"},  # sem desconto
            ]
        )

        assert campos["desconto"]["is_nullable"]
        assert not campos["nome"]["is_nullable"]

    def test_tipos_divergentes_dao_mixed(self):
        # Um campo com tipos diferentes entre documentos não pode ser
        # reduzido a um tipo só sem mentir sobre os dados.
        campos = inferir([{"_id": 1, "codigo": "A1"}, {"_id": 2, "codigo": 7}])

        assert campos["codigo"]["type"] == "mixed"
        assert "string" in campos["codigo"]["comment"]
        assert "int" in campos["codigo"]["comment"]

    def test_valor_nulo_torna_o_campo_nullable(self):
        campos = inferir([{"_id": 1, "obs": None}, {"_id": 2, "obs": None}])

        assert campos["obs"]["is_nullable"]

    def test_nulo_parcial_tambem_torna_nullable(self):
        # Campo presente em TODOS os documentos, mas nulo nalguns. Contar
        # apenas presenças diria "obrigatório" e perder-se-ia o null.
        campos = inferir([{"_id": 1, "obs": "texto"}, {"_id": 2, "obs": None}])

        assert campos["obs"]["is_nullable"]
        # O tipo continua a ser o do valor real, não "mixed".
        assert campos["obs"]["type"] == "string"

    def test_subdocumentos_e_arrays_nao_sao_achatados(self):
        # Nomes com ponto não são referenciáveis como colunas no resto do
        # sistema; descrevem-se como object/array.
        campos = inferir(
            [{"_id": 1, "morada": {"rua": "X", "cidade": "Y"}, "tags": ["a"]}]
        )

        assert campos["morada"]["type"] == "object"
        assert campos["tags"]["type"] == "array"
        assert "morada.rua" not in campos

    def test_indice_unico_simples_marca_o_campo(self):
        campos = inferir(
            [{"_id": 1, "sku": "A", "cor": "azul"}],
            indices={
                "sku_1": {"key": [("sku", 1)], "unique": True},
                "cor_1": {"key": [("cor", 1)]},
            },
        )

        assert campos["sku"]["is_unique"]
        assert not campos["cor"]["is_unique"]

    def test_indice_composto_nao_marca_campos_como_unicos(self):
        # A unicidade é da combinação; marcar cada campo daria uma garantia
        # que a base não faz.
        campos = inferir(
            [{"_id": 1, "loja": "L1", "sku": "A"}],
            indices={
                "loja_sku": {"key": [("loja", 1), ("sku", 1)], "unique": True}
            },
        )

        assert not campos["loja"]["is_unique"]
        assert not campos["sku"]["is_unique"]

    def test_coleccao_vazia_devolve_apenas_id(self):
        campos = inferir([])

        assert list(campos) == ["_id"]
        assert campos["_id"]["is_primary_key"]

    def test_comentario_expoe_a_cobertura_da_amostra(self):
        # Sem isto, um campo raro seria indistinguível de um obrigatório.
        campos = inferir([{"_id": i, "raro": 1} if i == 0 else {"_id": i} for i in range(10)])

        assert "1/10" in campos["raro"]["comment"]

    def test_amostra_limita_a_leitura(self):
        docs = [{"_id": i, "n": i} for i in range(500)]

        campos = inferir(docs, sample_size=5)

        assert "5 documentos" in campos["n"]["comment"]


class TestEncaminhamento:
    """
    Verifica que o fluxo de campos deteta MongoDB e desvia para a
    inferência, em vez de tentar `inspect()` e devolver vazio.
    """

    def _structure(self):
        class Structure:
            id = 1
            table_name = "produtos"
            schema_name = "loja"

        return Structure()

    def test_mongo_usa_inferencia_e_nao_o_inspector(self, monkeypatch):
        from app.services import field_info

        criados = []

        monkeypatch.setattr(field_info, "get_fields_by_structure", lambda *_: [])
        monkeypatch.setattr(field_info, "is_mongo", lambda _engine: True)
        monkeypatch.setattr(
            field_info, "get_mongo_database", lambda _e: BaseFalsa(
                ColecaoFalsa([{"_id": 1, "nome": "cadeira"}])
            )
        )
        monkeypatch.setattr(
            field_info,
            "create_db_field",
            lambda db, field_in, structure_id: criados.append(field_in) or field_in,
        )

        def _explode(*_a, **_k):
            raise AssertionError("caminho SQL não devia ser usado para MongoDB")

        monkeypatch.setattr(field_info, "safe_get_columns", _explode)

        resultado = field_info.buscar_ou_criar_campos_tabela(
            db=None, structure=self._structure(), engine=object(), db_type="MongoDB"
        )

        assert [c.name for c in resultado] == ["_id", "nome"]
        assert criados[0].is_primary_key

    def test_sql_continua_a_usar_o_inspector(self, monkeypatch):
        from app.services import field_info

        monkeypatch.setattr(field_info, "get_fields_by_structure", lambda *_: [])
        monkeypatch.setattr(field_info, "is_mongo", lambda _engine: False)

        chamou = {}

        def _fake_columns(*_a, **_k):
            chamou["sim"] = True
            raise RuntimeError("parar aqui")

        monkeypatch.setattr(field_info, "safe_get_columns", _fake_columns)

        with pytest.raises(RuntimeError):
            field_info.buscar_ou_criar_campos_tabela(
                db=None, structure=self._structure(), engine=object(), db_type="PostgreSQL"
            )

        assert chamou.get("sim")


class TestReferenciasDBRef:
    """
    Um DBRef declara a coleção de destino, pelo que a relação é afirmável
    — ao contrário das referências por convenção (guardar só o id noutro
    campo), que não se distinguem de dados normais.

    As formas usadas aqui são as de uma base real (Spring Data): _id como
    UUID em string nalgumas coleções e ObjectId noutras.
    """

    def test_dbref_vira_chave_estrangeira(self):
        campos = inferir(
            [
                {
                    "_id": "e13ff664-0f10-4568-9e9d-f8bc8226a580",
                    "designacao": "Sala 1",
                    "ultimaMensagem": DBRef("mensagens", "95be4fef-b31b"),
                }
            ]
        )

        assert campos["ultimaMensagem"]["type"] == "dbRef"
        assert campos["ultimaMensagem"]["is_foreign_key"]
        assert campos["ultimaMensagem"]["referenced_table"] == "mensagens"

    def test_nao_e_dbpointer(self):
        # dbPointer é um tipo BSON distinto e obsoleto (0x0C); um DBRef é
        # um subdocumento por convenção. Confundi-los aponta o utilizador
        # para a coisa errada.
        campos = inferir([{"_id": 1, "ref": DBRef("salas", "x")}])

        assert campos["ref"]["type"] != "dbPointer"

    def test_referencia_polimorfica_nao_afirma_alvo(self):
        # Aponta para coleções diferentes: não há uma tabela referenciada
        # única que se possa registar.
        campos = inferir(
            [
                {"_id": 1, "alvo": DBRef("mensagens", "a")},
                {"_id": 2, "alvo": DBRef("salas", "b")},
            ]
        )

        assert campos["alvo"]["referenced_table"] is None
        assert not campos["alvo"]["is_foreign_key"]
        # Mas o facto não se perde silenciosamente.
        assert "mensagens" in campos["alvo"]["comment"]
        assert "salas" in campos["alvo"]["comment"]

    def test_campo_normal_nao_e_marcado_como_fk(self):
        # salaId guarda o id como string, sem DBRef: é convenção da
        # aplicação e indistinguível de dados normais.
        campos = inferir([{"_id": 1, "salaId": "c341973f-4c9e"}])

        assert not campos["salaId"]["is_foreign_key"]
        assert campos["salaId"]["referenced_table"] is None

    def test_id_string_nao_e_auto_gerado(self):
        # UUID gerado pela aplicação, ao contrário do ObjectId do Mongo.
        campos = inferir([{"_id": "95be4fef-b31b-4e49-b57d-87d117458035"}])

        assert campos["_id"]["is_primary_key"]
        assert not campos["_id"]["is_auto_increment"]
        assert "aplicação" in campos["_id"]["comment"]

    def test_id_objectid_e_auto_gerado(self):
        campos = inferir([{"_id": ObjectId()}])

        assert campos["_id"]["is_auto_increment"]
        assert "MongoDB" in campos["_id"]["comment"]
