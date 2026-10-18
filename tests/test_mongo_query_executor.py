"""
Testes unitários e de integração para o executor de consultas MongoDB.
"""

from datetime import datetime
import pytest
from bson import ObjectId
from bson.dbref import DBRef

from app.schemas.query_select_upAndInsert_schema import (
    AdvancedJoinOption,
    CondicaoFiltro,
    JoinCondition,
    OrderByOption,
    QueryPayload,
)
from app.services.mongo_query_executor import (
    _bare_table,
    _coerce_value,
    _condition_to_mongo,
    _extract_row_value,
    _jsonable,
    _leaf,
    _output_columns,
    _parse_col_ref,
    build_mongo_pipeline,
    describe_mongo_query,
    run_mongo_query,
)


class MockConnection:
    id = 1
    type = "mongodb"
    database_name = "test_db"


class TestMongoQueryHelpers:
    def test_parse_col_ref(self):
        assert _parse_col_ref("mensagens.salaId") == ("mensagens", "salaId")
        assert _parse_col_ref("chat_db.mensagens.salaId") == ("mensagens", "salaId")
        assert _parse_col_ref("salaId", default_table="mensagens") == ("mensagens", "salaId")
        assert _parse_col_ref("") == ("", "")

    def test_leaf_and_bare_table(self):
        assert _leaf("chat_db.mensagens.mensagem") == "mensagem"
        assert _leaf("mensagens.mensagem") == "mensagem"
        assert _leaf("mensagem") == "mensagem"

        assert _bare_table("chat_db.mensagens") == "mensagens"
        assert _bare_table("mensagens") == "mensagens"

    def test_jsonable_dbref_and_objectid(self):
        oid = ObjectId("60d5ec49f1b2c8b1f8e4e1a1")
        assert _jsonable(oid) == "60d5ec49f1b2c8b1f8e4e1a1"

        ref = DBRef("mensagens", "msg-uuid-1234")
        assert _jsonable(ref) == "msg-uuid-1234"

        now = datetime(2026, 10, 18, 12, 0, 0)
        assert _jsonable(now) == "2026-10-18T12:00:00"

        nested = {"ref": ref, "list": [oid, now]}
        res = _jsonable(nested)
        assert res == {
            "ref": "msg-uuid-1234",
            "list": ["60d5ec49f1b2c8b1f8e4e1a1", "2026-10-18T12:00:00"],
        }

    def test_condition_to_mongo_single_and_joined(self):
        cond = CondicaoFiltro(
            table_name_fil="salas",
            column="_id",
            operator="=",
            value="febcc7ac-ca78-4c2b-99e9-e322537ebf04",
            column_type="string",
        )
        # Single table query ignores table_name_fil
        single_f = _condition_to_mongo(cond, is_joined=False)
        assert single_f == {"_id": "febcc7ac-ca78-4c2b-99e9-e322537ebf04"}

        # Joined query qualifies with table_name_fil
        joined_f = _condition_to_mongo(cond, is_joined=True)
        assert joined_f == {"salas._id": "febcc7ac-ca78-4c2b-99e9-e322537ebf04"}

    def test_condition_to_mongo_objectid_hex(self):
        hex_id = "60d5ec49f1b2c8b1f8e4e1a1"
        cond = CondicaoFiltro(
            table_name_fil="usuarios",
            column="_id",
            operator="=",
            value=hex_id,
            column_type="string",
        )
        res = _condition_to_mongo(cond, is_joined=False)
        # Deve aceitar tanto ObjectId como string literal
        assert "_id" in res
        assert "$in" in res["_id"]
        assert ObjectId(hex_id) in res["_id"]["$in"]
        assert hex_id in res["_id"]["$in"]


class TestMongoPipelineBuilder:
    def test_build_pipeline_right_join(self):
        payload = QueryPayload(
            baseTable="chat_db.mensagens",
            joins={
                "salas": AdvancedJoinOption(
                    conditions=[
                        JoinCondition(
                            table="salas",
                            leftColumn="mensagens.salaId",
                            operator="=",
                            rightColumn="salas._id",
                            valueColumnType="string",
                            useValue=False,
                        )
                    ],
                    typeJoin="RIGHT JOIN",
                )
            },
            aliaisTables={
                "mensagens.tipo": "mensagens.tipo",
                "mensagens._id": "mensagens._id",
                "salas.designacao": "salas.designacao",
                "salas.tipo": "salas.tipo",
            },
            where=[
                CondicaoFiltro(
                    table_name_fil="salas",
                    column="_id",
                    operator="=",
                    value="sala-uuid-1",
                    column_type="string",
                )
            ],
            orderBy=[OrderByOption(column="mensagens.creatBy", direction="desc")],
            limit=50,
        )

        driving_col, pipeline = build_mongo_pipeline(payload, "mensagens")
        # In RIGHT JOIN, the driving collection is 'salas'
        assert driving_col == "salas"

        # Step 1: namespace driving collection
        assert pipeline[0] == {"$project": {"salas": "$$ROOT"}}

        # Step 2: lookup mensagens
        lookup = pipeline[1]["$lookup"]
        assert lookup["from"] == "mensagens"
        assert lookup["localField"] == "salas._id"
        assert lookup["foreignField"] == "salaId"
        assert lookup["as"] == "mensagens"

        # Step 3: unwind mensagens (preserve for outer join)
        assert pipeline[2] == {"$unwind": {"path": "$mensagens", "preserveNullAndEmptyArrays": True}}

        # Step 4: match on salas._id
        assert pipeline[3] == {"$match": {"salas._id": "sala-uuid-1"}}

        # Step 5: sort on mensagens.creatBy descending
        assert pipeline[4] == {"$sort": {"mensagens.creatBy": -1}}

        # Step 6: limit 50
        assert pipeline[5] == {"$limit": 50}

    def test_build_pipeline_left_join(self):
        payload = QueryPayload(
            baseTable="chat_db.mensagens",
            joins={
                "salas": AdvancedJoinOption(
                    conditions=[
                        JoinCondition(
                            table="salas",
                            leftColumn="mensagens.salaId",
                            operator="=",
                            rightColumn="salas._id",
                            valueColumnType="string",
                            useValue=False,
                        )
                    ],
                    typeJoin="LEFT JOIN",
                )
            },
            limit=10,
        )

        driving_col, pipeline = build_mongo_pipeline(payload, "mensagens")
        # In LEFT JOIN, driving collection is baseTable ('mensagens')
        assert driving_col == "mensagens"
        assert pipeline[0] == {"$project": {"mensagens": "$$ROOT"}}
        assert pipeline[1]["$lookup"]["from"] == "salas"
        assert pipeline[1]["$lookup"]["localField"] == "mensagens.salaId"
        assert pipeline[1]["$lookup"]["foreignField"] == "_id"
        assert pipeline[2]["$unwind"]["preserveNullAndEmptyArrays"] is True

    def test_build_pipeline_count_query(self):
        payload = QueryPayload(
            baseTable="chat_db.mensagens",
            joins={
                "salas": AdvancedJoinOption(
                    conditions=[
                        JoinCondition(
                            leftColumn="mensagens.salaId",
                            operator="=",
                            rightColumn="salas._id",
                            useValue=False,
                        )
                    ],
                    typeJoin="INNER JOIN",
                )
            },
            isCountQuery=True,
        )

        _, pipeline = build_mongo_pipeline(payload, "mensagens")
        assert pipeline[-1] == {"$count": "count"}


class TestMongoRowExtraction:
    def test_extract_row_value_prevents_name_collision(self):
        # Simula documento resultante de aggregate com namespaces
        doc = {
            "mensagens": {
                "_id": "msg-1",
                "tipo": "TEXT",
                "mensagem": "olá mundo",
            },
            "salas": {
                "_id": "sala-1",
                "designacao": "Sala Geral",
                "tipo": "PESSOAL",
                "ultimaMensagem": DBRef("mensagens", "msg-1"),
            },
        }

        # mensagens.tipo deve ser "TEXT"
        assert _extract_row_value(doc, "mensagens.tipo", is_joined=True) == "TEXT"
        # salas.tipo NÃO deve ser sobrescrito com "TEXT", deve ser "PESSOAL"
        assert _extract_row_value(doc, "salas.tipo", is_joined=True) == "PESSOAL"
        assert _extract_row_value(doc, "salas.designacao", is_joined=True) == "Sala Geral"
        assert _extract_row_value(doc, "mensagens.mensagem", is_joined=True) == "olá mundo"


def test_run_mongo_query_with_mock():
    import asyncio

    # Testa a execução com agregação fingida
    class FakeCollection:
        def aggregate(self, pipeline):
            return [
                {
                    "mensagens": {
                        "_id": "msg-uuid-1",
                        "tipo": "TEXT",
                        "creatBy": "user-1",
                        "mensagem": "Teste de mensagem",
                    },
                    "salas": {
                        "_id": "sala-uuid-1",
                        "designacao": "Suporte Técnico",
                        "tipo": "SUPORTE",
                        "ultimaMensagem": DBRef("mensagens", "msg-uuid-1"),
                    },
                }
            ]

    class FakeDatabase:
        def __getitem__(self, name):
            return FakeCollection()

    class FakeClient:
        def __getitem__(self, name):
            return FakeDatabase()

    conn = MockConnection()
    payload = QueryPayload(
        baseTable="chat_db.mensagens",
        joins={
            "salas": AdvancedJoinOption(
                conditions=[
                    JoinCondition(
                        leftColumn="mensagens.salaId",
                        operator="=",
                        rightColumn="salas._id",
                        useValue=False,
                    )
                ],
                typeJoin="RIGHT JOIN",
            )
        },
        aliaisTables={
            "mensagens.tipo": "mensagens.tipo",
            "mensagens.creatBy": "mensagens.creatBy",
            "mensagens._id": "mensagens._id",
            "mensagens.mensagem": "mensagens.mensagem",
            "salas.designacao": "salas.designacao",
            "salas.tipo": "salas.tipo",
            "salas.ultimaMensagem": "salas.ultimaMensagem",
        },
    )

    rows, cols = asyncio.run(run_mongo_query(FakeClient(), conn, payload))

    assert cols == [
        "mensagens.tipo",
        "mensagens.creatBy",
        "mensagens._id",
        "mensagens.mensagem",
        "salas.designacao",
        "salas.tipo",
        "salas.ultimaMensagem",
    ]
    assert len(rows) == 1
    row = rows[0]
    assert row["mensagens.tipo"] == "TEXT"
    assert row["salas.tipo"] == "SUPORTE"
    assert row["salas.designacao"] == "Suporte Técnico"
    assert row["salas.ultimaMensagem"] == "msg-uuid-1"
    assert row["mensagens.mensagem"] == "Teste de mensagem"
