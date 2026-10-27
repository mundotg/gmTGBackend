# tests/test_data_serializer.py
import pytest
from app.ultils.data_serializer import serialize_for_sql, serialize_sql_params


def test_serialize_for_sql_json_columns():
    # List of strings for JSON column
    tags = ["Neurociência", "Aprendizagem", "Sono"]
    res = serialize_for_sql(tags, "JSON")
    assert isinstance(res, str)
    assert '["Neurociência", "Aprendizagem", "Sono"]' == res

    # List of dicts for JSON column
    mentions = [{"name": "Prof. António Damásio", "role": "Neurocientista"}]
    res = serialize_for_sql(mentions, "jsonb")
    assert isinstance(res, str)
    assert "António Damásio" in res

    # Single dict for JSON column
    doc = {"key": "value", "count": 10}
    res = serialize_for_sql(doc, "JSON")
    assert isinstance(res, str)
    assert '"key": "value"' in res


def test_serialize_for_sql_array_columns():
    # Native array columns in PostgreSQL should keep Python list
    tags = ["Física", "Química"]
    res = serialize_for_sql(tags, "text[]")
    assert isinstance(res, list)
    assert res == ["Física", "Química"]

    res_arr = serialize_for_sql(tags, "character varying[]")
    assert isinstance(res_arr, list)


def test_serialize_for_sql_scalars():
    assert serialize_for_sql(None, "JSON") is None
    assert serialize_for_sql("", "integer") is None
    assert serialize_for_sql("", "timestamp") is None
    assert serialize_for_sql("texto normal", "text") == "texto normal"


def test_serialize_sql_params():
    params = {
        "p0": "Título",
        "p1": ["Tag1", "Tag2"],
        "p2": {"info": 123},
    }
    col_types = {
        "p0": "VARCHAR(255)",
        "p1": "JSON",
        "p2": "JSONB",
    }
    sanitized = serialize_sql_params(params, col_types)
    assert sanitized["p0"] == "Título"
    assert sanitized["p1"] == '["Tag1", "Tag2"]'
    assert sanitized["p2"] == '{"info": 123}'
