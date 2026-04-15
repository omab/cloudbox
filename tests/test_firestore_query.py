"""Unit tests for Firestore query.py — targets uncovered operators and features."""
import math

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _doc(fields: dict) -> dict:
    """Wrap a plain dict as a Firestore-style document."""
    def _wrap(v):
        if v is None:
            return {"nullValue": None}
        if isinstance(v, bool):
            return {"booleanValue": v}
        if isinstance(v, int):
            return {"integerValue": str(v)}
        if isinstance(v, float):
            return {"doubleValue": v}
        if isinstance(v, str):
            return {"stringValue": v}
        if isinstance(v, list):
            return {"arrayValue": {"values": [_wrap(i) for i in v]}}
        return {"stringValue": str(v)}

    return {"fields": {k: _wrap(v) for k, v in fields.items()}}


def _field_filter(path, op, value):
    def _fv(v):
        if v is None:
            return {"nullValue": None}
        if isinstance(v, bool):
            return {"booleanValue": v}
        if isinstance(v, int):
            return {"integerValue": str(v)}
        if isinstance(v, float):
            return {"doubleValue": v}
        if isinstance(v, str):
            return {"stringValue": v}
        if isinstance(v, list):
            return {"arrayValue": {"values": [_fv(i) for i in v]}}
        return {"stringValue": str(v)}

    return {"fieldFilter": {"field": {"fieldPath": path}, "op": op, "value": _fv(value)}}


def _unary_filter(path, op):
    return {"unaryFilter": {"field": {"fieldPath": path}, "op": op}}


def _query(where=None, order_by=None, offset=None, limit=None):
    q = {}
    if where:
        q["where"] = where
    if order_by:
        q["orderBy"] = order_by
    if offset is not None:
        q["offset"] = offset
    if limit is not None:
        q["limit"] = limit
    return q


# ---------------------------------------------------------------------------
# _extract_value
# ---------------------------------------------------------------------------

def test_extract_value_all_types():
    from localgcp.services.firestore.query import _extract_value

    assert _extract_value({"nullValue": None}) is None
    assert _extract_value({"booleanValue": True}) is True
    assert _extract_value({"integerValue": "42"}) == 42
    assert _extract_value({"doubleValue": 3.14}) == pytest.approx(3.14)
    assert _extract_value({"stringValue": "hello"}) == "hello"
    assert _extract_value({"timestampValue": "2024-01-01T00:00:00Z"}) == "2024-01-01T00:00:00Z"
    assert _extract_value({"arrayValue": {"values": [{"integerValue": "1"}, {"integerValue": "2"}]}}) == [1, 2]
    assert _extract_value({"mapValue": {"fields": {"k": {"stringValue": "v"}}}}) == {"k": "v"}
    assert _extract_value({}) is None


# ---------------------------------------------------------------------------
# fieldFilter — comparison operators
# ---------------------------------------------------------------------------

def test_not_equal_matches():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"x": 1}), _doc({"x": 2}), _doc({"x": 3})]
    result = run_query(docs, _query(where=_field_filter("x", "NOT_EQUAL", 2)))
    assert len(result) == 2


def test_not_equal_no_match():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"x": 5}), _doc({"x": 5})]
    result = run_query(docs, _query(where=_field_filter("x", "NOT_EQUAL", 5)))
    assert result == []


def test_less_than():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"n": 1}), _doc({"n": 5}), _doc({"n": 10})]
    result = run_query(docs, _query(where=_field_filter("n", "LESS_THAN", 5)))
    assert len(result) == 1
    assert result[0]["fields"]["n"]["integerValue"] == "1"


def test_less_than_or_equal():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"n": 1}), _doc({"n": 5}), _doc({"n": 10})]
    result = run_query(docs, _query(where=_field_filter("n", "LESS_THAN_OR_EQUAL", 5)))
    assert len(result) == 2


def test_greater_than():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"n": 1}), _doc({"n": 5}), _doc({"n": 10})]
    result = run_query(docs, _query(where=_field_filter("n", "GREATER_THAN", 5)))
    assert len(result) == 1


def test_greater_than_or_equal():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"n": 1}), _doc({"n": 5}), _doc({"n": 10})]
    result = run_query(docs, _query(where=_field_filter("n", "GREATER_THAN_OR_EQUAL", 5)))
    assert len(result) == 2


def test_not_in():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"color": "red"}), _doc({"color": "blue"}), _doc({"color": "green"})]
    result = run_query(docs, _query(where=_field_filter("color", "NOT_IN", ["red", "blue"])))
    assert len(result) == 1
    assert result[0]["fields"]["color"]["stringValue"] == "green"


def test_not_in_non_list_value():
    from localgcp.services.firestore.query import run_query

    # If filter value is not a list, everything matches NOT_IN
    docs = [_doc({"x": 1}), _doc({"x": 2})]
    result = run_query(docs, _query(where=_field_filter("x", "NOT_IN", "not-a-list")))
    assert len(result) == 2


def test_array_contains_any():
    from localgcp.services.firestore.query import run_query

    docs = [
        _doc({"tags": ["a", "b"]}),
        _doc({"tags": ["c", "d"]}),
        _doc({"tags": ["e", "f"]}),
    ]
    result = run_query(docs, _query(where=_field_filter("tags", "ARRAY_CONTAINS_ANY", ["b", "c"])))
    assert len(result) == 2


def test_array_contains_any_no_match():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"tags": ["x", "y"]})]
    result = run_query(docs, _query(where=_field_filter("tags", "ARRAY_CONTAINS_ANY", ["a", "b"])))
    assert result == []


def test_type_error_returns_false():
    """Comparing incompatible types should return False (not crash)."""
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"x": "a string"})]
    result = run_query(docs, _query(where=_field_filter("x", "LESS_THAN", 5)))
    assert result == []


# ---------------------------------------------------------------------------
# unaryFilter
# ---------------------------------------------------------------------------

def test_is_null_matches():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"v": None}), _doc({"v": 1})]
    result = run_query(docs, _query(where=_unary_filter("v", "IS_NULL")))
    assert len(result) == 1


def test_is_not_null_matches():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"v": None}), _doc({"v": 1}), _doc({"v": "hi"})]
    result = run_query(docs, _query(where=_unary_filter("v", "IS_NOT_NULL")))
    assert len(result) == 2


def test_is_nan_matches():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"f": float("nan")}), _doc({"f": 1.0})]
    result = run_query(docs, _query(where=_unary_filter("f", "IS_NAN")))
    assert len(result) == 1


def test_is_nan_non_float_returns_false():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"f": "not-a-float"})]
    result = run_query(docs, _query(where=_unary_filter("f", "IS_NAN")))
    assert result == []


def test_is_not_nan_matches():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"f": float("nan")}), _doc({"f": 1.0}), _doc({"f": "x"})]
    result = run_query(docs, _query(where=_unary_filter("f", "IS_NOT_NAN")))
    assert len(result) == 2


# ---------------------------------------------------------------------------
# orderBy — multiple keys
# ---------------------------------------------------------------------------

def test_order_by_descending():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"n": 1}), _doc({"n": 3}), _doc({"n": 2})]
    result = run_query(docs, _query(order_by=[{"field": {"fieldPath": "n"}, "direction": "DESCENDING"}]))
    vals = [r["fields"]["n"]["integerValue"] for r in result]
    assert vals == ["3", "2", "1"]


def test_order_by_multi_key():
    from localgcp.services.firestore.query import run_query

    docs = [
        _doc({"g": 2, "n": 1}),
        _doc({"g": 1, "n": 3}),
        _doc({"g": 1, "n": 1}),
        _doc({"g": 2, "n": 2}),
    ]
    result = run_query(docs, _query(order_by=[
        {"field": {"fieldPath": "g"}, "direction": "ASCENDING"},
        {"field": {"fieldPath": "n"}, "direction": "ASCENDING"},
    ]))
    groups = [r["fields"]["g"]["integerValue"] for r in result]
    ns = [r["fields"]["n"]["integerValue"] for r in result]
    assert groups == ["1", "1", "2", "2"]
    assert ns == ["1", "3", "1", "2"]


# ---------------------------------------------------------------------------
# OFFSET
# ---------------------------------------------------------------------------

def test_offset_skips_leading_docs():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"i": i}) for i in range(5)]
    result = run_query(docs, _query(offset=2))
    assert len(result) == 3
    assert result[0]["fields"]["i"]["integerValue"] == "2"


def test_offset_with_limit():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"i": i}) for i in range(10)]
    result = run_query(docs, _query(offset=3, limit=4))
    assert len(result) == 4
    assert result[0]["fields"]["i"]["integerValue"] == "3"


def test_offset_zero_is_noop():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"i": i}) for i in range(3)]
    result = run_query(docs, _query(offset=0))
    assert len(result) == 3


# ---------------------------------------------------------------------------
# _get_field — nested path
# ---------------------------------------------------------------------------

def test_get_field_nested_path():
    from localgcp.services.firestore.query import _get_field

    fields = {
        "address": {
            "mapValue": {
                "fields": {
                    "city": {"stringValue": "Springfield"}
                }
            }
        }
    }
    result = _get_field(fields, "address.city")
    assert result == "Springfield"


def test_get_field_missing_returns_none():
    from localgcp.services.firestore.query import _get_field

    assert _get_field({}, "missing.path") is None


# ---------------------------------------------------------------------------
# compositeFilter AND / OR
# ---------------------------------------------------------------------------

def test_composite_and_filter():
    from localgcp.services.firestore.query import run_query

    docs = [
        _doc({"a": 1, "b": 1}),
        _doc({"a": 1, "b": 2}),
        _doc({"a": 2, "b": 1}),
    ]
    result = run_query(docs, _query(where={
        "compositeFilter": {
            "op": "AND",
            "filters": [
                _field_filter("a", "EQUAL", 1),
                _field_filter("b", "EQUAL", 1),
            ],
        }
    }))
    assert len(result) == 1


def test_composite_unknown_op_returns_true():
    from localgcp.services.firestore.query import run_query

    docs = [_doc({"x": 1}), _doc({"x": 2})]
    result = run_query(docs, _query(where={
        "compositeFilter": {
            "op": "UNKNOWN_OP",
            "filters": [_field_filter("x", "EQUAL", 999)],
        }
    }))
    # Unknown op returns True (all docs pass)
    assert len(result) == 2
