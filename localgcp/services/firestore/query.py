"""Firestore structured query evaluation."""
from __future__ import annotations

from typing import Any


def _extract_value(v: dict) -> Any:
    """Extract a Python value from a Firestore typed value dict."""
    if "nullValue" in v:
        return None
    if "booleanValue" in v:
        return v["booleanValue"]
    if "integerValue" in v:
        return int(v["integerValue"])
    if "doubleValue" in v:
        return float(v["doubleValue"])
    if "stringValue" in v:
        return v["stringValue"]
    if "timestampValue" in v:
        return v["timestampValue"]
    if "arrayValue" in v:
        return [_extract_value(item) for item in v["arrayValue"].get("values", [])]
    if "mapValue" in v:
        return {k: _extract_value(fv) for k, fv in v["mapValue"].get("fields", {}).items()}
    return None


def _get_field(doc_fields: dict, field_path: str) -> Any:
    """Navigate a dotted field path into a Firestore fields dict."""
    parts = field_path.split(".")
    current = doc_fields
    for part in parts:
        if not isinstance(current, dict):
            return None
        fv = current.get(part)
        if fv is None:
            return None
        if isinstance(fv, dict) and any(k in fv for k in (
            "stringValue", "integerValue", "booleanValue", "nullValue",
            "doubleValue", "mapValue", "arrayValue", "timestampValue",
        )):
            current = _extract_value(fv)
        else:
            current = fv
    return current


def _eval_filter(doc: dict, filter_node: dict) -> bool:
    """Evaluate a Firestore filter node against a document."""
    if "compositeFilter" in filter_node:
        cf = filter_node["compositeFilter"]
        op = cf.get("op", "AND")
        filters = cf.get("filters", [])
        if op == "AND":
            return all(_eval_filter(doc, f) for f in filters)
        if op == "OR":
            return any(_eval_filter(doc, f) for f in filters)
        return True

    if "fieldFilter" in filter_node:
        ff = filter_node["fieldFilter"]
        field_path = ff["field"]["fieldPath"]
        op = ff["op"]
        filter_value = _extract_value(ff["value"])
        doc_value = _get_field(doc.get("fields", {}), field_path)

        try:
            if op == "EQUAL":
                return doc_value == filter_value
            if op == "NOT_EQUAL":
                return doc_value != filter_value
            if op == "LESS_THAN":
                return doc_value < filter_value
            if op == "LESS_THAN_OR_EQUAL":
                return doc_value <= filter_value
            if op == "GREATER_THAN":
                return doc_value > filter_value
            if op == "GREATER_THAN_OR_EQUAL":
                return doc_value >= filter_value
            if op == "ARRAY_CONTAINS":
                return isinstance(doc_value, list) and filter_value in doc_value
            if op == "IN":
                return doc_value in (filter_value if isinstance(filter_value, list) else [])
            if op == "NOT_IN":
                return doc_value not in (filter_value if isinstance(filter_value, list) else [])
            if op == "ARRAY_CONTAINS_ANY":
                fv_list = filter_value if isinstance(filter_value, list) else []
                return isinstance(doc_value, list) and any(x in doc_value for x in fv_list)
        except TypeError:
            return False

    if "unaryFilter" in filter_node:
        uf = filter_node["unaryFilter"]
        field_path = uf["field"]["fieldPath"]
        op = uf["op"]
        doc_value = _get_field(doc.get("fields", {}), field_path)
        if op == "IS_NULL":
            return doc_value is None
        if op == "IS_NOT_NULL":
            return doc_value is not None
        if op == "IS_NAN":
            import math
            return isinstance(doc_value, float) and math.isnan(doc_value)
        if op == "IS_NOT_NAN":
            import math
            return not (isinstance(doc_value, float) and math.isnan(doc_value))

    return True


def run_query(docs: list[dict], query: dict) -> list[dict]:
    """Apply a structuredQuery dict to a list of document dicts."""
    results = list(docs)

    # WHERE
    where = query.get("where")
    if where:
        results = [d for d in results if _eval_filter(d, where)]

    # ORDER BY
    order_by = query.get("orderBy", [])
    if order_by:
        def _sort_key(doc):
            keys = []
            for order in order_by:
                field_path = order["field"]["fieldPath"]
                keys.append(_get_field(doc.get("fields", {}), field_path))
            return keys

        reverse_flags = [o.get("direction", "ASCENDING") == "DESCENDING" for o in order_by]
        # Multi-key sort: Python's sort is stable so we sort by each key in reverse priority
        for i in reversed(range(len(order_by))):
            field_path = order_by[i]["field"]["fieldPath"]
            desc = order_by[i].get("direction", "ASCENDING") == "DESCENDING"
            try:
                results.sort(
                    key=lambda d: (_get_field(d.get("fields", {}), field_path) is None,
                                   _get_field(d.get("fields", {}), field_path)),
                    reverse=desc,
                )
            except TypeError:
                pass

    # OFFSET
    offset = query.get("offset", 0)
    if offset:
        results = results[offset:]

    # LIMIT
    limit = query.get("limit")
    if limit is not None:
        results = results[:limit]

    return results
