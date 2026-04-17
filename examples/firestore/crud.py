"""Firestore — create, get, update (with field mask), and delete documents.

    uv run python examples/firestore/crud.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import FIRESTORE_BASE, PROJECT, client, ok

DB = f"projects/{PROJECT}/databases/(default)"
DOCS = f"{DB}/documents"


def str_val(v):
    return {"stringValue": v}

def int_val(v):
    return {"integerValue": str(v)}

def bool_val(v):
    return {"booleanValue": v}


def main():
    http = client()

    # Create document with a specific ID
    r = ok(http.post(
        f"{FIRESTORE_BASE}/v1/{DOCS}/users",
        params={"documentId": "alice"},
        json={"fields": {
            "name": str_val("Alice"),
            "age": int_val(30),
            "active": bool_val(True),
        }},
    ))
    print(f"Created: {r.json()['name']}")

    # Get document
    r = ok(http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/users/alice"))
    fields = r.json()["fields"]
    print(f"Got: name={fields['name']['stringValue']}, age={fields['age']['integerValue']}")

    # Update with field mask — only 'age' is changed, 'name' and 'active' are preserved
    ok(http.patch(
        f"{FIRESTORE_BASE}/v1/{DOCS}/users/alice",
        params={"updateMask.fieldPaths": "age"},
        json={"fields": {"age": int_val(31)}},
    ))
    r = ok(http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/users/alice"))
    fields = r.json()["fields"]
    print(f"After update: age={fields['age']['integerValue']}, name still={fields['name']['stringValue']!r}")

    # Delete document
    http.delete(f"{FIRESTORE_BASE}/v1/{DOCS}/users/alice")
    r = http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/users/alice")
    print(f"After delete: status={r.status_code}")  # 404


if __name__ == "__main__":
    main()
