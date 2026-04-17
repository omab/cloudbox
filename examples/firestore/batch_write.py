"""Firestore — batchWrite with independent per-write success/failure.

Unlike commit (all-or-nothing), batchWrite applies each write independently.
A failed write does not prevent the remaining writes from succeeding.

    uv run python examples/firestore/batch_write.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import FIRESTORE_BASE, PROJECT, client, ok

DB = f"projects/{PROJECT}/databases/(default)"
DOCS = f"{DB}/documents"


def main():
    http = client()

    # Seed one document
    http.post(f"{FIRESTORE_BASE}/v1/{DOCS}/items", params={"documentId": "existing"},
              json={"fields": {"value": {"integerValue": "1"}}})

    r = ok(http.post(f"{FIRESTORE_BASE}/v1/{DB}:batchWrite", json={"writes": [
        # Write 1: create a new document — will succeed
        {
            "update": {
                "name": f"{DOCS}/items/new-item",
                "fields": {"value": {"integerValue": "42"}},
            }
        },
        # Write 2: delete with exists=true precondition on a missing doc — will fail
        {
            "delete": f"{DOCS}/items/ghost",
            "currentDocument": {"exists": True},
        },
        # Write 3: update existing document — will succeed despite write 2 failing
        {
            "update": {
                "name": f"{DOCS}/items/existing",
                "fields": {"value": {"integerValue": "99"}},
            }
        },
    ]}))

    body = r.json()
    for i, (result, status) in enumerate(zip(body["writeResults"], body["status"]), 1):
        code = status["code"]
        if code == 0:
            print(f"  Write {i}: OK  — updateTime={result.get('updateTime', 'n/a')}")
        else:
            print(f"  Write {i}: ERR code={code} — {status.get('message')}")

    # Verify outcomes
    assert http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/items/new-item").status_code == 200
    assert http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/items/existing").json()["fields"]["value"]["integerValue"] == "99"
    assert http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/items/ghost").status_code == 404

    # Cleanup
    for doc_id in ("existing", "new-item"):
        http.delete(f"{FIRESTORE_BASE}/v1/{DOCS}/items/{doc_id}")
    print("Done")


if __name__ == "__main__":
    main()
