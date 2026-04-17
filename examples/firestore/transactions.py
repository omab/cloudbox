"""Firestore — atomic commit with multiple writes and field transforms.

    uv run python examples/firestore/transactions.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import FIRESTORE_BASE, PROJECT, client, ok

DB = f"projects/{PROJECT}/databases/(default)"
DOCS = f"{DB}/documents"


def main():
    http = client()

    # Seed two documents
    http.post(f"{FIRESTORE_BASE}/v1/{DOCS}/accounts", params={"documentId": "alice"},
              json={"fields": {"balance": {"integerValue": "1000"}}})
    http.post(f"{FIRESTORE_BASE}/v1/{DOCS}/accounts", params={"documentId": "bob"},
              json={"fields": {"balance": {"integerValue": "500"}}})
    print("Seeded: alice=$1000, bob=$500")

    # Commit: transfer $200 from alice to bob in one atomic operation
    # Using field transforms (increment/decrement) so no read-modify-write race
    ok(http.post(f"{FIRESTORE_BASE}/v1/{DB}:commit", json={"writes": [
        {
            "update": {"name": f"{DOCS}/accounts/alice", "fields": {}},
            "updateMask": {"fieldPaths": []},
            "updateTransforms": [
                {"fieldPath": "balance", "increment": {"integerValue": "-200"}},
                {"fieldPath": "lastUpdated", "setToServerValue": "REQUEST_TIME"},
            ],
        },
        {
            "update": {"name": f"{DOCS}/accounts/bob", "fields": {}},
            "updateMask": {"fieldPaths": []},
            "updateTransforms": [
                {"fieldPath": "balance", "increment": {"integerValue": "200"}},
                {"fieldPath": "lastUpdated", "setToServerValue": "REQUEST_TIME"},
            ],
        },
    ]}))

    alice = http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/accounts/alice").json()["fields"]
    bob = http.get(f"{FIRESTORE_BASE}/v1/{DOCS}/accounts/bob").json()["fields"]
    print(f"After transfer: alice=${alice['balance']['integerValue']}, bob=${bob['balance']['integerValue']}")
    assert alice["balance"]["integerValue"] == "800"
    assert bob["balance"]["integerValue"] == "700"
    assert "timestampValue" in alice["lastUpdated"]

    # Cleanup
    http.delete(f"{FIRESTORE_BASE}/v1/{DOCS}/accounts/alice")
    http.delete(f"{FIRESTORE_BASE}/v1/{DOCS}/accounts/bob")
    print("Done")


if __name__ == "__main__":
    main()
