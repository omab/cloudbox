"""Tests for Cloud Firestore emulator."""

DB = "projects/local-project/databases/(default)"
DOCS = f"{DB}/documents"


def test_create_and_get_document(firestore_client):
    r = firestore_client.post(
        f"/v1/{DOCS}/users",
        params={"documentId": "alice"},
        json={"fields": {"name": {"stringValue": "Alice"}, "age": {"integerValue": "30"}}},
    )
    assert r.status_code == 200
    assert r.json()["name"].endswith("/users/alice")

    r = firestore_client.get(f"/v1/{DOCS}/users/alice")
    assert r.status_code == 200
    assert r.json()["fields"]["name"]["stringValue"] == "Alice"


def test_update_document(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/items",
        params={"documentId": "item1"},
        json={"fields": {"count": {"integerValue": "1"}}},
    )
    firestore_client.patch(
        f"/v1/{DOCS}/items/item1",
        json={"fields": {"count": {"integerValue": "2"}, "label": {"stringValue": "hi"}}},
    )
    r = firestore_client.get(f"/v1/{DOCS}/items/item1")
    assert r.json()["fields"]["count"]["integerValue"] == "2"
    assert r.json()["fields"]["label"]["stringValue"] == "hi"


def test_delete_document(firestore_client):
    firestore_client.post(
        f"/v1/{DOCS}/things",
        params={"documentId": "t1"},
        json={"fields": {}},
    )
    r = firestore_client.delete(f"/v1/{DOCS}/things/t1")
    assert r.status_code == 204
    r = firestore_client.get(f"/v1/{DOCS}/things/t1")
    assert r.status_code == 404


def test_list_documents(firestore_client):
    for i in range(3):
        firestore_client.post(
            f"/v1/{DOCS}/col",
            params={"documentId": f"doc{i}"},
            json={"fields": {"n": {"integerValue": str(i)}}},
        )
    r = firestore_client.get(f"/v1/{DOCS}/col")
    assert r.status_code == 200
    docs = r.json()["documents"]
    assert len(docs) == 3


def test_run_query_filter(firestore_client):
    for i in range(5):
        firestore_client.post(
            f"/v1/{DOCS}/scores",
            params={"documentId": f"s{i}"},
            json={"fields": {"value": {"integerValue": str(i * 10)}}},
        )
    r = firestore_client.post(
        f"/v1/{DOCS}:runQuery",
        json={
            "structuredQuery": {
                "from": [{"collectionId": "scores"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "value"},
                        "op": "GREATER_THAN_OR_EQUAL",
                        "value": {"integerValue": "20"},
                    }
                },
            }
        },
    )
    assert r.status_code == 200
    results = r.json()
    assert len(results) == 3  # 20, 30, 40


def test_batch_get(firestore_client):
    for did in ("d1", "d2"):
        firestore_client.post(
            f"/v1/{DOCS}/batch",
            params={"documentId": did},
            json={"fields": {"x": {"stringValue": did}}},
        )
    r = firestore_client.post(
        f"/v1/{DB}/documents:batchGet",
        json={"documents": [f"{DOCS}/batch/d1", f"{DOCS}/batch/d2", f"{DOCS}/batch/missing"]},
    )
    assert r.status_code == 200
    results = r.json()
    found = [item for item in results if "found" in item]
    missing = [item for item in results if "missing" in item]
    assert len(found) == 2
    assert len(missing) == 1


def test_transaction_commit(firestore_client):
    r = firestore_client.post(f"/v1/{DB}:beginTransaction", json={})
    assert r.status_code == 200
    txn = r.json()["transaction"]

    r = firestore_client.post(
        f"/v1/{DB}:commit",
        json={
            "transaction": txn,
            "writes": [
                {
                    "update": {
                        "name": f"{DOCS}/txn/doc1",
                        "fields": {"val": {"stringValue": "from-txn"}},
                    }
                }
            ],
        },
    )
    assert r.status_code == 200
    r = firestore_client.get(f"/v1/{DOCS}/txn/doc1")
    assert r.json()["fields"]["val"]["stringValue"] == "from-txn"
