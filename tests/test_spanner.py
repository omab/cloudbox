"""Tests for Cloud Spanner emulator."""

PROJECT = "local-project"
INSTANCE = "test-instance"
DATABASE = "test-db"

BASE_INST = f"/v1/projects/{PROJECT}/instances"
BASE_DB = f"{BASE_INST}/{INSTANCE}/databases"
BASE_SESS = f"{BASE_DB}/{DATABASE}/sessions"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_instance(client):
    return client.post(
        BASE_INST,
        json={"instanceId": INSTANCE, "instance": {"displayName": "Test Instance"}},
    )


def _create_database(client, extra_statements=None):
    body = {"createStatement": f"CREATE DATABASE `{DATABASE}`"}
    if extra_statements:
        body["extraStatements"] = extra_statements
    return client.post(BASE_DB, json=body)


def _setup(client):
    _create_instance(client)
    _create_database(client)


def _create_session(client):
    r = client.post(BASE_SESS, json={})
    assert r.status_code == 200, r.text
    return r.json()["name"].split("/")[-1]


def _execute_ddl(client, statements):
    r = client.patch(
        f"{BASE_DB}/{DATABASE}/ddl",
        json={"statements": statements},
    )
    assert r.status_code == 200, r.text
    return r.json()


def _create_table(client, table="Singers"):
    _execute_ddl(
        client,
        [
            f"""CREATE TABLE {table} (
              SingerId INT64 NOT NULL,
              Name STRING(MAX),
              Age INT64,
            ) PRIMARY KEY (SingerId)"""
        ],
    )


def _commit(client, session_id, mutations):
    return client.post(
        f"{BASE_SESS}/{session_id}:commit",
        json={"mutations": mutations},
    )


# ---------------------------------------------------------------------------
# Instance CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_instance(spanner_client):
    r = _create_instance(spanner_client)
    assert r.status_code == 200
    op = r.json()
    assert op["done"] is True
    assert "response" in op

    r = spanner_client.get(f"{BASE_INST}/{INSTANCE}")
    assert r.status_code == 200
    assert r.json()["name"].endswith(f"/instances/{INSTANCE}")


def test_list_instances(spanner_client):
    for i in ("inst-a", "inst-b"):
        spanner_client.post(BASE_INST, json={"instanceId": i, "instance": {"displayName": i}})
    r = spanner_client.get(BASE_INST)
    assert r.status_code == 200
    names = [inst["name"].split("/")[-1] for inst in r.json()["instances"]]
    assert "inst-a" in names and "inst-b" in names


def test_duplicate_instance_returns_409(spanner_client):
    _create_instance(spanner_client)
    r = _create_instance(spanner_client)
    assert r.status_code == 409


def test_get_missing_instance_returns_404(spanner_client):
    r = spanner_client.get(f"{BASE_INST}/nonexistent")
    assert r.status_code == 404


def test_update_instance(spanner_client):
    _create_instance(spanner_client)
    r = spanner_client.patch(
        f"{BASE_INST}/{INSTANCE}",
        json={"instance": {"displayName": "Updated Name"}},
    )
    assert r.status_code == 200
    assert r.json()["displayName"] == "Updated Name"


def test_delete_instance(spanner_client):
    _create_instance(spanner_client)
    r = spanner_client.delete(f"{BASE_INST}/{INSTANCE}")
    assert r.status_code == 200
    r = spanner_client.get(f"{BASE_INST}/{INSTANCE}")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Database CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_database(spanner_client):
    _create_instance(spanner_client)
    r = _create_database(spanner_client)
    assert r.status_code == 200
    op = r.json()
    assert op["done"] is True

    r = spanner_client.get(f"{BASE_DB}/{DATABASE}")
    assert r.status_code == 200
    assert r.json()["name"].endswith(f"/databases/{DATABASE}")
    assert r.json()["state"] == "READY"


def test_list_databases(spanner_client):
    _create_instance(spanner_client)
    for db in ("db-one", "db-two"):
        spanner_client.post(BASE_DB, json={"createStatement": f"CREATE DATABASE `{db}`"})
    r = spanner_client.get(BASE_DB)
    assert r.status_code == 200
    names = [d["name"].split("/")[-1] for d in r.json()["databases"]]
    assert "db-one" in names and "db-two" in names


def test_duplicate_database_returns_409(spanner_client):
    _create_instance(spanner_client)
    _create_database(spanner_client)
    r = _create_database(spanner_client)
    assert r.status_code == 409


def test_delete_database(spanner_client):
    _setup(spanner_client)
    r = spanner_client.delete(f"{BASE_DB}/{DATABASE}")
    assert r.status_code == 200
    r = spanner_client.get(f"{BASE_DB}/{DATABASE}")
    assert r.status_code == 404


def test_database_requires_existing_instance(spanner_client):
    r = _create_database(spanner_client)
    assert r.status_code in (400, 404)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def test_update_ddl(spanner_client):
    _setup(spanner_client)
    r = _execute_ddl(
        spanner_client,
        [
            "CREATE TABLE Songs (SongId INT64 NOT NULL, Title STRING(MAX)) PRIMARY KEY (SongId)"
        ],
    )
    assert r["done"] is True


def test_get_database_ddl(spanner_client):
    _setup(spanner_client)
    _execute_ddl(
        spanner_client,
        ["CREATE TABLE Items (ItemId INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (ItemId)"],
    )
    r = spanner_client.get(f"{BASE_DB}/{DATABASE}/ddl")
    assert r.status_code == 200
    stmts = r.json()["statements"]
    assert any("Items" in s for s in stmts)


def test_ddl_with_interleave_is_handled(spanner_client):
    _setup(spanner_client)
    # INTERLEAVE clause should not cause an error
    r = _execute_ddl(
        spanner_client,
        [
            "CREATE TABLE Parents (ParentId INT64 NOT NULL) PRIMARY KEY (ParentId)",
            "CREATE TABLE Children (ParentId INT64 NOT NULL, ChildId INT64 NOT NULL) PRIMARY KEY (ParentId, ChildId), INTERLEAVE IN PARENT Parents ON DELETE CASCADE",
        ],
    )
    assert r["done"] is True


def test_ddl_multiple_types(spanner_client):
    _setup(spanner_client)
    r = _execute_ddl(
        spanner_client,
        [
            """CREATE TABLE AllTypes (
              Id INT64 NOT NULL,
              Name STRING(1024),
              Data BYTES(MAX),
              Score FLOAT64,
              Active BOOL,
              CreatedAt TIMESTAMP,
              BirthDate DATE,
              Amount NUMERIC,
              Tags ARRAY<STRING(MAX)>,
              Meta JSON,
            ) PRIMARY KEY (Id)"""
        ],
    )
    assert r["done"] is True


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


def test_create_and_get_session(spanner_client):
    _setup(spanner_client)
    r = spanner_client.post(BASE_SESS, json={})
    assert r.status_code == 200
    session = r.json()
    assert "name" in session
    assert "createTime" in session

    session_id = session["name"].split("/")[-1]
    r = spanner_client.get(f"{BASE_SESS}/{session_id}")
    assert r.status_code == 200


def test_delete_session(spanner_client):
    _setup(spanner_client)
    session_id = _create_session(spanner_client)
    r = spanner_client.delete(f"{BASE_SESS}/{session_id}")
    assert r.status_code == 200
    r = spanner_client.get(f"{BASE_SESS}/{session_id}")
    assert r.status_code == 404


def test_batch_create_sessions(spanner_client):
    _setup(spanner_client)
    r = spanner_client.post(
        f"{BASE_SESS}:batchCreate",
        json={"sessionCount": 3},
    )
    assert r.status_code == 200
    sessions = r.json()["session"]
    assert len(sessions) == 3


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


def test_begin_and_rollback_transaction(spanner_client):
    _setup(spanner_client)
    session_id = _create_session(spanner_client)

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:beginTransaction",
        json={"options": {"readWrite": {}}},
    )
    assert r.status_code == 200
    txn = r.json()
    assert "id" in txn

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:rollback",
        json={"transactionId": txn["id"]},
    )
    assert r.status_code == 200


def test_begin_read_only_transaction(spanner_client):
    _setup(spanner_client)
    session_id = _create_session(spanner_client)
    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:beginTransaction",
        json={"options": {"readOnly": {"strong": True}}},
    )
    assert r.status_code == 200
    txn = r.json()
    assert "id" in txn
    assert "readTimestamp" in txn


# ---------------------------------------------------------------------------
# Mutations (commit)
# ---------------------------------------------------------------------------


def test_insert_mutation(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    r = _commit(
        spanner_client,
        session_id,
        [
            {
                "insert": {
                    "table": "Singers",
                    "columns": ["SingerId", "Name", "Age"],
                    "values": [["1", "Alice", "30"]],
                }
            }
        ],
    )
    assert r.status_code == 200
    assert "commitTimestamp" in r.json()


def test_insert_and_query(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [
            {
                "insert": {
                    "table": "Singers",
                    "columns": ["SingerId", "Name", "Age"],
                    "values": [["1", "Alice", "30"], ["2", "Bob", "25"]],
                }
            }
        ],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT SingerId, Name FROM Singers ORDER BY SingerId"},
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 2
    assert rows[0][1] == "Alice"
    assert rows[1][1] == "Bob"


def test_update_mutation(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Alice"]]}}],
    )
    _commit(
        spanner_client,
        session_id,
        [{"update": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Alicia"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT Name FROM Singers WHERE SingerId = 1"},
    )
    assert r.status_code == 200
    assert r.json()["rows"][0][0] == "Alicia"


def test_insert_or_update_mutation(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    # Insert first
    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Alice"]]}}],
    )
    # InsertOrUpdate (upsert) — should update Name
    _commit(
        spanner_client,
        session_id,
        [{"insertOrUpdate": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Updated"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT Name FROM Singers WHERE SingerId = 1"},
    )
    assert r.status_code == 200
    assert r.json()["rows"][0][0] == "Updated"


def test_replace_mutation(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Alice"]]}}],
    )
    _commit(
        spanner_client,
        session_id,
        [{"replace": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Replaced"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT Name FROM Singers WHERE SingerId = 1"},
    )
    assert r.status_code == 200
    assert r.json()["rows"][0][0] == "Replaced"


def test_delete_mutation(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [
            {
                "insert": {
                    "table": "Singers",
                    "columns": ["SingerId", "Name"],
                    "values": [["1", "Alice"], ["2", "Bob"]],
                }
            }
        ],
    )
    _commit(
        spanner_client,
        session_id,
        [{"delete": {"table": "Singers", "keySet": {"keys": [["1"]]}}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT SingerId FROM Singers"},
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    assert rows[0][0] == "2"


def test_delete_all_mutation(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [
            {
                "insert": {
                    "table": "Singers",
                    "columns": ["SingerId", "Name"],
                    "values": [["1", "Alice"], ["2", "Bob"]],
                }
            }
        ],
    )
    _commit(
        spanner_client,
        session_id,
        [{"delete": {"table": "Singers", "keySet": {"all": True}}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT COUNT(*) FROM Singers"},
    )
    assert r.status_code == 200
    assert r.json()["rows"][0][0] == "0"


def test_commit_with_single_use_transaction(spanner_client):
    """commit with singleUseTransaction (no explicit beginTransaction)."""
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:commit",
        json={
            "singleUseTransaction": {"readWrite": {}},
            "mutations": [
                {
                    "insert": {
                        "table": "Singers",
                        "columns": ["SingerId", "Name"],
                        "values": [["99", "Solo"]],
                    }
                }
            ],
        },
    )
    assert r.status_code == 200
    assert "commitTimestamp" in r.json()


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------


def test_execute_sql_with_params(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name", "Age"], "values": [["1", "Alice", "30"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={
            "sql": "SELECT Name FROM Singers WHERE SingerId = @id",
            "params": {"id": "1"},
            "paramTypes": {"id": {"code": "INT64"}},
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    assert rows[0][0] == "Alice"


def test_execute_sql_result_metadata(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT SingerId, Name FROM Singers"},
    )
    assert r.status_code == 200
    fields = r.json()["metadata"]["rowType"]["fields"]
    field_names = [f["name"] for f in fields]
    assert "SingerId" in field_names
    assert "Name" in field_names


def test_execute_streaming_sql(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Alice"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeStreamingSql",
        json={"sql": "SELECT SingerId, Name FROM Singers"},
    )
    assert r.status_code == 200
    import json
    # Response is newline-delimited JSON
    chunks = [json.loads(line) for line in r.text.strip().splitlines() if line.strip()]
    assert len(chunks) > 0
    assert "metadata" in chunks[0]
    # Values is a flat array
    assert "values" in chunks[0]


def test_execute_batch_dml(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "A"], ["2", "B"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeBatchDml",
        json={
            "statements": [
                {"sql": "UPDATE Singers SET Name = 'X' WHERE SingerId = 1"},
                {"sql": "UPDATE Singers SET Name = 'Y' WHERE SingerId = 2"},
            ]
        },
    )
    assert r.status_code == 200
    result_sets = r.json()["resultSets"]
    assert len(result_sets) == 2


# ---------------------------------------------------------------------------
# Read API
# ---------------------------------------------------------------------------


def test_read_with_keys(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [
            {
                "insert": {
                    "table": "Singers",
                    "columns": ["SingerId", "Name"],
                    "values": [["1", "Alice"], ["2", "Bob"], ["3", "Carol"]],
                }
            }
        ],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:read",
        json={
            "table": "Singers",
            "columns": ["SingerId", "Name"],
            "keySet": {"keys": [["1"], ["3"]]},
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 2


def test_read_all(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [
            {
                "insert": {
                    "table": "Singers",
                    "columns": ["SingerId", "Name"],
                    "values": [["1", "Alice"], ["2", "Bob"]],
                }
            }
        ],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:read",
        json={
            "table": "Singers",
            "columns": ["SingerId", "Name"],
            "keySet": {"all": True},
        },
    )
    assert r.status_code == 200
    assert len(r.json()["rows"]) == 2


def test_streaming_read(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    _commit(
        spanner_client,
        session_id,
        [{"insert": {"table": "Singers", "columns": ["SingerId", "Name"], "values": [["1", "Alice"]]}}],
    )

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:streamingRead",
        json={
            "table": "Singers",
            "columns": ["SingerId", "Name"],
            "keySet": {"all": True},
        },
    )
    assert r.status_code == 200
    import json
    chunks = [json.loads(line) for line in r.text.strip().splitlines() if line.strip()]
    assert len(chunks) > 0
    assert "values" in chunks[0]


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def test_get_operation(spanner_client):
    _create_instance(spanner_client)
    r = _create_database(spanner_client)
    op_name = r.json()["name"]
    op_id = op_name.split("/")[-1]

    r = spanner_client.get(f"{BASE_DB}/{DATABASE}/operations/{op_id}")
    assert r.status_code == 200
    assert r.json()["done"] is True


def test_get_unknown_operation_returns_done(spanner_client):
    _setup(spanner_client)
    r = spanner_client.get(f"{BASE_DB}/{DATABASE}/operations/fake-op-id")
    assert r.status_code == 200
    assert r.json()["done"] is True


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_commit_to_missing_session(spanner_client):
    _setup(spanner_client)
    r = spanner_client.post(
        f"{BASE_SESS}/nonexistent-session:commit",
        json={"mutations": []},
    )
    assert r.status_code == 404


def test_execute_sql_invalid_query(spanner_client):
    _setup(spanner_client)
    _create_table(spanner_client)
    session_id = _create_session(spanner_client)

    r = spanner_client.post(
        f"{BASE_SESS}/{session_id}:executeSql",
        json={"sql": "SELECT * FROM NonExistentTable12345"},
    )
    assert r.status_code == 400


def test_instance_configs(spanner_client):
    r = spanner_client.get(f"/v1/projects/{PROJECT}/instanceConfigs")
    assert r.status_code == 200
    assert "instanceConfigs" in r.json()
