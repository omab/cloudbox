"""BigQuery — parameterized queries (named and positional modes).

    uv run python examples/bigquery/parameterized_query.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import BIGQUERY_BASE, PROJECT, client, ok

DATASET = "param_dataset"
TABLE = "events"


def query(http, sql, params, mode="NAMED"):
    r = ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/queries",
        json={
            "query": sql,
            "useLegacySql": False,
            "parameterMode": mode,
            "queryParameters": params,
        },
    ))
    return r.json().get("rows", [])


def main():
    http = client()

    # Setup
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": DATASET}},
    ))
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": TABLE},
            "schema": {"fields": [
                {"name": "user",    "type": "STRING"},
                {"name": "action",  "type": "STRING"},
                {"name": "score",   "type": "INTEGER"},
            ]},
        },
    ))
    ok(http.post(
        f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/{TABLE}/insertAll",
        json={"rows": [
            {"insertId": "e1", "json": {"user": "alice", "action": "login",    "score": 10}},
            {"insertId": "e2", "json": {"user": "alice", "action": "purchase", "score": 50}},
            {"insertId": "e3", "json": {"user": "bob",   "action": "login",    "score": 10}},
            {"insertId": "e4", "json": {"user": "bob",   "action": "logout",   "score":  5}},
        ]},
    ))

    # Named parameters: @user and @min_score
    rows = query(http,
        f"SELECT action, score FROM `{PROJECT}.{DATASET}.{TABLE}` WHERE user = @user AND score >= @min_score ORDER BY score DESC",
        [
            {"name": "user",      "parameterType": {"type": "STRING"},  "parameterValue": {"value": "alice"}},
            {"name": "min_score", "parameterType": {"type": "INTEGER"}, "parameterValue": {"value": "10"}},
        ],
        mode="NAMED",
    )
    print("Alice's events (score >= 10) [named params]:")
    for row in rows:
        vals = [f["v"] for f in row["f"]]
        print(f"  action={vals[0]:10s}  score={vals[1]}")

    # Positional parameters: ?
    rows = query(http,
        f"SELECT user, action FROM `{PROJECT}.{DATASET}.{TABLE}` WHERE action = ? ORDER BY user",
        [
            {"parameterType": {"type": "STRING"}, "parameterValue": {"value": "login"}},
        ],
        mode="POSITIONAL",
    )
    print("\nAll login events [positional params]:")
    for row in rows:
        vals = [f["v"] for f in row["f"]]
        print(f"  user={vals[0]:8s}  action={vals[1]}")

    # Cleanup
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}/tables/{TABLE}")
    http.delete(f"{BIGQUERY_BASE}/bigquery/v2/projects/{PROJECT}/datasets/{DATASET}?deleteContents=true")
    print("\nDone")


if __name__ == "__main__":
    main()
