"""Tests for Secret Manager emulator."""
import base64


PROJECT = "local-project"


def test_create_and_get_secret(sm_client):
    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets",
        params={"secretId": "my-secret"},
        json={},
    )
    assert r.status_code == 200
    assert r.json()["name"] == f"projects/{PROJECT}/secrets/my-secret"

    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/my-secret")
    assert r.status_code == 200


def test_duplicate_secret_returns_409(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dup"}, json={})
    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "dup"}, json={})
    assert r.status_code == 409


def test_add_and_access_version(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "api-key"}, json={})
    payload = base64.b64encode(b"super-secret-value").decode()

    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/api-key:addVersion",
        json={"payload": {"data": payload}},
    )
    assert r.status_code == 200
    version_name = r.json()["name"]
    assert "/versions/1" in version_name

    r = sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/api-key/versions/latest:access"
    )
    assert r.status_code == 200
    assert r.json()["payload"]["data"] == payload


def test_multiple_versions_latest_resolves(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "versioned"}, json={})
    for i in range(3):
        sm_client.post(
            f"/v1/projects/{PROJECT}/secrets/versioned:addVersion",
            json={"payload": {"data": base64.b64encode(f"v{i}".encode()).decode()}},
        )

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/versioned/versions/latest:access")
    assert r.status_code == 200
    # latest should be version 3
    val = base64.b64decode(r.json()["payload"]["data"]).decode()
    assert val == "v2"


def test_list_secrets(sm_client):
    for name in ("s1", "s2", "s3"):
        sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": name}, json={})
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets")
    assert r.status_code == 200
    names = [s["name"].split("/")[-1] for s in r.json()["secrets"]]
    assert {"s1", "s2", "s3"}.issubset(set(names))


def test_delete_secret(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "to-del"}, json={})
    r = sm_client.delete(f"/v1/projects/{PROJECT}/secrets/to-del")
    assert r.status_code == 200
    r = sm_client.get(f"/v1/projects/{PROJECT}/secrets/to-del")
    assert r.status_code == 404


def test_destroy_version_clears_payload(sm_client):
    sm_client.post(f"/v1/projects/{PROJECT}/secrets", params={"secretId": "destroyable"}, json={})
    payload = base64.b64encode(b"sensitive").decode()
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets/destroyable:addVersion",
        json={"payload": {"data": payload}},
    )
    sm_client.post(f"/v1/projects/{PROJECT}/secrets/destroyable/versions/1:destroy")

    r = sm_client.post(f"/v1/projects/{PROJECT}/secrets/destroyable/versions/1:access")
    assert r.status_code == 403  # not enabled
