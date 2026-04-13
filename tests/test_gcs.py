"""Tests for Cloud Storage emulator."""


def test_create_and_get_bucket(gcs_client):
    r = gcs_client.post("/storage/v1/b", json={"name": "test-bucket"})
    assert r.status_code == 200
    assert r.json()["name"] == "test-bucket"

    r = gcs_client.get("/storage/v1/b/test-bucket")
    assert r.status_code == 200
    assert r.json()["name"] == "test-bucket"


def test_list_buckets(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bucket-a"})
    gcs_client.post("/storage/v1/b", json={"name": "bucket-b"})
    r = gcs_client.get("/storage/v1/b")
    assert r.status_code == 200
    names = [b["name"] for b in r.json()["items"]]
    assert "bucket-a" in names
    assert "bucket-b" in names


def test_duplicate_bucket_returns_409(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "dup"})
    r = gcs_client.post("/storage/v1/b", json={"name": "dup"})
    assert r.status_code == 409


def test_delete_bucket(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "to-delete"})
    r = gcs_client.delete("/storage/v1/b/to-delete")
    assert r.status_code == 204
    r = gcs_client.get("/storage/v1/b/to-delete")
    assert r.status_code == 404


def test_delete_non_empty_bucket_returns_409(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "nonempty"})
    gcs_client.post(
        "/upload/storage/v1/b/nonempty/o?name=file.txt&uploadType=media",
        content=b"hello",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.delete("/storage/v1/b/nonempty")
    assert r.status_code == 409


def test_upload_and_download_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt"})
    r = gcs_client.post(
        "/upload/storage/v1/b/bkt/o?name=hello.txt&uploadType=media",
        content=b"Hello, world!",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "hello.txt"

    r = gcs_client.get("/download/storage/v1/b/bkt/o/hello.txt")
    assert r.status_code == 200
    assert r.content == b"Hello, world!"


def test_list_objects(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt2"})
    for name in ("a.txt", "b.txt", "c.txt"):
        gcs_client.post(
            f"/upload/storage/v1/b/bkt2/o?name={name}&uploadType=media",
            content=b"x",
            headers={"content-type": "text/plain"},
        )
    r = gcs_client.get("/storage/v1/b/bkt2/o")
    assert r.status_code == 200
    names = [o["name"] for o in r.json()["items"]]
    assert set(names) == {"a.txt", "b.txt", "c.txt"}


def test_list_objects_with_prefix(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "pfx"})
    for name in ("dir/a.txt", "dir/b.txt", "other.txt"):
        gcs_client.post(
            f"/upload/storage/v1/b/pfx/o?name={name}&uploadType=media",
            content=b"x",
            headers={"content-type": "text/plain"},
        )
    r = gcs_client.get("/storage/v1/b/pfx/o?prefix=dir/")
    assert r.status_code == 200
    names = [o["name"] for o in r.json()["items"]]
    assert set(names) == {"dir/a.txt", "dir/b.txt"}


def test_delete_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt3"})
    gcs_client.post(
        "/upload/storage/v1/b/bkt3/o?name=del.txt&uploadType=media",
        content=b"bye",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.delete("/storage/v1/b/bkt3/o/del.txt")
    assert r.status_code == 204
    r = gcs_client.get("/storage/v1/b/bkt3/o/del.txt")
    assert r.status_code == 404


def test_get_object_metadata(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "meta-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/meta-bkt/o?name=file.txt&uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.get("/storage/v1/b/meta-bkt/o/file.txt")
    assert r.status_code == 200
    meta = r.json()
    assert meta["name"] == "file.txt"
    assert meta["size"] == "4"
    assert meta["contentType"] == "text/plain"


def test_copy_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "src-bkt"})
    gcs_client.post("/storage/v1/b", json={"name": "dst-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/src-bkt/o?name=orig.txt&uploadType=media",
        content=b"copy me",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.post(
        "/storage/v1/b/src-bkt/o/orig.txt/copyTo/b/dst-bkt/o/copy.txt"
    )
    assert r.status_code == 200
    r = gcs_client.get("/download/storage/v1/b/dst-bkt/o/copy.txt")
    assert r.content == b"copy me"
