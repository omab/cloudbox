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


def test_multipart_upload(gcs_client):
    """uploadType=multipart carries name + content-type in the metadata part."""
    import json
    gcs_client.post("/storage/v1/b", json={"name": "mp-bucket"})
    boundary = "foo_boundary"
    metadata = json.dumps({"name": "multi.json", "contentType": "application/json"})
    body_bytes = b'{"key": "value"}'
    payload = (
        f"--{boundary}\r\n"
        "Content-Type: application/json\r\n\r\n"
        f"{metadata}\r\n"
        f"--{boundary}\r\n"
        "Content-Type: application/json\r\n\r\n"
    ).encode() + body_bytes + f"\r\n--{boundary}--".encode()

    r = gcs_client.post(
        "/upload/storage/v1/b/mp-bucket/o?uploadType=multipart",
        content=payload,
        headers={"content-type": f"multipart/related; boundary={boundary}"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "multi.json"

    r = gcs_client.get("/download/storage/v1/b/mp-bucket/o/multi.json")
    assert r.content == body_bytes


def test_upload_missing_name_returns_400(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "noname-bkt"})
    r = gcs_client.post(
        "/upload/storage/v1/b/noname-bkt/o?uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 400


def test_upload_missing_bucket_returns_404(gcs_client):
    r = gcs_client.post(
        "/upload/storage/v1/b/ghost-bucket/o?name=f.txt&uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 404


def test_get_missing_bucket_returns_404(gcs_client):
    r = gcs_client.get("/storage/v1/b/no-such-bucket")
    assert r.status_code == 404


def test_get_missing_object_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "empty-bkt"})
    r = gcs_client.get("/storage/v1/b/empty-bkt/o/phantom.txt")
    assert r.status_code == 404


def test_download_alt_media(gcs_client):
    """GET with ?alt=media on the metadata endpoint streams the object body."""
    gcs_client.post("/storage/v1/b", json={"name": "alt-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/alt-bkt/o?name=payload.bin&uploadType=media",
        content=b"binary content",
        headers={"content-type": "application/octet-stream"},
    )
    r = gcs_client.get("/storage/v1/b/alt-bkt/o/payload.bin?alt=media")
    assert r.status_code == 200
    assert r.content == b"binary content"


def test_metadata_patch(gcs_client):
    """PATCH updates mutable fields without touching the body."""
    gcs_client.post("/storage/v1/b", json={"name": "patch-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/patch-bkt/o?name=obj.txt&uploadType=media",
        content=b"original",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.patch(
        "/storage/v1/b/patch-bkt/o/obj.txt",
        json={"contentType": "text/markdown", "metadata": {"author": "alice"}},
    )
    assert r.status_code == 200
    assert r.json()["contentType"] == "text/markdown"
    assert r.json()["metadata"]["author"] == "alice"

    # Body is still intact
    r = gcs_client.get("/download/storage/v1/b/patch-bkt/o/obj.txt")
    assert r.content == b"original"


def test_overwrite_increments_generation(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "gen-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/gen-bkt/o?name=f.bin&uploadType=media",
        content=b"v1",
        headers={"content-type": "application/octet-stream"},
    )
    r1 = gcs_client.get("/storage/v1/b/gen-bkt/o/f.bin")
    gen1 = int(r1.json()["generation"])

    gcs_client.post(
        "/upload/storage/v1/b/gen-bkt/o?name=f.bin&uploadType=media",
        content=b"v2",
        headers={"content-type": "application/octet-stream"},
    )
    r2 = gcs_client.get("/storage/v1/b/gen-bkt/o/f.bin")
    gen2 = int(r2.json()["generation"])

    assert gen2 > gen1


def test_checksums_present_in_metadata(gcs_client):
    """md5Hash and crc32c are computed and returned on upload."""
    gcs_client.post("/storage/v1/b", json={"name": "chk-bkt"})
    r = gcs_client.post(
        "/upload/storage/v1/b/chk-bkt/o?name=chk.txt&uploadType=media",
        content=b"checksum me",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200
    meta = r.json()
    assert meta["md5Hash"]
    assert meta["crc32c"]
    assert meta["etag"] == meta["md5Hash"]


def test_list_objects_delimiter_virtual_dirs(gcs_client):
    """delimiter collapses common prefixes into the prefixes[] result."""
    gcs_client.post("/storage/v1/b", json={"name": "delim-bkt"})
    for name in ("a/1.txt", "a/2.txt", "b/3.txt", "top.txt"):
        gcs_client.post(
            f"/upload/storage/v1/b/delim-bkt/o?name={name}&uploadType=media",
            content=b"x",
            headers={"content-type": "text/plain"},
        )
    r = gcs_client.get("/storage/v1/b/delim-bkt/o?delimiter=/")
    assert r.status_code == 200
    body = r.json()
    assert set(body["prefixes"]) == {"a/", "b/"}
    assert [o["name"] for o in body["items"]] == ["top.txt"]


def test_delete_missing_object_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "del404-bkt"})
    r = gcs_client.delete("/storage/v1/b/del404-bkt/o/ghost.txt")
    assert r.status_code == 404


def test_copy_to_missing_bucket_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "copy-src"})
    gcs_client.post(
        "/upload/storage/v1/b/copy-src/o?name=f.txt&uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.post("/storage/v1/b/copy-src/o/f.txt/copyTo/b/no-dst/o/f.txt")
    assert r.status_code == 404
