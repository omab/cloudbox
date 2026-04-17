"""GCS — upload, download, list, and delete objects.

    uv run python examples/gcs/upload_download.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import GCS_BASE, PROJECT, client, ok

BUCKET = "example-bucket"
OBJECT = "hello.txt"
CONTENT = b"Hello from Cloudbox!"


def main():
    http = client()

    # Create bucket
    ok(http.post(f"{GCS_BASE}/storage/v1/b", params={"project": PROJECT}, json={"name": BUCKET}))
    print(f"Created bucket: {BUCKET}")

    # Upload object
    ok(http.post(
        f"{GCS_BASE}/upload/storage/v1/b/{BUCKET}/o",
        params={"uploadType": "media", "name": OBJECT},
        content=CONTENT,
        headers={"Content-Type": "text/plain"},
    ))
    print(f"Uploaded: {OBJECT}")

    # List objects
    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}/o"))
    items = r.json().get("items", [])
    print(f"Objects in bucket: {[i['name'] for i in items]}")

    # Download object
    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{OBJECT}", params={"alt": "media"}))
    print(f"Downloaded content: {r.content!r}")
    assert r.content == CONTENT

    # Delete object
    http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{OBJECT}")
    print(f"Deleted: {OBJECT}")

    # Delete bucket
    http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}")
    print(f"Deleted bucket: {BUCKET}")


if __name__ == "__main__":
    main()
