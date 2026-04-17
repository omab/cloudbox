"""GCS — compose multiple objects into one.

    uv run python examples/gcs/compose.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import GCS_BASE, PROJECT, client, ok

BUCKET = "compose-bucket"


def upload(http, name, content):
    ok(http.post(
        f"{GCS_BASE}/upload/storage/v1/b/{BUCKET}/o",
        params={"uploadType": "media", "name": name},
        content=content.encode(),
        headers={"Content-Type": "text/plain"},
    ))


def main():
    http = client()

    ok(http.post(f"{GCS_BASE}/storage/v1/b", params={"project": PROJECT}, json={"name": BUCKET}))
    print(f"Created bucket: {BUCKET}")

    upload(http, "part-1.txt", "Hello, ")
    upload(http, "part-2.txt", "world")
    upload(http, "part-3.txt", "!")
    print("Uploaded 3 source objects")

    # Compose into a single destination object
    ok(http.post(
        f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/composed.txt/compose",
        json={
            "sourceObjects": [
                {"name": "part-1.txt"},
                {"name": "part-2.txt"},
                {"name": "part-3.txt"},
            ],
            "destination": {"contentType": "text/plain"},
        },
    ))
    print("Composed into: composed.txt")

    r = ok(http.get(f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/composed.txt", params={"alt": "media"}))
    print(f"Content: {r.text!r}")
    assert r.text == "Hello, world!"

    # Cleanup
    for name in ("part-1.txt", "part-2.txt", "part-3.txt", "composed.txt"):
        http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{name}")
    http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}")
    print("Cleaned up")


if __name__ == "__main__":
    main()
