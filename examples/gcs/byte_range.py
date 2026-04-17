"""GCS — download a specific byte range (Range header / partial content).

    uv run python examples/gcs/byte_range.py
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import GCS_BASE, PROJECT, client, ok

BUCKET = "range-bucket"
OBJECT = "alphabet.txt"
CONTENT = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def main():
    http = client()

    ok(http.post(f"{GCS_BASE}/storage/v1/b", params={"project": PROJECT}, json={"name": BUCKET}))
    ok(http.post(
        f"{GCS_BASE}/upload/storage/v1/b/{BUCKET}/o",
        params={"uploadType": "media", "name": OBJECT},
        content=CONTENT,
        headers={"Content-Type": "text/plain"},
    ))
    print(f"Uploaded {len(CONTENT)} bytes: {CONTENT!r}")

    # Fetch bytes 0-4 (first 5 bytes)
    r = http.get(
        f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{OBJECT}",
        params={"alt": "media"},
        headers={"Range": "bytes=0-4"},
    )
    assert r.status_code == 206
    print(f"bytes=0-4  → {r.content!r}")   # b"ABCDE"

    # Fetch bytes 10-14
    r = http.get(
        f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{OBJECT}",
        params={"alt": "media"},
        headers={"Range": "bytes=10-14"},
    )
    assert r.status_code == 206
    print(f"bytes=10-14 → {r.content!r}")  # b"KLMNO"

    # Suffix range: last 5 bytes
    r = http.get(
        f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{OBJECT}",
        params={"alt": "media"},
        headers={"Range": "bytes=-5"},
    )
    assert r.status_code == 206
    print(f"bytes=-5   → {r.content!r}")   # b"VWXYZ"

    # Cleanup
    http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}/o/{OBJECT}")
    http.delete(f"{GCS_BASE}/storage/v1/b/{BUCKET}")
    print("Cleaned up")


if __name__ == "__main__":
    main()
