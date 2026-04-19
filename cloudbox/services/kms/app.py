"""Cloud KMS emulator.

Implements the Cloud KMS REST API v1 used by google-cloud-kms.

Supports:
  - KeyRing CRUD
  - CryptoKey CRUD (ENCRYPT_DECRYPT purpose)
  - CryptoKeyVersion lifecycle (create, enable, disable, destroy, restore)
  - Symmetric encrypt/decrypt (AES-256-GCM)

Asymmetric operations (ASYMMETRIC_SIGN, ASYMMETRIC_DECRYPT) return 501.
"""

from __future__ import annotations

import base64
import os
import struct

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.kms.models import (
    CryptoKeyModel,
    CryptoKeyPurpose,
    CryptoKeyVersionAlgorithm,
    CryptoKeyVersionModel,
    CryptoKeyVersionState,
    CryptoKeyVersionTemplate,
    DecryptRequest,
    DecryptResponse,
    EncryptRequest,
    EncryptResponse,
    KeyRingModel,
    ListCryptoKeyVersionsResponse,
    ListCryptoKeysResponse,
    ListKeyRingsResponse,
    _now,
)
from cloudbox.services.kms.store import get_store

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _AESGCM_AVAILABLE = True
except ImportError:
    _AESGCM_AVAILABLE = False

app = FastAPI(title="Cloudbox — Cloud KMS", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "kms")

_NONCE_SIZE = 12  # AES-GCM nonce bytes


def _store():
    return get_store()


# ---------------------------------------------------------------------------
# Crypto helpers
# ---------------------------------------------------------------------------


def _new_aes_key() -> bytes:
    return os.urandom(32)


def _encrypt_payload(version_name: str, plaintext: bytes, aad: bytes | None) -> bytes:
    """Encrypt plaintext with the key for version_name using AES-256-GCM.

    Returns raw bytes: 2-byte version-name length + version name + nonce + GCM output.
    """
    if not _AESGCM_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot encrypt")
    store = _store()
    raw_key_b64 = store.get("keys", version_name)
    if raw_key_b64 is None:
        raise GCPError(404, f"Key material for {version_name} not found")
    key = base64.b64decode(raw_key_b64)
    aesgcm = AESGCM(key)
    nonce = os.urandom(_NONCE_SIZE)
    ct = aesgcm.encrypt(nonce, plaintext, aad)
    name_bytes = version_name.encode()
    return struct.pack(">H", len(name_bytes)) + name_bytes + nonce + ct


def _decrypt_payload(blob: bytes, aad: bytes | None) -> tuple[str, bytes]:
    """Decrypt a blob produced by _encrypt_payload.

    Returns (version_name, plaintext).
    """
    if not _AESGCM_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot decrypt")
    if len(blob) < 2:
        raise GCPError(400, "Invalid ciphertext")
    name_len = struct.unpack(">H", blob[:2])[0]
    offset = 2 + name_len
    if len(blob) < offset + _NONCE_SIZE:
        raise GCPError(400, "Invalid ciphertext")
    version_name = blob[2:offset].decode()
    nonce = blob[offset:offset + _NONCE_SIZE]
    ct = blob[offset + _NONCE_SIZE:]
    store = _store()
    raw_key_b64 = store.get("keys", version_name)
    if raw_key_b64 is None:
        raise GCPError(404, f"Key material for {version_name} not found")
    key = base64.b64decode(raw_key_b64)
    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ct, aad)
    except Exception:
        raise GCPError(400, "Decryption failed: invalid ciphertext or key")
    return version_name, plaintext


def _provision_version(version_name: str, purpose: str) -> None:
    """Generate and store key material for a new CryptoKeyVersion."""
    if purpose == CryptoKeyPurpose.ENCRYPT_DECRYPT:
        key = _new_aes_key()
        _store().set("keys", version_name, base64.b64encode(key).decode())


def _next_version_number(key_name: str) -> int:
    store = _store()
    prefix = f"{key_name}/cryptoKeyVersions/"
    nums = []
    for k in store.keys("versions"):
        if k.startswith(prefix):
            try:
                nums.append(int(k[len(prefix):]))
            except ValueError:
                pass
    return max(nums, default=0) + 1


def _get_algorithm(purpose: str) -> str:
    if purpose == CryptoKeyPurpose.ENCRYPT_DECRYPT:
        return CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    if purpose == CryptoKeyPurpose.ASYMMETRIC_SIGN:
        return CryptoKeyVersionAlgorithm.EC_SIGN_P256_SHA256
    if purpose == CryptoKeyPurpose.ASYMMETRIC_DECRYPT:
        return CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256
    return CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION


# ---------------------------------------------------------------------------
# KeyRings
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/keyRings")
async def create_key_ring(project: str, location: str, request: Request):
    key_ring_id = request.query_params.get("keyRingId")
    if not key_ring_id:
        body = await request.json()
        key_ring_id = body.get("keyRingId", "")
    if not key_ring_id:
        raise GCPError(400, "keyRingId is required")
    name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
    store = _store()
    if store.exists("keyrings", name):
        raise GCPError(409, f"KeyRing {name} already exists")
    ring = KeyRingModel(name=name)
    store.set("keyrings", name, ring.model_dump())
    return JSONResponse(status_code=200, content=ring.model_dump())


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}")
async def get_key_ring(project: str, location: str, key_ring_id: str):
    name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
    data = _store().get("keyrings", name)
    if data is None:
        raise GCPError(404, f"KeyRing {name} not found")
    return data


@app.get("/v1/projects/{project}/locations/{location}/keyRings")
async def list_key_rings(
    project: str,
    location: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
):
    prefix = f"projects/{project}/locations/{location}/keyRings/"
    store = _store()
    all_rings = [KeyRingModel(**v) for v in store.list("keyrings") if v["name"].startswith(prefix)]
    all_rings.sort(key=lambda r: r.name)
    offset = int(pageToken) if pageToken else 0
    page = all_rings[offset:offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_rings) else None
    return ListKeyRingsResponse(
        keyRings=page, nextPageToken=next_token, totalSize=len(all_rings)
    ).model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# CryptoKeys
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys")
async def create_crypto_key(project: str, location: str, key_ring_id: str, request: Request):
    crypto_key_id = request.query_params.get("cryptoKeyId")
    body = await request.json()
    if not crypto_key_id:
        crypto_key_id = body.get("cryptoKeyId", "")
    if not crypto_key_id:
        raise GCPError(400, "cryptoKeyId is required")

    ring_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
    store = _store()
    if not store.exists("keyrings", ring_name):
        raise GCPError(404, f"KeyRing {ring_name} not found")

    key_name = f"{ring_name}/cryptoKeys/{crypto_key_id}"
    if store.exists("cryptokeys", key_name):
        raise GCPError(409, f"CryptoKey {key_name} already exists")

    purpose = body.get("purpose", CryptoKeyPurpose.ENCRYPT_DECRYPT)
    algorithm = _get_algorithm(purpose)
    template = CryptoKeyVersionTemplate(algorithm=algorithm)

    ck = CryptoKeyModel(
        name=key_name,
        purpose=purpose,
        versionTemplate=template,
        labels=body.get("labels", {}),
        nextRotationTime=body.get("nextRotationTime"),
        rotationPeriod=body.get("rotationPeriod"),
    )

    # Create initial version 1 as primary
    v1_name = f"{key_name}/cryptoKeyVersions/1"
    v1 = CryptoKeyVersionModel(name=v1_name, algorithm=algorithm)
    store.set("versions", v1_name, v1.model_dump())
    _provision_version(v1_name, purpose)

    ck.primary = v1
    store.set("cryptokeys", key_name, ck.model_dump())
    return JSONResponse(status_code=200, content=ck.model_dump(exclude_none=True))


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}")
async def get_crypto_key(project: str, location: str, key_ring_id: str, crypto_key_id: str):
    name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    data = _store().get("cryptokeys", name)
    if data is None:
        raise GCPError(404, f"CryptoKey {name} not found")
    return data


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys")
async def list_crypto_keys(
    project: str,
    location: str,
    key_ring_id: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
):
    prefix = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/"
    store = _store()
    all_keys = [
        CryptoKeyModel(**v) for v in store.list("cryptokeys")
        if v["name"].startswith(prefix) and "/cryptoKeyVersions/" not in v["name"]
    ]
    all_keys.sort(key=lambda k: k.name)
    offset = int(pageToken) if pageToken else 0
    page = all_keys[offset:offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_keys) else None
    return ListCryptoKeysResponse(
        cryptoKeys=page, nextPageToken=next_token, totalSize=len(all_keys)
    ).model_dump(exclude_none=True)


@app.patch("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}")
async def update_crypto_key(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, request: Request
):
    name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    store = _store()
    data = store.get("cryptokeys", name)
    if data is None:
        raise GCPError(404, f"CryptoKey {name} not found")
    body = await request.json()
    for field in ("labels", "nextRotationTime", "rotationPeriod"):
        if field in body:
            data[field] = body[field]
    store.set("cryptokeys", name, data)
    return data


# ---------------------------------------------------------------------------
# Encrypt / Decrypt
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}:encrypt")
async def encrypt(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, body: EncryptRequest
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    store = _store()
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        raise GCPError(404, f"CryptoKey {key_name} not found")
    if ck_data.get("purpose") != CryptoKeyPurpose.ENCRYPT_DECRYPT:
        raise GCPError(400, f"CryptoKey {key_name} does not support ENCRYPT_DECRYPT")
    primary = ck_data.get("primary")
    if not primary or primary.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKey {key_name} has no enabled primary version")

    version_name = primary["name"]
    plaintext = base64.b64decode(body.plaintext)
    aad = base64.b64decode(body.additionalAuthenticatedData) if body.additionalAuthenticatedData else None
    blob = _encrypt_payload(version_name, plaintext, aad)
    ciphertext_b64 = base64.b64encode(blob).decode()
    return EncryptResponse(name=version_name, ciphertext=ciphertext_b64).model_dump(exclude_none=True)


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}:decrypt")
async def decrypt(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, body: DecryptRequest
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    store = _store()
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        raise GCPError(404, f"CryptoKey {key_name} not found")
    if ck_data.get("purpose") != CryptoKeyPurpose.ENCRYPT_DECRYPT:
        raise GCPError(400, f"CryptoKey {key_name} does not support ENCRYPT_DECRYPT")

    blob = base64.b64decode(body.ciphertext)
    aad = base64.b64decode(body.additionalAuthenticatedData) if body.additionalAuthenticatedData else None
    _version_name, plaintext = _decrypt_payload(blob, aad)

    # Verify the version belongs to this key
    if not _version_name.startswith(key_name + "/cryptoKeyVersions/"):
        raise GCPError(400, "Ciphertext was not encrypted by this key")

    version_data = store.get("versions", _version_name)
    if version_data and version_data.get("state") not in (
        CryptoKeyVersionState.ENABLED, CryptoKeyVersionState.DISABLED
    ):
        raise GCPError(400, f"CryptoKeyVersion {_version_name} is destroyed and cannot decrypt")

    return DecryptResponse(
        plaintext=base64.b64encode(plaintext).decode(), usedPrimary=True
    ).model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# CryptoKeyVersions
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions")
async def create_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    store = _store()
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        raise GCPError(404, f"CryptoKey {key_name} not found")

    n = _next_version_number(key_name)
    version_name = f"{key_name}/cryptoKeyVersions/{n}"
    purpose = ck_data.get("purpose", CryptoKeyPurpose.ENCRYPT_DECRYPT)
    algorithm = _get_algorithm(purpose)
    v = CryptoKeyVersionModel(name=version_name, algorithm=algorithm)
    store.set("versions", version_name, v.model_dump())
    _provision_version(version_name, purpose)
    _sync_primary(store, key_name)
    return JSONResponse(status_code=200, content=v.model_dump(exclude_none=True))


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}")
async def get_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    data = _store().get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    return data


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions")
async def list_crypto_key_versions(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
    filter: str = Query(default=""),
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    prefix = f"{key_name}/cryptoKeyVersions/"
    store = _store()
    all_versions = [
        CryptoKeyVersionModel(**v) for k, v in
        [(k, store.get("versions", k)) for k in store.keys("versions")]
        if k.startswith(prefix) and v
    ]
    all_versions.sort(key=lambda v: v.name)
    if filter:
        state_filter = filter.upper().replace("STATE=", "").strip()
        all_versions = [v for v in all_versions if v.state == state_filter]
    offset = int(pageToken) if pageToken else 0
    page = all_versions[offset:offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_versions) else None
    return ListCryptoKeyVersionsResponse(
        cryptoKeyVersions=page, nextPageToken=next_token, totalSize=len(all_versions)
    ).model_dump(exclude_none=True)


@app.patch("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}")
async def update_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str,
    request: Request,
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    data = store.get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    body = await request.json()
    if "state" in body:
        data["state"] = body["state"]
    store.set("versions", version_name, data)
    # Sync primary on the CryptoKey if needed
    _sync_primary(store, key_name)
    return data


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:destroy")
async def destroy_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    return _set_version_state(project, location, key_ring_id, crypto_key_id, version_id,
                               CryptoKeyVersionState.DESTROY_SCHEDULED, wipe_key=False)


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:restore")
async def restore_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    data = store.get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if data.get("state") not in (CryptoKeyVersionState.DESTROY_SCHEDULED,):
        raise GCPError(400, f"CryptoKeyVersion {version_name} cannot be restored from state {data.get('state')}")
    data["state"] = CryptoKeyVersionState.DISABLED
    data["destroyTime"] = None
    store.set("versions", version_name, data)
    return data


# ---------------------------------------------------------------------------
# Asymmetric stubs (501)
# ---------------------------------------------------------------------------


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}/publicKey")
async def get_public_key(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    raise GCPError(501, "Asymmetric operations are not supported by this emulator")


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:asymmetricSign")
async def asymmetric_sign(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    raise GCPError(501, "Asymmetric operations are not supported by this emulator")


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:asymmetricDecrypt")
async def asymmetric_decrypt(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    raise GCPError(501, "Asymmetric operations are not supported by this emulator")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _set_version_state(
    project: str, location: str, key_ring_id: str, crypto_key_id: str,
    version_id: str, state: str, wipe_key: bool = False,
) -> dict:
    key_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    data = store.get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    data["state"] = state
    if state in (CryptoKeyVersionState.DESTROY_SCHEDULED,):
        data["destroyTime"] = _now()
    if wipe_key:
        store.delete("keys", version_name)
    store.set("versions", version_name, data)
    _sync_primary(store, key_name)
    return data


def _sync_primary(store, key_name: str) -> None:
    """Update the primary pointer on a CryptoKey to the highest-numbered enabled version."""
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        return
    prefix = f"{key_name}/cryptoKeyVersions/"
    enabled = []
    for k in store.keys("versions"):
        if k.startswith(prefix):
            v = store.get("versions", k)
            if v and v.get("state") == CryptoKeyVersionState.ENABLED:
                try:
                    enabled.append((int(k[len(prefix):]), v))
                except ValueError:
                    pass
    if enabled:
        ck_data["primary"] = max(enabled, key=lambda x: x[0])[1]
    else:
        ck_data["primary"] = None
    store.set("cryptokeys", key_name, ck_data)
