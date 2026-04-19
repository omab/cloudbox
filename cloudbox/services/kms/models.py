"""Pydantic models for Cloud KMS REST API v1."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class KeyRingModel(BaseModel):
    name: str
    createTime: str = Field(default_factory=_now)


class CryptoKeyPurpose:
    ENCRYPT_DECRYPT = "ENCRYPT_DECRYPT"
    ASYMMETRIC_SIGN = "ASYMMETRIC_SIGN"
    ASYMMETRIC_DECRYPT = "ASYMMETRIC_DECRYPT"
    MAC = "MAC"


class CryptoKeyVersionAlgorithm:
    GOOGLE_SYMMETRIC_ENCRYPTION = "GOOGLE_SYMMETRIC_ENCRYPTION"
    RSA_SIGN_PSS_2048_SHA256 = "RSA_SIGN_PSS_2048_SHA256"
    RSA_SIGN_PSS_3072_SHA256 = "RSA_SIGN_PSS_3072_SHA256"
    RSA_SIGN_PSS_4096_SHA256 = "RSA_SIGN_PSS_4096_SHA256"
    RSA_DECRYPT_OAEP_2048_SHA256 = "RSA_DECRYPT_OAEP_2048_SHA256"
    EC_SIGN_P256_SHA256 = "EC_SIGN_P256_SHA256"
    EC_SIGN_P384_SHA384 = "EC_SIGN_P384_SHA384"


class CryptoKeyVersionState:
    PENDING_GENERATION = "PENDING_GENERATION"
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DESTROY_SCHEDULED = "DESTROY_SCHEDULED"
    DESTROYED = "DESTROYED"


class CryptoKeyVersionModel(BaseModel):
    name: str
    state: str = CryptoKeyVersionState.ENABLED
    createTime: str = Field(default_factory=_now)
    generateTime: str = Field(default_factory=_now)
    destroyTime: str | None = None
    destroyEventTime: str | None = None
    algorithm: str = CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    protectionLevel: str = "SOFTWARE"


class CryptoKeyVersionTemplate(BaseModel):
    algorithm: str = CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    protectionLevel: str = "SOFTWARE"


class CryptoKeyModel(BaseModel):
    name: str
    purpose: str = CryptoKeyPurpose.ENCRYPT_DECRYPT
    createTime: str = Field(default_factory=_now)
    nextRotationTime: str | None = None
    rotationPeriod: str | None = None
    primary: CryptoKeyVersionModel | None = None
    versionTemplate: CryptoKeyVersionTemplate = Field(default_factory=CryptoKeyVersionTemplate)
    labels: dict[str, str] = Field(default_factory=dict)


class ListKeyRingsResponse(BaseModel):
    keyRings: list[KeyRingModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class ListCryptoKeysResponse(BaseModel):
    cryptoKeys: list[CryptoKeyModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class ListCryptoKeyVersionsResponse(BaseModel):
    cryptoKeyVersions: list[CryptoKeyVersionModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class EncryptRequest(BaseModel):
    plaintext: str  # base64-encoded
    additionalAuthenticatedData: str | None = None


class EncryptResponse(BaseModel):
    name: str
    ciphertext: str  # base64-encoded
    ciphertextCrc32c: str | None = None


class DecryptRequest(BaseModel):
    ciphertext: str  # base64-encoded
    additionalAuthenticatedData: str | None = None


class DecryptResponse(BaseModel):
    plaintext: str  # base64-encoded
    plaintextCrc32c: str | None = None
    usedPrimary: bool = True
