"""Pydantic models matching the Firestore REST API wire format."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FirestoreValue(BaseModel):
    """A typed Firestore value. Only one field should be set."""

    nullValue: str | None = None
    booleanValue: bool | None = None
    integerValue: str | None = None  # int64 as string
    doubleValue: float | None = None
    timestampValue: str | None = None  # RFC3339
    stringValue: str | None = None
    bytesValue: str | None = None  # base64
    referenceValue: str | None = None
    geoPointValue: dict | None = None
    arrayValue: dict | None = None  # {"values": [...]}
    mapValue: dict | None = None  # {"fields": {...}}


class Document(BaseModel):
    """A Firestore document resource."""

    name: str = ""
    fields: dict[str, Any] = Field(default_factory=dict)
    createTime: str = ""
    updateTime: str = ""


class DocumentMask(BaseModel):
    """A set of field paths that limit which fields are returned or updated."""

    fieldPaths: list[str] = Field(default_factory=list)


class FieldTransform(BaseModel):
    """A transformation to apply to a single document field."""

    fieldPath: str
    setToServerValue: str | None = None
    increment: dict | None = None  # FirestoreValue
    appendMissingElements: dict | None = None  # {"values": [...]}
    removeAllFromArray: dict | None = None  # {"values": [...]}


class Write(BaseModel):
    """A single write operation within a commit or batch write."""

    update: Document | None = None
    delete: str | None = None
    currentDocument: dict | None = None
    updateMask: DocumentMask | None = None
    updateTransforms: list[FieldTransform] = Field(default_factory=list)


class CommitRequest(BaseModel):
    """Request body for a Firestore commit (transactional writes)."""

    writes: list[Write] = Field(default_factory=list)
    transaction: str | None = None


class CommitResponse(BaseModel):
    """Response body for a Firestore commit."""

    writeResults: list[dict] = Field(default_factory=list)
    commitTime: str = ""


class BatchWriteRequest(BaseModel):
    """Request body for a Firestore batch write (non-transactional)."""

    writes: list[Write] = Field(default_factory=list)
    labels: dict = Field(default_factory=dict)


class BatchWriteResponse(BaseModel):
    """Response body for a Firestore batch write."""

    writeResults: list[dict] = Field(default_factory=list)
    status: list[dict] = Field(default_factory=list)


class StructuredQuery(BaseModel):
    """A Firestore structured query."""

    select: dict | None = None
    from_: list[dict] | None = Field(default=None, alias="from")
    where: dict | None = None
    orderBy: list[dict] | None = None
    limit: int | None = None
    offset: int | None = None
    startAt: dict | None = None
    endAt: dict | None = None

    model_config = {"populate_by_name": True}


class RunQueryRequest(BaseModel):
    """Request body for running a Firestore structured query."""

    structuredQuery: StructuredQuery | None = None
    transaction: str | None = None
    newTransaction: dict | None = None
    readTime: str | None = None


class AggregationConfig(BaseModel):
    """Configuration for a Firestore aggregation query."""

    structuredQuery: StructuredQuery | None = None
    aggregations: list[dict] = Field(default_factory=list)


class RunAggregationQueryRequest(BaseModel):
    """Request body for running a Firestore aggregation query."""

    structuredAggregationQuery: AggregationConfig | None = None
    transaction: str | None = None
    newTransaction: dict | None = None
    readTime: str | None = None


class BatchGetRequest(BaseModel):
    """Request body for batch-getting multiple Firestore documents."""

    documents: list[str]
    mask: DocumentMask | None = None
    transaction: str | None = None


class ListDocumentsResponse(BaseModel):
    """Response body for listing Firestore documents."""

    documents: list[Document] = Field(default_factory=list)
    nextPageToken: str | None = None
