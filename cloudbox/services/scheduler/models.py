"""Pydantic models for Cloud Scheduler REST API v1."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HttpTarget(BaseModel):
    """HTTP target configuration for a scheduled job."""

    uri: str
    httpMethod: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: str = ""  # base64-encoded


class RetryConfig(BaseModel):
    """Retry policy for a Cloud Scheduler job."""

    retryCount: int = 0
    maxRetryDuration: str = "0s"
    minBackoffDuration: str = "5s"
    maxBackoffDuration: str = "1h"
    maxDoublings: int = 5


class JobModel(BaseModel):
    """A Cloud Scheduler job resource."""

    name: str = ""
    description: str = ""
    schedule: str = ""
    timeZone: str = "UTC"
    state: str = "ENABLED"  # ENABLED, PAUSED, DISABLED
    httpTarget: HttpTarget | None = None
    retryConfig: RetryConfig = Field(default_factory=RetryConfig)
    userUpdateTime: str = ""
    scheduleTime: str = ""  # next scheduled run
    lastAttemptTime: str = ""
    status: dict[str, Any] = Field(default_factory=dict)


class JobListResponse(BaseModel):
    """Response body for listing Cloud Scheduler jobs."""

    jobs: list[JobModel] = Field(default_factory=list)
    nextPageToken: str | None = None
