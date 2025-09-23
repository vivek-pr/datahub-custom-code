"""HTTP routing for the FastAPI application."""

from __future__ import annotations

from fastapi import APIRouter

from .models import HealthResponse

router = APIRouter()


@router.get("/healthz", response_model=HealthResponse)
async def healthz() -> HealthResponse:
    return HealthResponse()
