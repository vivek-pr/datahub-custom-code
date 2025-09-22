"""HTTP routing for the FastAPI application."""

from __future__ import annotations

import logging
import os
import time

from fastapi import APIRouter, HTTPException

from . import db_dbx, db_pg
from .models import HealthResponse, TriggerRequest, TriggerResponse
from .sdk_adapter import TokenizationSDKAdapter

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/healthz", response_model=HealthResponse)
async def healthz() -> HealthResponse:
    return HealthResponse()


@router.post("/trigger", response_model=TriggerResponse)
async def trigger(request: TriggerRequest) -> TriggerResponse:
    dataset = request.dataset_ref
    adapter = TokenizationSDKAdapter.from_env()

    start = time.perf_counter()
    if dataset.platform == "postgres":
        conn_str = os.environ.get("PG_CONN_STR")
        if not conn_str:
            raise HTTPException(status_code=500, detail="PG_CONN_STR is not configured")
        result = db_pg.tokenize_table(
            conn_str, dataset, request.columns, request.limit, adapter
        )
    elif dataset.platform == "databricks":
        jdbc_url = os.environ.get("DBX_JDBC_URL")
        if not jdbc_url:
            raise HTTPException(
                status_code=500, detail="DBX_JDBC_URL is not configured"
            )
        result = db_dbx.tokenize_table(
            jdbc_url, dataset, request.columns, request.limit, adapter
        )
    else:
        raise HTTPException(
            status_code=400, detail=f"Unsupported platform: {dataset.platform}"
        )

    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "Trigger completed for %s: %s updates, %s skipped in %.2fms",
        dataset.table_expression,
        result["updated_count"],
        result["skipped_count"],
        elapsed_ms,
    )
    return TriggerResponse(
        updated_count=result["updated_count"],
        skipped_count=result["skipped_count"],
        platform=dataset.platform,
        elapsed_ms=round(elapsed_ms, 2),
    )
