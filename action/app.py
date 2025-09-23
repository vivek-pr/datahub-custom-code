"""Entry point for the FastAPI application."""

from __future__ import annotations

import logging
import os

from fastapi import FastAPI

from .datahub_client import DataHubClient
from .mcl_consumer import MCLConsumer
from .pii_detector import PiiDetector
from .router import router
from .run_manager import RunManager
from .sdk_adapter import TokenizationSDKAdapter

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_app() -> FastAPI:
    app = FastAPI(title="Tokenize POC Action", version="0.2.0")
    app.include_router(router)

    @app.on_event("startup")
    async def startup_event() -> None:
        gms = os.environ.get("DATAHUB_GMS")
        if not gms:
            LOGGER.warning(
                "DATAHUB_GMS is not configured; tokenization consumer disabled"
            )
            return
        token = os.environ.get("DATAHUB_TOKEN")
        poll_interval = int(os.environ.get("TOKENIZE_POLL_INTERVAL", "10"))
        batch_limit = int(os.environ.get("TOKENIZE_BATCH_LIMIT", "100"))

        try:
            client = DataHubClient(gms_endpoint=gms, token=token)
        except Exception as exc:  # pragma: no cover - startup validation
            LOGGER.error("Failed to initialise DataHub client: %s", exc)
            return

        detector = PiiDetector.from_env()
        adapter = TokenizationSDKAdapter.from_env()
        manager = RunManager(client, detector, adapter, batch_limit=batch_limit)
        consumer = MCLConsumer(client, manager, poll_interval=poll_interval)
        consumer.start()
        app.state.consumer = consumer
        app.state.run_manager = manager
        LOGGER.info(
            "Tokenize action ready (poll interval=%ss, batch limit=%s)",
            poll_interval,
            batch_limit,
        )

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        consumer: MCLConsumer | None = getattr(app.state, "consumer", None)
        if consumer:
            consumer.stop()

    return app


app = create_app()
