"""Entry point for the FastAPI application."""

from __future__ import annotations

import logging
from fastapi import FastAPI

from .router import router

logging.basicConfig(level=logging.INFO)


def create_app() -> FastAPI:
    app = FastAPI(title="Tokenize POC Action", version="0.1.0")
    app.include_router(router)
    return app


app = create_app()
