"""Token generation and detection utilities for the POC."""

from __future__ import annotations

import base64
import logging
import re
from typing import Optional

TOKEN_PREFIX = "tok_"
TOKEN_SUFFIX = "_poc"
TOKEN_REGEX = re.compile(r"^tok_[A-Za-z0-9+/=]+_poc$")

logger = logging.getLogger(__name__)


def generate_token(value: str) -> str:
    """Return the deterministic dummy token for a plaintext value."""
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    token = f"{TOKEN_PREFIX}{encoded}{TOKEN_SUFFIX}"
    logger.debug("Generated token for value of length %s", len(value))
    return token


def is_token(value: Optional[str]) -> bool:
    """Return True if the value already matches the dummy token format."""
    if value is None:
        return False
    return bool(TOKEN_REGEX.match(value))


def tokenize_if_needed(value: Optional[str]) -> Optional[str]:
    """Tokenize value when required and return the new token.

    None values are returned unchanged. Existing tokens are also returned unchanged
    to ensure idempotency.
    """

    if value is None:
        return None
    if is_token(value):
        return value
    return generate_token(value)
