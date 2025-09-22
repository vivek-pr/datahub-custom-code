"""Thin adapter around the tokenization SDK."""

from __future__ import annotations

import logging
import os
from typing import Iterable, List

from . import token_logic

logger = logging.getLogger(__name__)


class TokenizationSDKAdapter:
    """Adapter that hides the underlying SDK implementation details."""

    def __init__(self, mode: str = "dummy") -> None:
        self.mode = mode
        logger.debug("Tokenization SDK adapter initialised in %s mode", mode)
        if mode != "dummy":
            raise NotImplementedError("Only dummy SDK mode is implemented for the POC.")

    @classmethod
    def from_env(cls) -> "TokenizationSDKAdapter":
        mode = os.environ.get("TOKEN_SDK_MODE", "dummy")
        return cls(mode=mode)

    def tokenize(self, value: str) -> str:
        if self.mode == "dummy":
            return token_logic.generate_token(value)
        raise NotImplementedError("Unsupported SDK mode")

    def tokenize_many(self, values: Iterable[str]) -> List[str]:
        return [self.tokenize(value) for value in values]

    def is_token(self, value: str) -> bool:
        return token_logic.is_token(value)
