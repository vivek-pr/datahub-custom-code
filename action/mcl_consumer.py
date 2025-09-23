"""Background worker that polls DataHub for tokenize/run tags."""

from __future__ import annotations

import logging
import threading
from typing import Optional, Sequence, Set

from .datahub_client import DataHubClient, RUN_TAG_URN
from .models import DatasetMetadata
from .run_manager import RunManager

LOGGER = logging.getLogger(__name__)


class MCLConsumer:
    """Simple polling consumer that emulates an MCL listener."""

    def __init__(
        self,
        client: DataHubClient,
        run_manager: RunManager,
        *,
        poll_interval: int = 10,
    ) -> None:
        self.client = client
        self.run_manager = run_manager
        self.poll_interval = max(poll_interval, 5)
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="tokenize-mcl-consumer",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info("MCL consumer started (interval=%ss)", self.poll_interval)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=self.poll_interval + 5)
            LOGGER.info("MCL consumer stopped")

    # ------------------------------------------------------------------
    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception:  # pragma: no cover - background logging
                LOGGER.exception("Failed to poll DataHub for tokenize/run tags")
            self._stop.wait(self.poll_interval)

    def _poll_once(self) -> None:
        dataset_urns = self.client.list_all_dataset_urns()
        for urn in dataset_urns:
            dataset = self.client.get_dataset(urn)
            if not dataset:
                continue
            field_scope = self._fields_with_run_tag(dataset)
            dataset_tagged = RUN_TAG_URN in dataset.global_tags
            if not dataset_tagged and not field_scope:
                continue
            LOGGER.info(
                "Detected tokenize/run tag on %s (dataset=%s, fields=%s)",
                urn,
                dataset_tagged,
                len(field_scope),
            )
            explicit_scope: Optional[Sequence[str]] = (
                None if dataset_tagged else list(field_scope)
            )
            self.run_manager.process(dataset, explicit_scope)

    @staticmethod
    def _fields_with_run_tag(dataset: DatasetMetadata) -> Set[str]:
        return {
            field.field_path for field in dataset.fields if RUN_TAG_URN in field.tags
        }
