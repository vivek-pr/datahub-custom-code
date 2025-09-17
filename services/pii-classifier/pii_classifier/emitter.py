"""DataHub tag emission utilities."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Sequence, Set

from datahub.emitter.mce_builder import make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    TagAssociationClass,
    TagPropertiesClass,
)

from .config import DataHubConfig

LOGGER = logging.getLogger(__name__)


@dataclass
class TagUpsertResult:
    schema_field_urn: str
    tag_urn: str
    was_emitted: bool
    confidence: float
    rule_name: str
    reason: str


class DataHubTagEmitter:
    def __init__(self, config: DataHubConfig):
        self._config = config
        self._emitter = DatahubRestEmitter(
            gms_server=config.gms,
            token=config.token,
        )

    @property
    def dry_run(self) -> bool:
        return self._config.dry_run

    def ensure_tag_definitions(self, tag_specs: Sequence[tuple[str, str, Optional[str]]]) -> None:
        for tag_urn, name, description in tag_specs:
            aspect = TagPropertiesClass(name=name, description=description)
            mcp = MetadataChangeProposalWrapper(entityUrn=tag_urn, aspect=aspect)
            if self.dry_run:
                LOGGER.info("[dry-run] Would ensure tag definition %s (%s)", tag_urn, name)
                continue
            LOGGER.debug("Ensuring tag definition for %s", tag_urn)
            self._emitter.emit(mcp)

    def add_field_tag(self, dataset_name: str, field: str, tag_urn: str, confidence: float, rule_name: str, reason: str) -> TagUpsertResult:
        dataset_urn = make_dataset_urn(self._config.platform, dataset_name, self._config.env)
        schema_field_urn = make_schema_field_urn(dataset_urn, field)
        existing = self._get_existing_tags(schema_field_urn)
        if tag_urn in existing:
            LOGGER.info("Tag already present: %s -> %s", schema_field_urn, tag_urn)
            return TagUpsertResult(
                schema_field_urn=schema_field_urn,
                tag_urn=tag_urn,
                was_emitted=False,
                confidence=confidence,
                rule_name=rule_name,
                reason=reason,
            )
        merged = sorted(existing | {tag_urn})
        aspect = GlobalTagsClass(tags=[TagAssociationClass(tag=urn) for urn in merged])
        mcp = MetadataChangeProposalWrapper(entityUrn=schema_field_urn, aspect=aspect)
        if self.dry_run:
            LOGGER.info(
                "[dry-run] Would tag %s with %s (confidence=%.2f, rule=%s, reason=%s)",
                schema_field_urn,
                tag_urn,
                confidence,
                rule_name,
                reason,
            )
            return TagUpsertResult(
                schema_field_urn=schema_field_urn,
                tag_urn=tag_urn,
                was_emitted=False,
                confidence=confidence,
                rule_name=rule_name,
                reason=reason,
            )
        LOGGER.info(
            "Tagging %s with %s (confidence=%.2f, rule=%s, reason=%s)",
            schema_field_urn,
            tag_urn,
            confidence,
            rule_name,
            reason,
        )
        self._emitter.emit(mcp)
        return TagUpsertResult(
            schema_field_urn=schema_field_urn,
            tag_urn=tag_urn,
            was_emitted=True,
            confidence=confidence,
            rule_name=rule_name,
            reason=reason,
        )

    def _get_existing_tags(self, schema_field_urn: str) -> Set[str]:
        try:
            aspect_obj = self._emitter.get_aspect(schema_field_urn, "schemaFieldTags")
        except Exception as exc:
            LOGGER.debug("Failed to fetch existing tags for %s: %s", schema_field_urn, exc)
            return set()
        if aspect_obj is None:
            return set()
        if isinstance(aspect_obj, dict):
            aspect = GlobalTagsClass.from_obj(aspect_obj)
        else:
            aspect = aspect_obj
        return {tag_assoc.tag for tag_assoc in aspect.tags or []}
