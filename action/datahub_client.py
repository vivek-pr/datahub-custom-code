"""Lightweight GraphQL client tailored for DataHub interactions."""

from __future__ import annotations

import json
import logging
from typing import Dict, Iterable, List, Optional

import requests

from .models import DatasetMetadata, DatasetRef

LOGGER = logging.getLogger(__name__)

RUN_TAG_URN = "urn:li:tag:tokenize/run"


class DataHubClient:
    """Wrapper around the DataHub GraphQL endpoint used by the action."""

    def __init__(
        self, gms_endpoint: str, token: Optional[str] = None, timeout: int = 15
    ) -> None:
        if not gms_endpoint:
            raise ValueError("DATAHUB_GMS must be configured")
        self._base_url = gms_endpoint.rstrip("/")
        self._graphql_url = f"{self._base_url}/graphql"
        self._token = token
        self._timeout = timeout

    # ------------------------------------------------------------------
    # GraphQL helpers
    # ------------------------------------------------------------------
    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def execute(
        self, query: str, variables: Optional[Dict[str, object]] = None
    ) -> Dict[str, object]:
        payload = {"query": query, "variables": variables or {}}
        response = requests.post(
            self._graphql_url,
            headers=self._headers(),
            data=json.dumps(payload),
            timeout=self._timeout,
        )
        if response.status_code >= 400:
            LOGGER.error("GraphQL error %s: %s", response.status_code, response.text)
            raise RuntimeError(f"GraphQL request failed with {response.status_code}")
        data = response.json()
        if data.get("errors"):
            LOGGER.error("GraphQL errors: %s", data["errors"])
            raise RuntimeError("GraphQL request returned errors")
        return data.get("data", {})

    # ------------------------------------------------------------------
    # Dataset discovery
    # ------------------------------------------------------------------
    def list_all_dataset_urns(self, batch_size: int = 50) -> List[str]:
        query = """
        query listDatasets($input: ListEntitiesInput!) {
          listEntities(input: $input) {
            start
            count
            total
            entities {
              urn
            }
          }
        }
        """
        urns: List[str] = []
        start = 0
        while True:
            variables = {
                "input": {
                    "type": "DATASET",
                    "start": start,
                    "count": batch_size,
                }
            }
            data = self.execute(query, variables)
            payload = data.get("listEntities") or {}
            entities = payload.get("entities") or []
            for entity in entities:
                urn = entity.get("urn") if isinstance(entity, dict) else None
                if urn:
                    urns.append(urn)
            start = (payload.get("start") or 0) + (payload.get("count") or 0)
            total = payload.get("total") or 0
            if start >= total:
                break
        return urns

    def get_dataset(self, urn: str) -> Optional[DatasetMetadata]:
        query = """
        query getDataset($urn: String!) {
          dataset(urn: $urn) {
            urn
            name
            platform { urn }
            schemaMetadata {
              fields {
                fieldPath
                nativeDataType
                globalTags {
                  tags {
                    tag { urn }
                  }
                }
              }
            }
            editableSchemaMetadata {
              editableSchemaFieldInfo {
                fieldPath
                globalTags {
                  tags {
                    tag { urn }
                  }
                }
              }
            }
            globalTags {
              tags {
                tag { urn }
              }
            }
            editableProperties {
              customProperties
            }
          }
        }
        """
        data = self.execute(query, {"urn": urn})
        payload = data.get("dataset")
        if not payload:
            return None
        return DatasetMetadata.from_graphql(urn, payload)

    # ------------------------------------------------------------------
    # Tag management
    # ------------------------------------------------------------------
    def add_tag(
        self,
        entity_urn: str,
        tag_urn: str,
        *,
        subresource: Optional[str] = None,
        subresource_type: Optional[str] = None,
    ) -> None:
        mutation = """
        mutation addTag($input: TagAssociationInput!) {
          addTag(input: $input) {
            __typename
          }
        }
        """
        variables: Dict[str, object] = {
            "input": {
                "tagUrn": tag_urn,
                "resourceUrn": entity_urn,
            }
        }
        if subresource:
            variables["input"]["subResource"] = subresource
            variables["input"]["subResourceType"] = subresource_type or "DATASET_FIELD"
        self.execute(mutation, variables)

    def remove_tag(
        self,
        entity_urn: str,
        tag_urn: str,
        *,
        subresource: Optional[str] = None,
        subresource_type: Optional[str] = None,
    ) -> None:
        mutation = """
        mutation removeTag($input: TagAssociationInput!) {
          removeTag(input: $input)
        }
        """
        variables: Dict[str, object] = {
            "input": {
                "tagUrn": tag_urn,
                "resourceUrn": entity_urn,
            }
        }
        if subresource:
            variables["input"]["subResource"] = subresource
            variables["input"]["subResourceType"] = subresource_type or "DATASET_FIELD"
        self.execute(mutation, variables)

    # ------------------------------------------------------------------
    # Editable dataset properties
    # ------------------------------------------------------------------
    def upsert_editable_properties(self, urn: str, properties: Dict[str, str]) -> None:
        mutation = """
        mutation upsertEditableDatasetProperties($input: UpsertEditableDatasetPropertiesInput!) {
          upsertEditableDatasetProperties(input: $input) {
            urn
          }
        }
        """
        serialised = {key: str(value) for key, value in properties.items()}
        variables = {
            "input": {
                "urn": urn,
                "editableDatasetProperties": {
                    "customProperties": serialised,
                },
            }
        }
        self.execute(mutation, variables)

    # ------------------------------------------------------------------
    def ensure_tags(
        self,
        entity_urn: str,
        tags_to_add: Iterable[str],
        tags_to_remove: Iterable[str],
        *,
        field_path: Optional[str] = None,
    ) -> None:
        for tag in tags_to_remove:
            try:
                self.remove_tag(
                    entity_urn,
                    tag,
                    subresource=field_path,
                    subresource_type="DATASET_FIELD" if field_path else None,
                )
            except Exception as exc:  # pragma: no cover - best effort logging
                LOGGER.debug(
                    "Failed to remove tag %s from %s: %s", tag, entity_urn, exc
                )
        for tag in tags_to_add:
            try:
                self.add_tag(
                    entity_urn,
                    tag,
                    subresource=field_path,
                    subresource_type="DATASET_FIELD" if field_path else None,
                )
            except Exception as exc:  # pragma: no cover - best effort logging
                LOGGER.debug("Failed to add tag %s to %s: %s", tag, entity_urn, exc)


def to_dataset_ref(urn: str) -> DatasetRef:
    return DatasetRef.from_urn(urn)
