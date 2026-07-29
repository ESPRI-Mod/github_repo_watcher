"""Extract field-level terms from EMD src-data format."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from repo_watcher.config.schema import FieldMappingConfig
from repo_watcher.domain import Term

logger = logging.getLogger(__name__)


def extract_terms_emd(
    repo_directory: Path,
    field_mappings: dict[str, FieldMappingConfig],
) -> set[Term]:
    """Extract field-level terms from EMD src-data format.

    Each collection is a subdirectory with one JSON per term.
    field_mappings maps source collection to target collection + field name mappings.
    """
    result: set[Term] = set()

    for source_collection, mapping in field_mappings.items():
        collection_dir = repo_directory / source_collection
        if not collection_dir.is_dir():
            continue

        target_collection = mapping["target_collection"]
        fields = mapping["fields"]

        for term_file in sorted(collection_dir.glob("*.json")):
            try:
                data = json.loads(term_file.read_text())
            except json.JSONDecodeError:
                logger.warning("Skipping malformed JSON: %s", term_file)
                continue
            raw_id = data.get("@id") or data.get("id") or term_file.stem
            term_id = raw_id.replace("-", "_")

            # Skip computational grids: output grids have a single
            # region while computational grids list multiple regions.
            region = data.get("region")
            if isinstance(region, list) and len(region) > 1:
                logger.info("Skipping computational grid %s (multi-region)", term_id)
                continue

            for source_field, target_field in fields.items():
                if source_field in data:
                    result.add(
                        Term(
                            name=term_id,
                            collection=target_collection,
                            field=target_field,
                            value=str(data[source_field]),
                        ),
                    )

    return result
