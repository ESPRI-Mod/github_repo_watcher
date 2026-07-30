"""Write generated JSON files to disk."""

from __future__ import annotations

import json
from pathlib import Path

from repo_watcher.generation.report import GenerationReport


def write_report(report: GenerationReport, universe_repo: Path | None = None, project_repo: Path | None = None) -> None:
    """Write the JSON files planned in a GenerationReport to disk."""
    if universe_repo is not None:
        for tf in report.universe_to_create:
            path = universe_repo / tf.descriptor / f"{tf.term_id}.json"
            path.write_text(json.dumps(tf.content, indent=2, ensure_ascii=False) + "\n")
        for fc in report.universe_to_update:
            path = universe_repo / fc.descriptor / f"{fc.term_id}.json"
            existing = json.loads(path.read_text())
            existing.update({f: new for f, (_old, new) in fc.changes.items()})
            path.write_text(json.dumps(existing, indent=2, ensure_ascii=False) + "\n")
    if project_repo is not None:
        for tf in report.project_to_create:
            path = project_repo / tf.descriptor / f"{tf.term_id}.json"
            path.write_text(json.dumps(tf.content, indent=2, ensure_ascii=False) + "\n")
