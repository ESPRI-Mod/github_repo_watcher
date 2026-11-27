"""Write generated JSON files to disk."""

from __future__ import annotations

import json
from pathlib import Path

from repo_watcher.generation.report import GenerationReport


def write_report(report: GenerationReport, universe_repo: Path | None = None, project_repo: Path | None = None) -> None:
    """Write the JSON files planned in a GenerationReport to disk (4-space indent, as in the repos)."""
    if universe_repo is not None:
        for tf in report.universe_to_create:
            path = universe_repo / tf.descriptor / f"{tf.term_id}.json"
            path.write_text(json.dumps(tf.content, indent=4, ensure_ascii=False) + "\n")
        for fc in report.universe_to_update:
            path = universe_repo / fc.descriptor / f"{fc.term_id}.json"
            existing = json.loads(path.read_text())
            existing.update({f: new for f, (_old, new) in fc.changes.items()})
            path.write_text(json.dumps(existing, indent=4, ensure_ascii=False) + "\n")
    if project_repo is not None:
        for tf in report.project_to_create:
            path = project_repo / tf.descriptor / f"{tf.term_id}.json"
            path.write_text(json.dumps(tf.content, indent=4, ensure_ascii=False) + "\n")
        # Project updates are merged into the existing files.
        for fc in report.project_to_update:
            path = project_repo / fc.descriptor / f"{fc.term_id}.json"
            existing = json.loads(path.read_text())
            for f, (_old, new) in fc.changes.items():
                if new is None:
                    existing.pop(f, None)
                else:
                    existing[f] = new
            path.write_text(json.dumps(existing, indent=4, ensure_ascii=False) + "\n")
        for cc in report.project_context_to_update:
            path = project_repo / cc.collection / "000_context.jsonld"
            existing = json.loads(path.read_text())
            existing["@context"].update(cc.terms)
            path.write_text(json.dumps(existing, indent=4, ensure_ascii=False) + "\n")
