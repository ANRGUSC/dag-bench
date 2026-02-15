"""Recompute and fix graph stats for ALL workflows.

Loads each workflow's graph.json, computes fresh stats from the actual graph,
and overwrites metadata.yaml with corrected values (preserving all other fields).
Reports all changes made.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.stats import compute_graph_stats
from dagbench.loader import load_workflow_from_path

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"


def recompute_all():
    changes = []
    errors = []

    for meta_path in sorted(WORKFLOWS_DIR.rglob("metadata.yaml")):
        wf_dir = meta_path.parent
        graph_path = wf_dir / "graph.json"

        if not graph_path.exists():
            errors.append(f"  SKIP {wf_dir.name}: no graph.json")
            continue

        try:
            # Load and compute fresh stats
            instance = load_workflow_from_path(graph_path)
            stats = compute_graph_stats(instance.task_graph)

            # Load existing metadata
            with open(meta_path, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f)

            old_stats = raw.get("graph_stats", {})
            new_stats = {
                "num_tasks": stats.num_tasks,
                "num_edges": stats.num_edges,
                "depth": stats.depth,
                "width": stats.width,
                "ccr": round(stats.ccr, 4) if stats.ccr is not None else None,
                "parallelism": round(stats.parallelism, 4) if stats.parallelism is not None else None,
            }

            # Compare and report differences
            diffs = []
            for key in new_stats:
                old_val = old_stats.get(key)
                new_val = new_stats[key]
                # Handle float comparison
                if isinstance(old_val, float) and isinstance(new_val, float):
                    if abs(old_val - new_val) > 0.001:
                        diffs.append(f"{key}: {old_val} -> {new_val}")
                elif old_val != new_val:
                    diffs.append(f"{key}: {old_val} -> {new_val}")

            if diffs:
                wf_id = raw.get("id", wf_dir.name)
                changes.append((wf_id, diffs))
                raw["graph_stats"] = new_stats
                with open(meta_path, "w", encoding="utf-8") as f:
                    yaml.dump(raw, f, default_flow_style=False, sort_keys=False,
                              allow_unicode=False)
                print(f"  FIXED {wf_id}: {', '.join(diffs)}")
            else:
                pass  # No changes needed

        except Exception as e:
            errors.append(f"  ERROR {wf_dir.name}: {e}")

    print(f"\n=== Summary ===")
    print(f"  {len(changes)} workflows updated")
    if errors:
        print(f"  {len(errors)} errors:")
        for e in errors:
            print(e)

    return changes


if __name__ == "__main__":
    print("Recomputing stats for all workflows...")
    recompute_all()
