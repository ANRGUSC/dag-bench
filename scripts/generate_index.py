"""Generate the master index.yaml from all workflow metadata."""
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.catalog import list_workflows

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"


def main():
    workflows = list_workflows(workflows_dir=WORKFLOWS_DIR)
    index = {
        "version": "0.1.0",
        "total_workflows": len(workflows),
        "workflows": [],
    }

    for wf in sorted(workflows, key=lambda w: w.id):
        index["workflows"].append({
            "id": wf.id,
            "name": wf.name,
            "domains": [d.value for d in wf.domains],
            "num_tasks": wf.graph_stats.num_tasks,
            "num_edges": wf.graph_stats.num_edges,
            "completeness": wf.completeness.value,
        })

    index_path = WORKFLOWS_DIR / "index.yaml"
    with open(index_path, "w", encoding="utf-8") as f:
        yaml.dump(index, f, default_flow_style=False, sort_keys=False)

    print(f"Generated index.yaml with {len(workflows)} workflows")


if __name__ == "__main__":
    main()
