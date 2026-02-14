"""Generate random layered DAG workflows for the synthetic domain.

Uses the random_layered_dag generator (clean reimplementation of ANRGUSC's
Automatic-DAG-Generator algorithm, BSD license) to produce 9 pre-baked
workflows with varying structure and cost characteristics.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import yaml
from saga import Network
from saga.schedulers.data import ProblemInstance

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.generators.random_dag import random_layered_dag
from dagbench.networks import homogeneous_network
from dagbench.stats import compute_graph_stats

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"
TODAY = date.today().isoformat()


def _save_workflow(wf_dir: Path, instance: ProblemInstance, metadata: dict) -> None:
    wf_dir.mkdir(parents=True, exist_ok=True)
    raw = json.loads(instance.model_dump_json(indent=2))
    if "network" in raw and "edges" in raw["network"]:
        for e in raw["network"]["edges"]:
            if e.get("speed") is None:
                e["speed"] = 1e9
    with open(wf_dir / "graph.json", "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)
    with open(wf_dir / "metadata.yaml", "w", encoding="utf-8") as f:
        yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)
    print(f"  Saved: {wf_dir.relative_to(PROJECT_ROOT)}")


def _make_metadata(wf_id, name, description, task_graph, tags=None):
    stats = compute_graph_stats(task_graph)
    return {
        "id": wf_id,
        "name": name,
        "description": description,
        "domains": ["synthetic"],
        "provenance": {
            "source": "Random layered DAG generator",
            "repo_url": "https://github.com/ANRGUSC/Automatic-DAG-Generator",
            "extraction_method": "generated",
            "extractor": "dagbench-random-dag-generator",
            "extraction_date": TODAY,
            "notes": "Clean reimplementation of ANRGUSC Automatic-DAG-Generator algorithm (Sep 2018, BSD license). Structure and costs are randomly generated with fixed seed for reproducibility.",
        },
        "license": {
            "source_license": "BSD-3-Clause",
            "dagbench_license": "Apache-2.0",
            "notes": "Algorithm inspired by ANRGUSC/Automatic-DAG-Generator; this is a clean reimplementation",
        },
        "completeness": "full",
        "cost_model": "deterministic",
        "network": {
            "included": True,
            "topology": "fully-connected",
            "num_nodes_min": 4,
            "num_nodes_max": 4,
        },
        "graph_stats": {
            "num_tasks": stats.num_tasks,
            "num_edges": stats.num_edges,
            "depth": stats.depth,
            "width": stats.width,
            "ccr": round(stats.ccr, 4) if stats.ccr else None,
            "parallelism": round(stats.parallelism, 4) if stats.parallelism else None,
        },
        "campaign": "campaign_004_random_layered",
        "tags": tags,
    }


# --- Workflow definitions ---

RANDOM_DAGS = [
    {
        "dir": "random_small_narrow",
        "id": "synthetic.random_small_narrow",
        "name": "Random Small Narrow DAG",
        "desc": "Small narrow random layered DAG (depth=4, width 1-3). Tests scheduling on thin, sequential-ish graphs.",
        "params": dict(depth=4, width_min=1, width_max=3, ccr=0.5, seed=42),
        "tags": ["random-layered", "small", "narrow"],
    },
    {
        "dir": "random_small_wide",
        "id": "synthetic.random_small_wide",
        "name": "Random Small Wide DAG",
        "desc": "Small wide random layered DAG (depth=2, width 4-8). High parallelism relative to depth.",
        "params": dict(depth=2, width_min=4, width_max=8, ccr=0.5, seed=43),
        "tags": ["random-layered", "small", "wide"],
    },
    {
        "dir": "random_medium_balanced",
        "id": "synthetic.random_medium_balanced",
        "name": "Random Medium Balanced DAG",
        "desc": "Medium random layered DAG with balanced depth and width (depth=6, width 3-6).",
        "params": dict(depth=6, width_min=3, width_max=6, ccr=0.5, seed=44),
        "tags": ["random-layered", "medium", "balanced"],
    },
    {
        "dir": "random_medium_deep",
        "id": "synthetic.random_medium_deep",
        "name": "Random Medium Deep DAG",
        "desc": "Medium deep random layered DAG (depth=12, width 2-4). Long critical paths.",
        "params": dict(depth=12, width_min=2, width_max=4, ccr=0.5, seed=45),
        "tags": ["random-layered", "medium", "deep"],
    },
    {
        "dir": "random_medium_compute",
        "id": "synthetic.random_medium_compute",
        "name": "Random Medium Compute-Heavy DAG",
        "desc": "Medium random DAG with low CCR (0.1) -- computation dominates communication.",
        "params": dict(depth=6, width_min=3, width_max=6, ccr=0.1, seed=46),
        "tags": ["random-layered", "medium", "compute-heavy", "low-ccr"],
    },
    {
        "dir": "random_medium_comm",
        "id": "synthetic.random_medium_comm",
        "name": "Random Medium Comm-Heavy DAG",
        "desc": "Medium random DAG with high CCR (2.0) -- communication dominates computation.",
        "params": dict(depth=6, width_min=3, width_max=6, ccr=2.0, seed=47),
        "tags": ["random-layered", "medium", "comm-heavy", "high-ccr"],
    },
    {
        "dir": "random_large_balanced",
        "id": "synthetic.random_large_balanced",
        "name": "Random Large Balanced DAG",
        "desc": "Large random layered DAG (depth=10, width 5-12). Stress-tests scheduling algorithms.",
        "params": dict(depth=10, width_min=5, width_max=12, ccr=0.5, seed=48),
        "tags": ["random-layered", "large", "balanced"],
    },
    {
        "dir": "random_large_dense",
        "id": "synthetic.random_large_dense",
        "name": "Random Large Dense DAG",
        "desc": "Large dense random DAG (deg_mu=6, delta_lvl=3). High connectivity, many cross-level edges.",
        "params": dict(depth=8, width_min=5, width_max=10, deg_mu=6.0, delta_lvl=3, ccr=0.5, seed=49),
        "tags": ["random-layered", "large", "dense"],
    },
    {
        "dir": "random_xlarge",
        "id": "synthetic.random_xlarge",
        "name": "Random Extra-Large DAG",
        "desc": "Extra-large random layered DAG (depth=15, width 6-15). Largest random benchmark for scalability testing.",
        "params": dict(depth=15, width_min=6, width_max=15, ccr=0.5, seed=50),
        "tags": ["random-layered", "xlarge", "stress-test"],
    },
]


def main():
    print(f"DAGBench Random Layered DAG Import - {TODAY}")
    print("=" * 50)

    net = homogeneous_network(num_nodes=4, speed=1.0, bandwidth=100.0)

    for dag_def in RANDOM_DAGS:
        tg = random_layered_dag(**dag_def["params"])
        print(f"\n{dag_def['name']}: {len(tg.tasks)} tasks, {len(tg.dependencies)} edges")

        wf_dir = WORKFLOWS_DIR / "synthetic" / dag_def["dir"]
        instance = ProblemInstance(name=dag_def["id"], task_graph=tg, network=net)
        metadata = _make_metadata(
            dag_def["id"], dag_def["name"], dag_def["desc"], tg, dag_def["tags"]
        )
        _save_workflow(wf_dir, instance, metadata)

    print(f"\n=== Done: {len(RANDOM_DAGS)} random DAG workflows generated ===")


if __name__ == "__main__":
    main()
