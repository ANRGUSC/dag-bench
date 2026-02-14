"""Generate 3 new synthetic workflows: one_task, chain_2, random_xxlarge.

Extends the synthetic domain with edge cases (single task, minimal chain)
and a large-scale benchmark (~1000 tasks).
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import yaml
from saga import TaskGraph, TaskGraphNode, Network
from saga.schedulers.data import ProblemInstance

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.converters.manual import DAGBuilder
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
            "source": "DAGBench synthetic generator",
            "extraction_method": "generated",
            "extractor": "dagbench-synthetic-generator",
            "extraction_date": TODAY,
            "notes": "Synthetic workflow generated for benchmarking edge cases and scalability testing.",
        },
        "license": {
            "source_license": "Apache-2.0",
            "dagbench_license": "Apache-2.0",
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
        "campaign": "campaign_005_new_synthetics",
        "tags": tags,
    }


def main():
    print(f"DAGBench New Synthetics Import - {TODAY}")
    print("=" * 50)

    net = homogeneous_network(num_nodes=4, speed=1.0, bandwidth=100.0)

    # 1. one_task: single task, no edges
    print("\n1. one_task: Single task DAG")
    tg_one = TaskGraph(
        tasks=frozenset([TaskGraphNode(name="T0", cost=10.0)]),
        dependencies=frozenset(),
    )
    print(f"   {len(tg_one.tasks)} tasks, {len(tg_one.dependencies)} edges")
    wf_dir = WORKFLOWS_DIR / "synthetic" / "one_task"
    instance = ProblemInstance(name="synthetic.one_task", task_graph=tg_one, network=net)
    metadata = _make_metadata(
        "synthetic.one_task",
        "Single Task DAG",
        "Degenerate base case: a single task with no edges. Tests scheduler behavior on trivial input.",
        tg_one,
        ["single-task", "base-case", "tiny"],
    )
    _save_workflow(wf_dir, instance, metadata)

    # 2. chain_2: two tasks with one edge
    print("\n2. chain_2: Two-task chain DAG")
    tg_chain = (
        DAGBuilder("chain_2")
        .task("A", 10.0)
        .task("B", 15.0)
        .edge("A", "B", 5.0)
        .build()
    )
    print(f"   {len(tg_chain.tasks)} tasks, {len(tg_chain.dependencies)} edges")
    wf_dir = WORKFLOWS_DIR / "synthetic" / "chain_2"
    instance = ProblemInstance(name="synthetic.chain_2", task_graph=tg_chain, network=net)
    metadata = _make_metadata(
        "synthetic.chain_2",
        "Chain DAG (2 nodes)",
        "Minimal dependency DAG: two tasks A->B with one edge. The smallest non-trivial DAG.",
        tg_chain,
        ["chain", "minimal", "tiny"],
    )
    _save_workflow(wf_dir, instance, metadata)

    # 3. random_xxlarge: ~1000 tasks
    print("\n3. random_xxlarge: Extra-extra-large random DAG (~1000 tasks)")
    tg_xxl = random_layered_dag(
        depth=20, width_min=30, width_max=70, ccr=0.5, seed=51
    )
    print(f"   {len(tg_xxl.tasks)} tasks, {len(tg_xxl.dependencies)} edges")
    wf_dir = WORKFLOWS_DIR / "synthetic" / "random_xxlarge"
    instance = ProblemInstance(name="synthetic.random_xxlarge", task_graph=tg_xxl, network=net)
    metadata = _make_metadata(
        "synthetic.random_xxlarge",
        "Random XXL DAG",
        f"Extra-extra-large random layered DAG (depth=20, width 30-70, ~{len(tg_xxl.tasks)} tasks). Largest benchmark for extreme scalability testing.",
        tg_xxl,
        ["random-layered", "xxlarge", "stress-test"],
    )
    # Update provenance with random DAG specifics
    metadata["provenance"]["source"] = "Random layered DAG generator"
    metadata["provenance"]["repo_url"] = "https://github.com/ANRGUSC/Automatic-DAG-Generator"
    metadata["provenance"]["notes"] = (
        "Clean reimplementation of ANRGUSC Automatic-DAG-Generator algorithm "
        "(Sep 2018, BSD license). Structure and costs are randomly generated "
        "with fixed seed for reproducibility."
    )
    metadata["license"]["source_license"] = "BSD-3-Clause"
    _save_workflow(wf_dir, instance, metadata)

    print(f"\n=== Done: 3 new synthetic workflows generated ===")


if __name__ == "__main__":
    main()
