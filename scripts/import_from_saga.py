"""Import workflows from SAGA's built-in dataset generators.

Imports:
- RIoTBench IoT workflows (etl, stats, train, predict)
- Synthetic DAG patterns (diamond, chain, fork, branching)
- Fog network topology presets

Each workflow gets:
- graph.json (SAGA ProblemInstance format)
- metadata.yaml (DAGBench provenance tracking)
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import yaml
from saga import TaskGraph, Network
from saga.schedulers.data import ProblemInstance
from saga.schedulers.data.riotbench import (
    get_etl_task_graphs,
    get_stats_task_graphs,
    get_train_task_graphs,
    get_predict_task_graphs,
    get_fog_networks,
)
from saga.utils.random_graphs import (
    get_diamond_dag,
    get_chain_dag,
    get_fork_dag,
    get_branching_dag,
    get_network,
)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.stats import compute_graph_stats
from dagbench.schema import (
    WorkflowMetadata,
    Provenance,
    LicenseInfo,
    NetworkInfo,
    ExtractionMethod,
    Domain,
    Completeness,
    CostModel,
    NetworkTopology,
)

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"
TODAY = date.today().isoformat()

SAGA_PROVENANCE_BASE = {
    "source": "SAGA framework (anrg-saga)",
    "paper_arxiv": "2403.07120",
    "paper_title": "PISA: An Adversarial Approach To Comparing Task Graph Scheduling Algorithms",
    "authors": ["Jared Coleman", "Bhaskar Krishnamachari"],
    "year": 2024,
    "repo_url": "https://github.com/anrgusc/saga",
    "extraction_method": "programmatic",
    "extractor": "dagbench-import-script",
    "extraction_date": TODAY,
}


def _save_workflow(
    wf_dir: Path,
    instance: ProblemInstance,
    metadata_dict: dict,
) -> None:
    """Save a ProblemInstance and metadata to a workflow directory."""
    wf_dir.mkdir(parents=True, exist_ok=True)

    # Write graph.json - fix null speed self-loops
    raw = json.loads(instance.model_dump_json(indent=2))
    if "network" in raw and "edges" in raw["network"]:
        for e in raw["network"]["edges"]:
            if e.get("speed") is None:
                e["speed"] = 1e9
    with open(wf_dir / "graph.json", "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)

    # Write metadata.yaml
    with open(wf_dir / "metadata.yaml", "w", encoding="utf-8") as f:
        yaml.dump(metadata_dict, f, default_flow_style=False, sort_keys=False)

    print(f"  Saved: {wf_dir.relative_to(PROJECT_ROOT)}")


def _make_metadata(
    wf_id: str,
    name: str,
    description: str,
    domains: list[str],
    task_graph: TaskGraph,
    completeness: str = "full",
    cost_model: str = "deterministic",
    network_included: bool = True,
    network_topology: str | None = None,
    network_nodes_min: int | None = None,
    network_nodes_max: int | None = None,
    extra_provenance: dict | None = None,
    tags: list[str] | None = None,
) -> dict:
    """Build a metadata dict for a workflow."""
    stats = compute_graph_stats(task_graph)
    provenance = {**SAGA_PROVENANCE_BASE}
    if extra_provenance:
        provenance.update(extra_provenance)

    return {
        "id": wf_id,
        "name": name,
        "description": description,
        "domains": domains,
        "provenance": provenance,
        "license": {
            "source_license": "Non-commercial (USC/ANRG)",
            "dagbench_license": "Apache-2.0",
            "notes": "Task graph structure extracted from SAGA; SAGA itself is non-commercial licensed",
        },
        "completeness": completeness,
        "cost_model": cost_model,
        "network": {
            "included": network_included,
            "topology": network_topology,
            "num_nodes_min": network_nodes_min,
            "num_nodes_max": network_nodes_max,
        },
        "graph_stats": {
            "num_tasks": stats.num_tasks,
            "num_edges": stats.num_edges,
            "depth": stats.depth,
            "width": stats.width,
            "ccr": round(stats.ccr, 4) if stats.ccr is not None else None,
            "parallelism": round(stats.parallelism, 4) if stats.parallelism is not None else None,
        },
        "campaign": "campaign_001_initial_import",
        "tags": tags,
    }


def import_riotbench():
    """Import RIoTBench IoT sensor network workflows."""
    print("\n=== Importing RIoTBench IoT Workflows ===")

    # Generate one fog network to pair with all IoT workflows
    networks = get_fog_networks(
        num=1,
        num_edges_nodes=10,
        num_fog_nodes=3,
        num_cloud_nodes=2,
    )
    fog_net = networks[0]

    riotbench_workflows = [
        ("etl", get_etl_task_graphs, "ETL Pipeline",
         "Extract-Transform-Load pipeline for IoT sensor data. Tasks: source, parse, filter, interpolate, join, annotate, publish."),
        ("stats", get_stats_task_graphs, "Statistical Analytics",
         "Statistical computation pipeline for IoT sensor streams. Tasks: parse, average, kalman filter, regression, visualization."),
        ("train", get_train_task_graphs, "ML Training Pipeline",
         "Machine learning model training pipeline for IoT data. Tasks: read, annotate, train regression, write, publish."),
        ("predict", get_predict_task_graphs, "ML Prediction Pipeline",
         "Machine learning inference pipeline for IoT data. Tasks: subscribe, read model, predict, publish results."),
    ]

    riotbench_paper = {
        "paper_doi": "10.1002/cpe.4257",
        "paper_title": "RIoTBench: An IoT Benchmark for Distributed Stream Processing Systems",
        "authors": ["Anshu Shukla", "Shilpa Chaturvedi", "Yogesh Simmhan"],
        "year": 2017,
        "figure_or_table": "Fig. 2-5",
    }

    for short_name, gen_func, human_name, description in riotbench_workflows:
        task_graphs = gen_func(num=1)
        tg = task_graphs[0]

        wf_id = f"iot.riotbench_{short_name}"
        wf_dir = WORKFLOWS_DIR / "iot_sensor_networks" / f"riotbench_{short_name}"

        instance = ProblemInstance(name=wf_id, task_graph=tg, network=fog_net)

        metadata = _make_metadata(
            wf_id=wf_id,
            name=f"RIoTBench {human_name}",
            description=description,
            domains=["iot", "data-analytics"],
            task_graph=tg,
            network_included=True,
            network_topology="fog-three-tier",
            network_nodes_min=15,
            network_nodes_max=15,
            extra_provenance=riotbench_paper,
            tags=["riotbench", "iot", "streaming", short_name],
        )

        _save_workflow(wf_dir, instance, metadata)


def import_synthetic_patterns():
    """Import synthetic DAG patterns (diamond, chain, fork, branching)."""
    print("\n=== Importing Synthetic DAG Patterns ===")

    net = get_network(num_nodes=4)

    # Diamond
    tg = get_diamond_dag()
    wf_id = "synthetic.diamond"
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)
    metadata = _make_metadata(
        wf_id=wf_id,
        name="Diamond DAG",
        description="Classic 4-node diamond pattern: A -> (B, C) -> D. Minimal fork-join structure.",
        domains=["synthetic"],
        task_graph=tg,
        network_topology="fully-connected",
        network_nodes_min=4,
        network_nodes_max=4,
        tags=["diamond", "fork-join", "small"],
    )
    _save_workflow(WORKFLOWS_DIR / "synthetic" / "diamond", instance, metadata)

    # Chain (4 nodes)
    tg = get_chain_dag(num_nodes=4)
    wf_id = "synthetic.chain_4"
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)
    metadata = _make_metadata(
        wf_id=wf_id,
        name="Chain DAG (4 nodes)",
        description="Linear chain of 4 tasks: A -> B -> C -> D. No parallelism, purely sequential.",
        domains=["synthetic"],
        task_graph=tg,
        network_topology="fully-connected",
        network_nodes_min=4,
        network_nodes_max=4,
        tags=["chain", "sequential", "small"],
    )
    _save_workflow(WORKFLOWS_DIR / "synthetic" / "chain_4", instance, metadata)

    # Chain (8 nodes)
    tg = get_chain_dag(num_nodes=8)
    wf_id = "synthetic.chain_8"
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)
    metadata = _make_metadata(
        wf_id=wf_id,
        name="Chain DAG (8 nodes)",
        description="Linear chain of 8 tasks. No parallelism, purely sequential.",
        domains=["synthetic"],
        task_graph=tg,
        network_topology="fully-connected",
        network_nodes_min=4,
        network_nodes_max=4,
        tags=["chain", "sequential", "medium"],
    )
    _save_workflow(WORKFLOWS_DIR / "synthetic" / "chain_8", instance, metadata)

    # Fork
    tg = get_fork_dag()
    wf_id = "synthetic.fork"
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)
    metadata = _make_metadata(
        wf_id=wf_id,
        name="Fork-Join DAG",
        description="Fork-join pattern: A -> (B, C) -> (D, E) -> F. Two levels of parallelism.",
        domains=["synthetic"],
        task_graph=tg,
        network_topology="fully-connected",
        network_nodes_min=4,
        network_nodes_max=4,
        tags=["fork-join", "parallel", "small"],
    )
    _save_workflow(WORKFLOWS_DIR / "synthetic" / "fork", instance, metadata)

    # Branching (3 levels, branching factor 2)
    tg = get_branching_dag(levels=3, branching_factor=2)
    wf_id = "synthetic.branching_3x2"
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)
    metadata = _make_metadata(
        wf_id=wf_id,
        name="Branching DAG (3 levels, factor 2)",
        description="Tree-like DAG with 3 levels and branching factor 2, plus a synthetic sink node. Tests scheduling of tree-structured parallelism.",
        domains=["synthetic"],
        task_graph=tg,
        network_topology="fully-connected",
        network_nodes_min=4,
        network_nodes_max=4,
        tags=["branching", "tree", "parallel", "medium"],
    )
    _save_workflow(WORKFLOWS_DIR / "synthetic" / "branching_3x2", instance, metadata)

    # Branching (4 levels, branching factor 3)
    tg = get_branching_dag(levels=4, branching_factor=3)
    wf_id = "synthetic.branching_4x3"
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)
    metadata = _make_metadata(
        wf_id=wf_id,
        name="Branching DAG (4 levels, factor 3)",
        description="Tree-like DAG with 4 levels and branching factor 3, plus sink. Large synthetic benchmark for parallel scheduling.",
        domains=["synthetic"],
        task_graph=tg,
        network_topology="fully-connected",
        network_nodes_min=4,
        network_nodes_max=4,
        tags=["branching", "tree", "parallel", "large"],
    )
    _save_workflow(WORKFLOWS_DIR / "synthetic" / "branching_4x3", instance, metadata)


def main():
    print(f"DAGBench Import Script - {TODAY}")
    print(f"Workflows dir: {WORKFLOWS_DIR}")

    import_riotbench()
    import_synthetic_patterns()

    # Count results
    from dagbench.catalog import list_workflows
    workflows = list_workflows(workflows_dir=WORKFLOWS_DIR)
    print(f"\n=== Import Complete: {len(workflows)} workflows ===")
    for wf in workflows:
        print(f"  {wf.id:40s} {wf.graph_stats.num_tasks:4d} tasks  {wf.name}")


if __name__ == "__main__":
    main()
