"""Validation utilities for DAGBench workflow entries."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import networkx as nx
import yaml
from pydantic import ValidationError

from dagbench.schema import WorkflowMetadata


class ValidationResult:
    """Collects validation errors and warnings."""

    def __init__(self, workflow_path: Path):
        self.workflow_path = workflow_path
        self.errors: List[str] = []
        self.warnings: List[str] = []

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def __repr__(self) -> str:
        status = "PASS" if self.ok else "FAIL"
        return f"ValidationResult({self.workflow_path.name}: {status}, {len(self.errors)} errors, {len(self.warnings)} warnings)"


def validate_workflow(workflow_dir: Path) -> ValidationResult:
    """Run all validation checks on a workflow directory.

    Checks:
    - metadata.yaml exists and conforms to schema
    - graph.json exists and is valid JSON
    - graph.json represents a valid DAG (acyclic)
    - metadata stats match actual graph
    """
    result = ValidationResult(workflow_dir)

    # Check metadata.yaml
    metadata_path = workflow_dir / "metadata.yaml"
    if not metadata_path.exists():
        result.error("metadata.yaml not found")
        return result

    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        metadata = WorkflowMetadata.model_validate(raw)
    except yaml.YAMLError as e:
        result.error(f"metadata.yaml is not valid YAML: {e}")
        return result
    except ValidationError as e:
        result.error(f"metadata.yaml does not conform to schema: {e}")
        return result

    # Check graph.json
    graph_path = workflow_dir / "graph.json"
    if not graph_path.exists():
        result.error("graph.json not found")
        return result

    try:
        with open(graph_path, "r", encoding="utf-8") as f:
            graph_data = json.load(f)
    except json.JSONDecodeError as e:
        result.error(f"graph.json is not valid JSON: {e}")
        return result

    # Validate graph structure
    if "task_graph" not in graph_data:
        result.error("graph.json missing 'task_graph' field")
        return result

    tg = graph_data["task_graph"]
    tasks = {t["name"] for t in tg.get("tasks", [])}
    deps = tg.get("dependencies", [])

    # Build networkx DAG and check acyclicity
    G = nx.DiGraph()
    G.add_nodes_from(tasks)
    for dep in deps:
        src, tgt = dep["source"], dep["target"]
        if src not in tasks:
            result.error(f"Dependency source '{src}' not in tasks")
        if tgt not in tasks:
            result.error(f"Dependency target '{tgt}' not in tasks")
        G.add_edge(src, tgt)

    if not nx.is_directed_acyclic_graph(G):
        result.error("Graph contains cycles - not a valid DAG")

    # Cross-check stats
    actual_tasks = len(tasks)
    actual_edges = len(deps)
    if metadata.graph_stats.num_tasks != actual_tasks:
        result.warn(
            f"Metadata num_tasks={metadata.graph_stats.num_tasks} but graph has {actual_tasks}"
        )
    if metadata.graph_stats.num_edges != actual_edges:
        result.warn(
            f"Metadata num_edges={metadata.graph_stats.num_edges} but graph has {actual_edges}"
        )

    # Check network if claimed to be included
    if metadata.network.included:
        if "network" not in graph_data:
            result.error("Metadata says network is included but graph.json has no 'network' field")

    # Provenance checks
    prov = metadata.provenance
    if not prov.source:
        result.warn("Provenance source is empty")

    return result
