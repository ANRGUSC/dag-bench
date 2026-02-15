"""Tests for dagbench.loader and catalog integration.

These tests create temp workflow directories and verify round-trip loading.
"""
import json
import tempfile
from pathlib import Path

import pytest
import yaml
from saga import TaskGraph, Network
from saga.schedulers.data import ProblemInstance

from dagbench.catalog import list_workflows, search, get_workflow_path
from dagbench.loader import load_workflow, load_metadata, load_workflow_from_path
from dagbench.schema import Domain


def _create_test_workflow(root: Path, wf_id: str = "test.diamond") -> Path:
    """Create a minimal valid workflow in a temp directory."""
    wf_dir = root / "test_domain" / "diamond"
    wf_dir.mkdir(parents=True, exist_ok=True)

    # Create a SAGA ProblemInstance
    tg = TaskGraph.create(
        tasks=[("A", 10.0), ("B", 20.0), ("C", 15.0), ("D", 10.0)],
        dependencies=[("A", "B", 5.0), ("A", "C", 3.0), ("B", "D", 4.0), ("C", "D", 2.0)],
    )
    net = Network.create(
        nodes=[("N0", 1.0), ("N1", 2.0)],
        edges=[("N0", "N1", 100.0)],
    )
    instance = ProblemInstance(name=wf_id, task_graph=tg, network=net)

    # Write graph.json
    graph_path = wf_dir / "graph.json"
    graph_path.write_text(instance.model_dump_json(indent=2), encoding="utf-8")

    # Write metadata.yaml
    metadata = {
        "id": wf_id,
        "name": "Test Diamond",
        "description": "A test diamond DAG",
        "domains": ["synthetic"],
        "provenance": {
            "source": "test",
            "extraction_method": "generated",
            "extractor": "test-harness",
            "extraction_date": "2025-01-01",
        },
        "license": {"source_license": "Apache-2.0"},
        "completeness": "full",
        "cost_model": "deterministic",
        "network": {"included": True, "topology": "fully-connected"},
        "graph_stats": {
            "num_tasks": 4,
            "num_edges": 4,
            "depth": 3,
            "width": 2,
            "ccr": 0.2545,
            "parallelism": 1.333,
        },
    }
    with open(wf_dir / "metadata.yaml", "w", encoding="utf-8") as f:
        yaml.dump(metadata, f, default_flow_style=False)

    return wf_dir


class TestLoadWorkflow:
    def test_round_trip(self, tmp_path):
        _create_test_workflow(tmp_path)
        instance = load_workflow("test.diamond", workflows_dir=tmp_path)
        assert instance.name == "test.diamond"
        assert len(instance.task_graph.tasks) == 4

    def test_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_workflow("nonexistent", workflows_dir=tmp_path)

    def test_load_from_path(self, tmp_path):
        wf_dir = _create_test_workflow(tmp_path)
        instance = load_workflow_from_path(wf_dir / "graph.json")
        assert instance.name == "test.diamond"


class TestLoadMetadata:
    def test_load(self, tmp_path):
        _create_test_workflow(tmp_path)
        meta = load_metadata("test.diamond", workflows_dir=tmp_path)
        assert meta.id == "test.diamond"
        assert meta.graph_stats.num_tasks == 4


class TestCatalog:
    def test_list_workflows(self, tmp_path):
        _create_test_workflow(tmp_path, "test.one")
        results = list_workflows(workflows_dir=tmp_path)
        assert len(results) == 1
        assert results[0].id == "test.one"

    def test_get_workflow_path(self, tmp_path):
        wf_dir = _create_test_workflow(tmp_path)
        found = get_workflow_path("test.diamond", workflows_dir=tmp_path)
        assert found == wf_dir

    def test_search_by_domain(self, tmp_path):
        _create_test_workflow(tmp_path)
        results = search(domain="synthetic", workflows_dir=tmp_path)
        assert len(results) == 1

    def test_search_no_match(self, tmp_path):
        _create_test_workflow(tmp_path)
        results = search(domain="iot", workflows_dir=tmp_path)
        assert len(results) == 0

    def test_search_by_task_count(self, tmp_path):
        _create_test_workflow(tmp_path)
        results = search(min_tasks=3, max_tasks=5, workflows_dir=tmp_path)
        assert len(results) == 1
        results = search(min_tasks=10, workflows_dir=tmp_path)
        assert len(results) == 0
