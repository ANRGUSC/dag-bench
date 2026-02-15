"""Tests for dagbench.validate module."""
import json
import tempfile
from pathlib import Path

import yaml
from saga import TaskGraph, Network
from saga.schedulers.data import ProblemInstance

from dagbench.validate import validate_workflow


def _create_valid_workflow(root: Path) -> Path:
    """Create a minimal valid workflow directory."""
    wf_dir = root / "test_wf"
    wf_dir.mkdir(parents=True, exist_ok=True)

    tg = TaskGraph.create(
        tasks=[("A", 10.0), ("B", 20.0)],
        dependencies=[("A", "B", 5.0)],
    )
    net = Network.create(
        nodes=[("N0", 1.0), ("N1", 2.0)],
        edges=[("N0", "N1", 100.0)],
    )
    instance = ProblemInstance(name="test", task_graph=tg, network=net)

    (wf_dir / "graph.json").write_text(instance.model_dump_json(indent=2), encoding="utf-8")

    metadata = {
        "id": "test.workflow",
        "name": "Test",
        "description": "Test workflow",
        "domains": ["synthetic"],
        "provenance": {
            "source": "test",
            "extraction_method": "generated",
            "extractor": "test",
            "extraction_date": "2025-01-01",
        },
        "license": {"source_license": "Apache-2.0"},
        "completeness": "full",
        "cost_model": "deterministic",
        "network": {"included": True, "topology": "fully-connected"},
        "graph_stats": {"num_tasks": 2, "num_edges": 1, "depth": 2, "width": 1, "ccr": 0.1667, "parallelism": 1.0},
    }
    with open(wf_dir / "metadata.yaml", "w", encoding="utf-8") as f:
        yaml.dump(metadata, f)

    return wf_dir


class TestValidateWorkflow:
    def test_valid(self, tmp_path):
        wf_dir = _create_valid_workflow(tmp_path)
        result = validate_workflow(wf_dir)
        assert result.ok, f"Errors: {result.errors}"

    def test_missing_metadata(self, tmp_path):
        wf_dir = tmp_path / "empty_wf"
        wf_dir.mkdir()
        result = validate_workflow(wf_dir)
        assert not result.ok
        assert "metadata.yaml not found" in result.errors[0]

    def test_missing_graph(self, tmp_path):
        wf_dir = _create_valid_workflow(tmp_path)
        (wf_dir / "graph.json").unlink()
        result = validate_workflow(wf_dir)
        assert not result.ok
        assert "graph.json not found" in result.errors[0]

    def test_invalid_metadata_schema(self, tmp_path):
        wf_dir = _create_valid_workflow(tmp_path)
        with open(wf_dir / "metadata.yaml", "w") as f:
            yaml.dump({"id": "test"}, f)  # Missing required fields
        result = validate_workflow(wf_dir)
        assert not result.ok

    def test_stats_mismatch_error(self, tmp_path):
        wf_dir = _create_valid_workflow(tmp_path)
        # Update metadata with wrong stats
        with open(wf_dir / "metadata.yaml", "r") as f:
            meta = yaml.safe_load(f)
        meta["graph_stats"]["num_tasks"] = 999
        with open(wf_dir / "metadata.yaml", "w") as f:
            yaml.dump(meta, f)
        result = validate_workflow(wf_dir)
        assert not result.ok
        assert any("num_tasks" in e for e in result.errors)
