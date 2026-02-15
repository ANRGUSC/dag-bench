"""Load workflows as SAGA ProblemInstance objects."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import yaml
from saga.schedulers.data import ProblemInstance

from dagbench.catalog import get_workflow_path
from dagbench.schema import WorkflowMetadata


def _fix_saga_json(data: str) -> str:
    """Fix SAGA serialization quirks before deserializing.

    SAGA's Network adds self-loop edges (source==target) with null speed.
    These fail Pydantic validation on re-read. Replace null with a large
    value (1e9) representing "local communication is essentially free".
    """
    obj = json.loads(data)
    if "network" in obj and "edges" in obj["network"]:
        for e in obj["network"]["edges"]:
            if e.get("speed") is None:
                e["speed"] = 1e9  # Local (same-node) transfer is near-instant
    return json.dumps(obj)


def load_metadata(
    workflow_id: str,
    workflows_dir: Optional[Path] = None,
) -> WorkflowMetadata:
    """Load metadata for a workflow by ID.

    Args:
        workflow_id: The workflow's unique ID (e.g. 'iot.riotbench_etl')
        workflows_dir: Override the default workflows/ directory

    Returns:
        WorkflowMetadata parsed from metadata.yaml

    Raises:
        FileNotFoundError: If workflow not found
    """
    wf_dir = get_workflow_path(workflow_id, workflows_dir)
    if wf_dir is None:
        raise FileNotFoundError(f"Workflow '{workflow_id}' not found")

    meta_path = wf_dir / "metadata.yaml"
    with open(meta_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return WorkflowMetadata.model_validate(raw)


def load_workflow(
    workflow_id: str,
    workflows_dir: Optional[Path] = None,
) -> ProblemInstance:
    """Load a workflow as a SAGA ProblemInstance.

    Args:
        workflow_id: The workflow's unique ID (e.g. 'iot.riotbench_etl')
        workflows_dir: Override the default workflows/ directory

    Returns:
        SAGA ProblemInstance ready for scheduling

    Raises:
        FileNotFoundError: If workflow not found
    """
    wf_dir = get_workflow_path(workflow_id, workflows_dir)
    if wf_dir is None:
        raise FileNotFoundError(f"Workflow '{workflow_id}' not found")

    graph_path = wf_dir / "graph.json"
    if not graph_path.exists():
        raise FileNotFoundError(f"graph.json not found in {wf_dir}")

    with open(graph_path, "r", encoding="utf-8") as f:
        data = f.read()

    return ProblemInstance.model_validate_json(_fix_saga_json(data))


def load_workflow_from_path(graph_json_path: Path) -> ProblemInstance:
    """Load a ProblemInstance directly from a graph.json file path.

    Args:
        graph_json_path: Path to a graph.json file

    Returns:
        SAGA ProblemInstance
    """
    with open(graph_json_path, "r", encoding="utf-8") as f:
        data = f.read()
    return ProblemInstance.model_validate_json(_fix_saga_json(data))
