"""Auto-discovers workflows and provides search/filter capabilities."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import yaml

from dagbench.schema import Domain, WorkflowMetadata


def _get_workflows_dir() -> Path:
    """Return the workflows/ directory path."""
    return Path(__file__).resolve().parent.parent.parent / "workflows"


def _discover_workflow_dirs(workflows_dir: Optional[Path] = None) -> List[Path]:
    """Find all directories containing a metadata.yaml file."""
    root = workflows_dir or _get_workflows_dir()
    if not root.exists():
        return []
    return sorted(
        d.parent for d in root.rglob("metadata.yaml")
    )


def get_workflow_path(workflow_id: str, workflows_dir: Optional[Path] = None) -> Optional[Path]:
    """Look up a workflow directory by its metadata ID.

    Args:
        workflow_id: The workflow's unique ID (e.g. 'iot.etl_pipeline')
        workflows_dir: Override the default workflows/ directory

    Returns:
        Path to the workflow directory, or None if not found.
    """
    for d in _discover_workflow_dirs(workflows_dir):
        meta_path = d / "metadata.yaml"
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f)
            if raw.get("id") == workflow_id:
                return d
        except Exception:
            continue
    return None


def list_workflows(workflows_dir: Optional[Path] = None) -> List[WorkflowMetadata]:
    """Return metadata for all discovered workflows."""
    results = []
    for d in _discover_workflow_dirs(workflows_dir):
        meta_path = d / "metadata.yaml"
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                raw = yaml.safe_load(f)
            results.append(WorkflowMetadata.model_validate(raw))
        except Exception:
            continue
    return results


def search(
    domain: Optional[str | Domain] = None,
    completeness: Optional[str] = None,
    cost_model: Optional[str] = None,
    min_tasks: Optional[int] = None,
    max_tasks: Optional[int] = None,
    tag: Optional[str] = None,
    workflows_dir: Optional[Path] = None,
) -> List[WorkflowMetadata]:
    """Search workflows by criteria.

    Args:
        domain: Filter by domain tag
        completeness: Filter by completeness level
        cost_model: Filter by cost model type
        min_tasks: Minimum number of tasks
        max_tasks: Maximum number of tasks
        tag: Filter by free-form tag
        workflows_dir: Override the default workflows/ directory

    Returns:
        List of matching WorkflowMetadata.
    """
    all_wf = list_workflows(workflows_dir)
    results = []

    for wf in all_wf:
        if domain is not None:
            domain_val = domain if isinstance(domain, str) else domain.value
            if not any(d.value == domain_val for d in wf.domains):
                continue
        if completeness is not None and wf.completeness.value != completeness:
            continue
        if cost_model is not None and wf.cost_model.value != cost_model:
            continue
        if min_tasks is not None and wf.graph_stats.num_tasks < min_tasks:
            continue
        if max_tasks is not None and wf.graph_stats.num_tasks > max_tasks:
            continue
        if tag is not None:
            if wf.tags is None or tag not in wf.tags:
                continue
        results.append(wf)

    return results
