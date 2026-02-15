"""DAGBench: The world's largest task graph benchmark repository.

Usage:
    import dagbench

    # List all available workflows
    dagbench.list_workflows()

    # Load a specific workflow as a SAGA ProblemInstance
    instance = dagbench.load_workflow("iot.riotbench_etl")

    # Search workflows by domain
    dagbench.search(domain="iot")
"""
from dagbench.catalog import list_workflows, search, get_workflow_path
from dagbench.loader import load_workflow, load_metadata
from dagbench.schema import (
    WorkflowMetadata,
    Domain,
    Completeness,
    CostModel,
    Provenance,
    GraphStats,
)

__version__ = "0.1.0"

__all__ = [
    "list_workflows",
    "search",
    "get_workflow_path",
    "load_workflow",
    "load_metadata",
    "WorkflowMetadata",
    "Domain",
    "Completeness",
    "CostModel",
    "Provenance",
    "GraphStats",
    "__version__",
]
