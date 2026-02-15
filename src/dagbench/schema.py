"""Pydantic metadata models for DAGBench workflow entries.

Every workflow in the repository has a metadata.yaml sidecar file
that conforms to the WorkflowMetadata schema defined here.
"""
from __future__ import annotations

from datetime import date
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class Domain(str, Enum):
    """Application domain tags for workflows."""
    IOT = "iot"
    EDGE_COMPUTING = "edge-computing"
    FOG_COMPUTING = "fog-computing"
    ML_PIPELINE = "ml-pipeline"
    SCIENTIFIC = "scientific"
    GENOMICS = "genomics"
    ASTRONOMY = "astronomy"
    SEISMOLOGY = "seismology"
    TELECOM = "telecom"
    FIVE_G = "5g"
    CLASSIC_BENCHMARK = "classic-benchmark"
    SYNTHETIC = "synthetic"
    IMAGE_PROCESSING = "image-processing"
    DATA_ANALYTICS = "data-analytics"
    CYBER_PHYSICAL = "cyber-physical"
    WIRELESS = "wireless"
    MANET = "manet"
    TACTICAL = "tactical"
    V2X = "v2x"
    UAV = "uav"
    MEC = "mec"
    AGRICULTURE_IOT = "agriculture-iot"
    INDUSTRIAL_IOT = "industrial-iot"
    SMART_CITY = "smart-city"
    NFV = "nfv"


class Completeness(str, Enum):
    """How complete the cost/timing information is."""
    FULL = "full"
    STRUCTURE_AND_COSTS = "structure-and-costs"
    STRUCTURE_ONLY = "structure-only"
    PARTIAL = "partial"


class CostModel(str, Enum):
    """Type of cost model used."""
    DETERMINISTIC = "deterministic"
    STOCHASTIC_INDEPENDENT = "stochastic-independent"
    STOCHASTIC_JOINT = "stochastic-joint"
    STRUCTURE_ONLY = "structure-only"
    PARAMETERIZED = "parameterized"


class ExtractionMethod(str, Enum):
    """How the DAG was obtained."""
    PROGRAMMATIC = "programmatic"
    MANUAL_FIGURE = "manual-figure"
    MANUAL_TABLE = "manual-table"
    TRACE_CONVERSION = "trace-conversion"
    GENERATED = "generated"
    EXISTING_DATASET = "existing-dataset"
    AI_GENERATED = "ai-generated"


class NetworkTopology(str, Enum):
    """Type of network topology."""
    FOG_THREE_TIER = "fog-three-tier"
    STAR = "star"
    FULLY_CONNECTED = "fully-connected"
    HIERARCHICAL = "hierarchical"
    HOMOGENEOUS = "homogeneous"
    HETEROGENEOUS = "heterogeneous"
    CUSTOM = "custom"
    MANET = "manet"
    MEC = "mec"


class Provenance(BaseModel):
    """Tracks where a workflow came from."""
    source: str = Field(description="Short description of the source (e.g. 'SAGA RIoTBench module')")
    paper_doi: Optional[str] = Field(default=None, description="DOI of the source paper")
    paper_arxiv: Optional[str] = Field(default=None, description="arXiv ID (e.g. '2403.07120')")
    paper_title: Optional[str] = Field(default=None, description="Title of the source paper")
    authors: Optional[List[str]] = Field(default=None, description="Author list")
    year: Optional[int] = Field(default=None, description="Publication year")
    figure_or_table: Optional[str] = Field(default=None, description="Which figure/table the DAG was extracted from")
    repo_url: Optional[str] = Field(default=None, description="URL to source repository")
    extraction_method: ExtractionMethod = Field(description="How the DAG was extracted/created")
    extractor: str = Field(description="Who/what extracted this (e.g. 'dagbench-import-script')")
    extraction_date: date = Field(description="When the extraction was performed")
    notes: Optional[str] = Field(default=None, description="Additional context about provenance (e.g. how AI-generated DAGs relate to cited work)")


class LicenseInfo(BaseModel):
    """License information for a workflow."""
    source_license: str = Field(description="License of the original source")
    dagbench_license: str = Field(default="Apache-2.0", description="License for dagbench's representation")
    notes: Optional[str] = Field(default=None, description="Any licensing caveats")


class NetworkInfo(BaseModel):
    """Information about the network topology included with the workflow."""
    included: bool = Field(description="Whether a network definition is included")
    topology: Optional[NetworkTopology] = Field(default=None, description="Type of network topology")
    num_nodes_min: Optional[int] = Field(default=None, ge=0, description="Minimum number of network nodes")
    num_nodes_max: Optional[int] = Field(default=None, ge=0, description="Maximum number of network nodes")


class GraphStats(BaseModel):
    """Auto-computed graph statistics."""
    num_tasks: int = Field(ge=0, description="Number of tasks (nodes)")
    num_edges: int = Field(ge=0, description="Number of dependencies (edges)")
    depth: int = Field(ge=0, description="Length of longest path (critical path depth)")
    width: int = Field(ge=0, description="Maximum number of tasks at any level")
    ccr: Optional[float] = Field(default=None, ge=0, description="Communication-to-Computation Ratio")
    parallelism: Optional[float] = Field(default=None, ge=0, description="Average parallelism (num_tasks / depth)")


class WorkflowMetadata(BaseModel):
    """Complete metadata for a DAGBench workflow entry."""
    id: str = Field(description="Unique identifier (e.g. 'iot.riotbench_etl')")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="Description of what this workflow represents")
    domains: List[Domain] = Field(description="Application domain tags")
    provenance: Provenance = Field(description="Source and extraction information")
    license: LicenseInfo = Field(description="License information")
    completeness: Completeness = Field(description="How complete the cost information is")
    cost_model: CostModel = Field(description="Type of cost model")
    network: NetworkInfo = Field(description="Network topology information")
    graph_stats: GraphStats = Field(description="Auto-computed graph statistics")
    campaign: Optional[str] = Field(default=None, description="Collection campaign that produced this entry")
    quality_issues: Optional[List[str]] = Field(default=None, description="Known quality issues")
    tags: Optional[List[str]] = Field(default=None, description="Additional free-form tags")
