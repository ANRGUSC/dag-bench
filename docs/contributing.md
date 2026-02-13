# Contributing to DAGBench

## Adding a New Workflow

### 1. Create the workflow directory

```
workflows/<domain>/<workflow_name>/
  ├── graph.json        # SAGA ProblemInstance JSON (required)
  └── metadata.yaml     # DAGBench metadata (required)
```

Domain directories: `iot_sensor_networks/`, `edge_computing/`, `ml_pipelines/`, `scientific_workflows/`, `classic_benchmarks/`, `synthetic/`

### 2. Build the task graph

Use the DAGBuilder for manual extraction from papers:

```python
from dagbench.converters.manual import DAGBuilder
from dagbench.networks import homogeneous_network
from saga.schedulers.data import ProblemInstance

tg = (DAGBuilder("my_workflow")
    .task("A", 10.0)     # (name, computation_cost)
    .task("B", 20.0)
    .edge("A", "B", 5.0) # (source, target, communication_size)
    .build())

net = homogeneous_network(num_nodes=3, speed=1.0, bandwidth=100.0)
instance = ProblemInstance(name="domain.my_workflow", task_graph=tg, network=net)
```

Or use a converter for existing formats:
```python
from dagbench.converters import parse_stg, parse_dot, parse_edge_list_csv
```

### 3. Save graph.json

```python
import json

raw = json.loads(instance.model_dump_json(indent=2))
# Fix SAGA self-loop serialization quirk
for e in raw.get("network", {}).get("edges", []):
    if e.get("speed") is None:
        e["speed"] = 1e9

with open("graph.json", "w") as f:
    json.dump(raw, f, indent=2)
```

### 4. Create metadata.yaml

```yaml
id: domain.my_workflow
name: "Human-Readable Name"
description: "What this workflow represents and where it comes from."
domains:
  - iot          # Choose from: iot, edge-computing, fog-computing, ml-pipeline,
                 # scientific, genomics, astronomy, seismology, telecom, 5g,
                 # classic-benchmark, synthetic, image-processing, data-analytics,
                 # cyber-physical
provenance:
  source: "Short description of source"
  paper_title: "Full Paper Title"
  paper_doi: "10.xxxx/xxxxx"       # Optional
  paper_arxiv: "2403.07120"        # Optional
  authors: ["Alice", "Bob"]
  year: 2024
  figure_or_table: "Fig. 3"       # Which figure/table you extracted from
  repo_url: "https://..."         # Optional
  extraction_method: manual-figure # programmatic | manual-figure | manual-table |
                                   # trace-conversion | generated | existing-dataset
  extractor: "your-name"
  extraction_date: "2026-01-15"
license:
  source_license: "Apache-2.0"    # License of the original source
  dagbench_license: "Apache-2.0"
completeness: full                 # full | structure-and-costs | structure-only | partial
cost_model: deterministic          # deterministic | stochastic-independent | structure-only
network:
  included: true
  topology: fully-connected        # fog-three-tier | star | fully-connected | etc.
graph_stats:
  num_tasks: 10
  num_edges: 12
  depth: 4
  width: 3
  ccr: 1.5
  parallelism: 2.5
tags:
  - my-tag
  - another-tag
```

### 5. Validate

```bash
dagbench validate domain.my_workflow
```

### 6. Test scheduling

```python
import dagbench
from saga.schedulers.heft import HeftScheduler

instance = dagbench.load_workflow("domain.my_workflow")
schedule = HeftScheduler().schedule(instance.network, instance.task_graph)
print(f"Makespan: {schedule.makespan}")
```

## Computing Stats Automatically

```python
from dagbench.stats import compute_graph_stats
stats = compute_graph_stats(task_graph)
# Returns: num_tasks, num_edges, depth, width, ccr, parallelism
```

## Provenance Guidelines

- Always cite the original paper/source
- Specify which figure/table the DAG was extracted from
- If costs are estimated (not from the paper), note this in `quality_issues`
- Use `extraction_method: manual-figure` for hand-coded DAGs from papers
- Use `extraction_method: programmatic` for generated/imported DAGs
