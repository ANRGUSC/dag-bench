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

## Finding Papers with Extractable DAGs

The hardest part of adding real-world workflows is finding papers that contain formal, extractable task graphs. Most scheduling papers use abstract DAGs (task_1, task_2...), and most systems papers describe pipelines only in prose. Here's what works.

### What Makes a Paper Extractable

A paper must have ALL of:
1. **Named application-level tasks** (e.g., "ObjectDetection", "FeatureExtract" -- not "task i")
2. **Explicit dependency structure** as a DAG figure or formal definition
3. **Edge/IoT/MEC deployment context**

Bonus (makes the workflow much more valuable):
- Per-task computation costs profiled on real hardware (Jetson, RPi, etc.)
- Data transfer sizes between tasks
- Multi-device profiling (enables heterogeneous network construction)

### Search Queries That Work

**Best queries** (these actually produced results):
- `"inference pipeline" edge computing scheduling DAG`
- `"video analytics" pipeline edge "task graph"`
- `"pipeline parallel" edge computing application`
- `"real application" DAG scheduling edge heterogeneous`
- `site:mdpi.com "pipeline" "edge computing" "task graph"` (open-access papers)

**Best venues** (ranked by hit rate):
- IEEE MILCOM, ACM HPDC, IEEE TPDS, ACM SEC
- MDPI Sensors/Computers/Electronics (open access, easy to verify)
- Elsevier JNCA (networking applications)
- arXiv cs.DC + cs.NI

**Best domains** for finding real pipelines:
- Video analytics (object detection -> classification -> tracking)
- Precision agriculture (sensing -> detection -> analysis)
- Face analysis (pipeline-parallel decomposition)
- Multi-model ML inference (natural DAG branching)

### Green Flags

- Figure captions: "Application model", "Task dependency graph", "Inference pipeline"
- Tables with "Model / Device / Latency (ms)" -- real per-task costs
- Papers evaluating on "N real applications" (N > 1 means multiple extractable DAGs)
- Pipeline-parallel decomposition papers
- Resilience/fault-tolerance papers (must describe pipeline structure explicitly)

### Red Flags

- "We model the application as a DAG" but only random graphs in evaluation
- DNN layer graphs (neural network architecture != application pipeline)
- Survey papers (describe patterns, no formal extractable DAGs)
- iFogSim built-in applications (2-4 tasks, too simple)

### Extraction Workflow

1. **Download and read the PDF locally** (WebFetch can't parse most academic PDFs)
2. **Verify the figure**: Are tasks named? Are dependencies explicit? Is there branching?
3. **Extract tasks and edges** from the figure, costs from tables
4. **Build the network**: Use device speedup ratios from the paper if available
5. **Create workflow files**: graph.json (via DAGBuilder), metadata.yaml with honest provenance
6. **Validate**: `dagbench validate`, HEFT scheduling, check stats match metadata
7. **Document honestly**: Note what's directly from the paper vs. estimated in the `notes` field

See `docs/sources.md` for the full list of successfully extracted papers and their provenance details.

## Provenance Guidelines

- Always cite the original paper/source
- Specify which figure/table the DAG was extracted from
- If costs are estimated (not from the paper), note this in the `notes` field
- Use `extraction_method: manual-figure` for hand-coded DAGs from papers
- Use `extraction_method: programmatic` for generated/imported DAGs
- The `notes` field is critical: explain what's from the paper vs. what's estimated
- Avoid Unicode characters (arrows, em dashes) in description fields -- Windows cp1252 encoding breaks CLI output
