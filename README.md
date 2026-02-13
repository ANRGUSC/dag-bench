# DAGBench

**A curated task graph benchmark for scheduling research — 82 DAGs across 25 application domains with honest provenance.**

DAGBench provides 82 diverse, well-characterized task graphs (DAGs) from 25 application domains, all in a uniform SAGA-compatible format with full provenance tracking. Every workflow has verified provenance — either genuinely sourced from code repositories, algorithmically generated, or clearly labeled as AI-generated with honest attribution. Load any workflow in one line and schedule it with any of SAGA's 17 algorithms.

## Installation

```bash
pip install dagbench
```

> **Note:** DAGBench depends on [SAGA](https://github.com/anrgusc/saga) (anrg-saga), which has a non-commercial license. DAGBench itself is Apache-2.0.

## Quick Start

```python
import dagbench

# List all available workflows
for wf in dagbench.list_workflows():
    print(f"{wf.id}: {wf.graph_stats.num_tasks} tasks, depth={wf.graph_stats.depth}")

# Load a workflow as a SAGA ProblemInstance
instance = dagbench.load_workflow("classic.gauss_elim_5")

# Schedule it with HEFT
from saga.schedulers.heft import HeftScheduler
schedule = HeftScheduler().schedule(instance.network, instance.task_graph)
print(f"Makespan: {schedule.makespan:.2f}")

# Search workflows by domain
iot_workflows = dagbench.search(domain="iot")

# Get detailed metadata
meta = dagbench.load_metadata("edge.face_recognition")
print(f"Source: {meta.provenance.source}")
print(f"Method: {meta.provenance.extraction_method}")
```

## CLI

```bash
# List all workflows
dagbench list

# Filter by domain
dagbench list --domain iot

# Show detailed info about a workflow
dagbench info classic.cholesky_4

# Validate all workflows
dagbench validate

# Aggregate statistics
dagbench stats

# Generate HTML documentation
dagbench docs                    # all workflows
dagbench docs classic.fft_8     # single workflow
```

## Provenance Policy

Every workflow in DAGBench has honest, verified provenance. We use three extraction methods:

| Method | Count | Description |
|--------|-------|-------------|
| `programmatic` | 23 | Genuinely sourced from code repositories (RIoTBench from SAGA) or algorithmically generated (classic benchmarks) |
| `generated` | 5 | Algorithmically generated synthetic patterns (fork-join, stencil, etc.) |
| `ai-generated` | 54 | AI-generated task graphs inspired by domain literature. Each cites its inspiration source with honest notes explaining the relationship |

Workflows labeled `ai-generated` are NOT extracted from paper figures. They are synthetic structures designed to represent typical patterns in their respective domains. Where papers are cited, the `notes` field explains exactly what the paper contains and how the workflow relates to it. No DOI has been fabricated — all cited DOIs have been verified.

Each workflow also includes an HTML documentation page (`docs.html`) with a Mermaid.js DAG visualization, per-task descriptions, and full provenance details.

## Workflow Catalog

### By Domain (82 workflows)

| Domain | Count | Examples |
|--------|-------|---------|
| Classic Benchmarks | 13 | Gaussian elimination, FFT butterfly, LU decomposition, Cholesky, MapReduce |
| Synthetic Patterns | 11 | Diamond, chain, fork-join, branching, stencil, reduction tree, wide parallel |
| IoT / Sensor Networks | 6 | RIoTBench (ETL, stats, train, predict), smart grid, predictive maintenance |
| Scientific Workflows | 6 | Montage, Epigenomics, Seismology, BLAST, SoyKB, BWA alignment |
| Fog Computing | 6 | Three-tier offload, latency-critical, federated fog, healthcare, content distribution, smart building |
| Edge Computing | 6 | Face recognition, AR pipeline, autonomous driving, smart home, video transcoding, SFC |
| Industrial IoT | 5 | Robotic assembly, CNC monitoring, process control, digital twin, energy monitoring |
| Smart City | 5 | Traffic management, emergency response, waste, air quality, water distribution |
| Agriculture IoT | 4 | Precision irrigation, livestock monitoring, greenhouse control, crop disease |
| V2X / Vehicular | 4 | Cooperative perception, platooning, intersection management, collision avoidance |
| ML Pipelines | 4 | CNN inference, federated learning, NLP pipeline, ETL warehouse |
| UAV / Drone | 3 | Search & rescue, precision agriculture, fleet delivery |
| Tactical / MANET | 3 | ISR pipeline, situational awareness, mesh routing |
| Telecom / 5G | 3 | O-RAN uplink, network slicing, 5G SFC |
| MEC Offloading | 2 | Face detection offloading, video analytics |
| NFV | 2 | CDN edge cache, 5G UE attach |

### By Size

| Range | Count | Examples |
|-------|-------|---------|
| 1-10 tasks | 18 | Diamond (4), chain (4-8), face recognition (7), SFC (8) |
| 11-15 tasks | 38 | CNC monitoring (11), crop disease (11), smart grid (10), etc. |
| 16-20 tasks | 13 | Robotic assembly (16), BLAST-like (19), search rescue (20) |
| 21-40 tasks | 9 | MapReduce (27), Cholesky-5 (35), delivery fleet (24) |
| 41-64 tasks | 3 | Branching-4x3 (41), Gauss-10 (55), Cholesky-6 (56) |
| 65+ tasks | 1 | FFT-32 (144) |

### Complete Workflow List

| ID | Tasks | Edges | Depth | Width | Name |
|----|-------|-------|-------|-------|------|
| agri.crop_disease | 11 | 12 | 7 | 3 | Crop Disease Detection |
| agri.greenhouse_control | 12 | 14 | 5 | 5 | Greenhouse Climate Control |
| agri.livestock_monitoring | 14 | 13 | 3 | 10 | Livestock Monitoring |
| agri.precision_irrigation | 14 | 13 | 5 | 9 | Precision Irrigation |
| classic.cholesky_4 | 20 | 26 | 10 | 6 | Cholesky Factorization (4x4 tiles) |
| classic.cholesky_5 | 35 | 50 | 13 | 10 | Cholesky Factorization (5x5 tiles) |
| classic.cholesky_6 | 56 | 85 | 16 | 15 | Cholesky Factorization (6x6 tiles) |
| classic.fft_8 | 28 | 32 | 5 | 8 | FFT Butterfly (8-point) |
| classic.fft_16 | 64 | 80 | 6 | 16 | FFT Butterfly (16-point) |
| classic.fft_32 | 144 | 192 | 7 | 32 | FFT Butterfly (32-point) |
| classic.gauss_elim_5 | 15 | 30 | 9 | 4 | Gaussian Elimination (n=5) |
| classic.gauss_elim_7 | 28 | 63 | 13 | 6 | Gaussian Elimination (n=7) |
| classic.gauss_elim_10 | 55 | 135 | 19 | 9 | Gaussian Elimination (n=10) |
| classic.lu_decomp_4 | 30 | 49 | 10 | 9 | LU Decomposition (4x4 tiles) |
| classic.mapreduce_4m_2r | 9 | 12 | 5 | 4 | MapReduce (4 mappers, 2 reducers) |
| classic.mapreduce_8m_4r | 15 | 24 | 5 | 8 | MapReduce (8 mappers, 4 reducers) |
| classic.mapreduce_16m_8r | 27 | 48 | 5 | 16 | MapReduce (16 mappers, 8 reducers) |
| edge.ar_pipeline | 8 | 8 | 6 | 2 | AR Object Detection Pipeline |
| edge.autonomous_driving | 11 | 10 | 7 | 3 | Autonomous Driving Pipeline |
| edge.face_recognition | 7 | 7 | 6 | 2 | Face Recognition Pipeline |
| edge.smart_home | 12 | 13 | 5 | 4 | Smart Home IoT Pipeline |
| edge.video_transcoding | 14 | 16 | 5 | 4 | Video Transcoding Pipeline |
| fog.content_distribution | 10 | 14 | 5 | 4 | Fog Content Distribution |
| fog.federated_fog | 23 | 26 | 7 | 5 | Federated Fog Learning |
| fog.healthcare_fog | 17 | 16 | 5 | 9 | Fog Healthcare Monitoring |
| fog.latency_critical | 8 | 7 | 7 | 2 | Fog Latency-Critical Processing |
| fog.smart_building | 21 | 22 | 5 | 12 | Fog Smart Building Management |
| fog.three_tier_offload | 12 | 11 | 12 | 1 | Fog Three-Tier Offloading |
| iiot.cnc_monitoring | 11 | 12 | 5 | 4 | IIoT CNC Machine Monitoring |
| iiot.digital_twin | 13 | 14 | 7 | 5 | IIoT Digital Twin Synchronization |
| iiot.energy_monitoring | 13 | 14 | 4 | 8 | IIoT Factory Energy Monitoring |
| iiot.process_control | 12 | 13 | 5 | 5 | IIoT Chemical Process Control |
| iiot.robotic_assembly | 16 | 17 | 10 | 3 | IIoT Robotic Assembly Coordination |
| iot.predictive_maintenance | 13 | 15 | 6 | 5 | Industrial Predictive Maintenance |
| iot.riotbench_etl | 11 | 11 | 10 | 2 | RIoTBench ETL Pipeline |
| iot.riotbench_predict | 11 | 14 | 7 | 3 | RIoTBench ML Prediction Pipeline |
| iot.riotbench_stats | 9 | 10 | 7 | 3 | RIoTBench Statistical Analytics |
| iot.riotbench_train | 10 | 11 | 8 | 2 | RIoTBench ML Training Pipeline |
| iot.smart_grid | 10 | 11 | 5 | 4 | Smart Grid Energy Management |
| mec.face_detection_offload | 9 | 9 | 7 | 2 | MEC Face Detection Offloading |
| mec.video_analytics | 13 | 14 | 8 | 3 | MEC Video Analytics Pipeline |
| ml.cnn_inference | 13 | 12 | 13 | 1 | CNN Inference Pipeline |
| ml.etl_warehouse | 12 | 12 | 8 | 4 | ETL Data Warehouse Pipeline |
| ml.federated_learning | 19 | 22 | 7 | 5 | Federated Learning Round (5 clients) |
| ml.nlp_pipeline | 9 | 11 | 5 | 4 | NLP Text Analysis Pipeline |
| nfv.cdn_edge_cache | 10 | 10 | 9 | 2 | NFV CDN Edge Caching |
| nfv.ue_attach | 12 | 15 | 8 | 4 | 5G UE Attach Procedure |
| scientific.blast_like | 19 | 22 | 7 | 5 | BLAST-like Sequence Search Workflow |
| scientific.bwa_like | 13 | 15 | 7 | 4 | BWA-like Alignment Workflow |
| scientific.epigenomics_like | 19 | 21 | 7 | 4 | Epigenomics-like Genome Workflow |
| scientific.montage_like | 19 | 29 | 7 | 6 | Montage-like Astronomy Workflow |
| scientific.seismology_like | 24 | 31 | 6 | 9 | Seismology-like Hazard Workflow |
| scientific.soykb_like | 19 | 20 | 9 | 3 | SoyKB-like Genome Pipeline |
| smart_city.air_quality | 15 | 15 | 5 | 5 | Air Quality Monitoring |
| smart_city.emergency_response | 12 | 16 | 6 | 3 | Emergency Response Coordination |
| smart_city.traffic_mgmt | 17 | 16 | 6 | 6 | Smart City Traffic Management |
| smart_city.waste_mgmt | 14 | 13 | 7 | 8 | IoT Waste Management |
| smart_city.water_distribution | 16 | 15 | 4 | 6 | Water Distribution Network |
| synthetic.branching_3x2 | 8 | 10 | 4 | 4 | Branching DAG (3 levels, factor 2) |
| synthetic.branching_4x3 | 41 | 66 | 5 | 27 | Branching DAG (4 levels, factor 3) |
| synthetic.chain_4 | 4 | 3 | 4 | 1 | Chain DAG (4 nodes) |
| synthetic.chain_8 | 8 | 7 | 8 | 1 | Chain DAG (8 nodes) |
| synthetic.diamond | 4 | 4 | 3 | 2 | Diamond DAG |
| synthetic.fork | 6 | 6 | 4 | 2 | Fork-Join DAG |
| synthetic.multi_fork_join | 14 | 19 | 8 | 4 | Multi-Level Fork-Join DAG |
| synthetic.pipeline_stages | 10 | 17 | 6 | 3 | Multi-Stage Pipeline DAG |
| synthetic.reduction_tree | 15 | 14 | 4 | 8 | Binary Reduction Tree (8 leaves) |
| synthetic.stencil_3x4 | 12 | 17 | 6 | 3 | Stencil Computation (3x4 grid) |
| synthetic.wide_parallel_20 | 22 | 40 | 3 | 20 | Wide Parallel DAG (20 workers) |
| tactical.isr_pipeline | 13 | 13 | 8 | 3 | Tactical ISR Processing |
| tactical.mesh_routing | 10 | 12 | 7 | 4 | MANET Mesh Routing Protocol |
| tactical.situational_awareness | 15 | 16 | 6 | 4 | Tactical COP Situational Awareness |
| telecom.network_slicing | 10 | 12 | 7 | 3 | 5G Network Slicing Orchestration |
| telecom.oran_uplink | 11 | 10 | 11 | 1 | O-RAN Uplink Processing Pipeline |
| telecom.sfc_5g | 8 | 8 | 7 | 2 | 5G Service Function Chain |
| uav.delivery_fleet | 24 | 27 | 8 | 5 | UAV Fleet Delivery Orchestration |
| uav.precision_agriculture | 13 | 16 | 12 | 2 | UAV Precision Agriculture Survey |
| uav.search_rescue | 20 | 21 | 10 | 3 | UAV Search and Rescue Coordination |
| v2x.collision_avoidance | 10 | 9 | 6 | 3 | V2X Collision Avoidance System |
| v2x.cooperative_perception | 15 | 14 | 8 | 6 | V2X Cooperative Perception |
| v2x.intersection_mgmt | 15 | 18 | 8 | 4 | V2X Intersection Management |
| v2x.platooning_control | 13 | 14 | 7 | 3 | V2X Truck Platooning Control |

## Data Format

Each workflow consists of:
- **`graph.json`** — SAGA `ProblemInstance` (task graph + network topology)
- **`metadata.yaml`** — Full provenance, license, statistics, domain tags
- **`docs.html`** — Self-contained HTML documentation with DAG visualization

```python
# graph.json structure
{
  "name": "iot.riotbench_etl",
  "task_graph": {
    "tasks": [{"name": "Source", "cost": 32.5}, ...],
    "dependencies": [{"source": "Source", "target": "Parse", "size": 1024.0}, ...]
  },
  "network": {
    "nodes": [{"name": "Edge0", "speed": 1.0}, ...],
    "edges": [{"source": "Edge0", "target": "Fog0", "speed": 7500.0}, ...]
  }
}
```

## Converters

Import DAGs from external formats:

```python
from dagbench.converters import parse_stg, parse_dot, parse_edge_list_csv, DAGBuilder

# STG format (Standard Task Graph Set)
tg = parse_stg(stg_text)

# DOT format (Graphviz)
tg = parse_dot(dot_text)

# CSV edge list
tg = parse_edge_list_csv(csv_text)

# Fluent builder for paper figures
tg = (DAGBuilder("my_dag")
    .task("A", 10.0).task("B", 20.0).task("C", 15.0)
    .edge("A", "B", 5.0).edge("A", "C", 3.0)
    .build())

# Classic benchmark generators
from dagbench.converters import gaussian_elimination_dag, fft_dag, cholesky_dag
tg = gaussian_elimination_dag(n=10)
tg = fft_dag(num_points=64)
tg = cholesky_dag(n=8)
```

## Network Presets

```python
from dagbench.networks import fog_network, homogeneous_network, star_network, mec_network, manet_network

# 3-tier IoT fog topology
net = fog_network(num_edge=10, num_fog=3, num_cloud=2)

# Homogeneous fully-connected
net = homogeneous_network(num_nodes=4, speed=1.0, bandwidth=100.0)

# Star topology
net = star_network(num_edge=6, hub_speed=10.0)

# MEC: UE devices + MEC servers + cloud
net = mec_network(num_ue=4, num_mec=2)

# MANET/tactical mesh: command node + mobile peers
net = manet_network(num_nodes=6)
```

## Contributing

See [docs/contributing.md](docs/contributing.md) for how to add new workflows.

## License

- **DAGBench package + workflow data**: Apache-2.0
- **SAGA dependency**: Non-commercial (USC/ANRG) — DAGBench imports but does not redistribute SAGA
- Per-workflow source licenses tracked in each `metadata.yaml`

## Citation

If you use DAGBench in your research, please cite:

```bibtex
@software{dagbench2026,
  title={DAGBench: A Task Graph Benchmark Repository for Scheduling Research},
  year={2026},
  url={https://github.com/dagbench/dagbench}
}
```

See also the SAGA/PISA paper that motivated this work:
```bibtex
@article{boggs2024pisa,
  title={PISA: A Comprehensive Benchmark for Scheduling Algorithms},
  author={Boggs, Justin and Sights, Landon and Krishnamachari, Bhaskar},
  journal={arXiv preprint arXiv:2403.07120},
  year={2024}
}
```
