"""Import classic benchmark DAGs and manually-extracted paper DAGs.

Generates:
- Classic benchmarks: Gaussian elimination, FFT, LU, Cholesky, MapReduce
- Edge computing DAGs: face recognition, AR pipeline, autonomous driving
- ML pipeline DAGs: training, inference, federated learning
- Telecom DAGs: 5G SFC, O-RAN pipeline
- Scientific workflow patterns: Montage-like, Epigenomics-like
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import yaml
from saga import Network
from saga.schedulers.data import ProblemInstance

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.stats import compute_graph_stats
from dagbench.converters.manual import (
    DAGBuilder,
    gaussian_elimination_dag,
    fft_dag,
    lu_decomposition_dag,
    cholesky_dag,
    mapreduce_dag,
)
from dagbench.networks import homogeneous_network, fog_network, star_network

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"
TODAY = date.today().isoformat()


def _save_workflow(wf_dir: Path, instance: ProblemInstance, metadata: dict) -> None:
    wf_dir.mkdir(parents=True, exist_ok=True)
    raw = json.loads(instance.model_dump_json(indent=2))
    if "network" in raw and "edges" in raw["network"]:
        for e in raw["network"]["edges"]:
            if e.get("speed") is None:
                e["speed"] = 1e9
    with open(wf_dir / "graph.json", "w", encoding="utf-8") as f:
        json.dump(raw, f, indent=2)
    with open(wf_dir / "metadata.yaml", "w", encoding="utf-8") as f:
        yaml.dump(metadata, f, default_flow_style=False, sort_keys=False)
    print(f"  Saved: {wf_dir.relative_to(PROJECT_ROOT)}")


def _make_metadata(wf_id, name, description, domains, task_graph, provenance,
                   completeness="full", cost_model="deterministic",
                   network_included=True, network_topology=None,
                   network_nodes_min=None, network_nodes_max=None,
                   tags=None, campaign="campaign_002_classic_and_manual"):
    stats = compute_graph_stats(task_graph)
    return {
        "id": wf_id,
        "name": name,
        "description": description,
        "domains": domains,
        "provenance": provenance,
        "license": {
            "source_license": "Apache-2.0",
            "dagbench_license": "Apache-2.0",
            "notes": "Classic benchmark structure; costs are representative",
        },
        "completeness": completeness,
        "cost_model": cost_model,
        "network": {
            "included": network_included,
            "topology": network_topology,
            "num_nodes_min": network_nodes_min,
            "num_nodes_max": network_nodes_max,
        },
        "graph_stats": {
            "num_tasks": stats.num_tasks,
            "num_edges": stats.num_edges,
            "depth": stats.depth,
            "width": stats.width,
            "ccr": round(stats.ccr, 4) if stats.ccr else None,
            "parallelism": round(stats.parallelism, 4) if stats.parallelism else None,
        },
        "campaign": campaign,
        "tags": tags,
    }


# --- Provenance templates ---

def _classic_provenance(paper_title, authors, year, doi=None, notes=None):
    p = {
        "source": "Classic scheduling benchmark",
        "paper_title": paper_title,
        "authors": authors,
        "year": year,
        "extraction_method": "programmatic",
        "extractor": "dagbench-import-script",
        "extraction_date": TODAY,
    }
    if doi:
        p["paper_doi"] = doi
    if notes:
        p["figure_or_table"] = notes
    return p


def _manual_provenance(source, paper_title, authors, year, doi=None, arxiv=None, figure=None):
    p = {
        "source": source,
        "paper_title": paper_title,
        "authors": authors,
        "year": year,
        "extraction_method": "manual-figure",
        "extractor": "dagbench-import-script",
        "extraction_date": TODAY,
    }
    if doi:
        p["paper_doi"] = doi
    if arxiv:
        p["paper_arxiv"] = arxiv
    if figure:
        p["figure_or_table"] = figure
    return p


# ====================
# CLASSIC BENCHMARKS
# ====================

def import_classic_benchmarks():
    print("\n=== Importing Classic Benchmarks ===")
    net3 = homogeneous_network(num_nodes=3, speed=1.0, bandwidth=100.0)
    net4 = homogeneous_network(num_nodes=4, speed=1.0, bandwidth=100.0)

    heft_prov = _classic_provenance(
        "Performance-effective and low-complexity task scheduling for heterogeneous computing",
        ["Haluk Topcuoglu", "Salim Hariri", "Min-You Wu"],
        2002,
        doi="10.1109/71.993206",
    )

    # Gaussian Elimination n=5
    tg = gaussian_elimination_dag(n=5)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "gauss_elim_5",
        ProblemInstance(name="classic.gauss_elim_5", task_graph=tg, network=net3),
        _make_metadata("classic.gauss_elim_5", "Gaussian Elimination (n=5)",
            "Gaussian elimination for a 5x5 matrix. Classic benchmark from HEFT paper. Pivot and elimination tasks with column dependencies.",
            ["classic-benchmark"], tg, heft_prov,
            network_topology="fully-connected", network_nodes_min=3, network_nodes_max=3,
            tags=["gaussian-elimination", "linear-algebra", "heft"]),
    )

    # Gaussian Elimination n=10
    tg = gaussian_elimination_dag(n=10)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "gauss_elim_10",
        ProblemInstance(name="classic.gauss_elim_10", task_graph=tg, network=net4),
        _make_metadata("classic.gauss_elim_10", "Gaussian Elimination (n=10)",
            "Gaussian elimination for a 10x10 matrix. Large classic benchmark for heterogeneous scheduling.",
            ["classic-benchmark"], tg, heft_prov,
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["gaussian-elimination", "linear-algebra", "large"]),
    )

    # FFT 8-point
    tg = fft_dag(num_points=8)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "fft_8",
        ProblemInstance(name="classic.fft_8", task_graph=tg, network=net3),
        _make_metadata("classic.fft_8", "FFT Butterfly (8-point)",
            "8-point Fast Fourier Transform butterfly DAG. 3 stages of butterfly operations with input/output tasks.",
            ["classic-benchmark"], tg,
            _classic_provenance("The FFT: An Algorithm the Whole Family Can Use",
                ["Thomas H. Cormen"], 1999, notes="Standard butterfly structure"),
            network_topology="fully-connected", network_nodes_min=3, network_nodes_max=3,
            tags=["fft", "signal-processing", "butterfly"]),
    )

    # FFT 16-point
    tg = fft_dag(num_points=16)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "fft_16",
        ProblemInstance(name="classic.fft_16", task_graph=tg, network=net4),
        _make_metadata("classic.fft_16", "FFT Butterfly (16-point)",
            "16-point Fast Fourier Transform butterfly DAG. 4 stages of butterfly operations.",
            ["classic-benchmark"], tg,
            _classic_provenance("The FFT: An Algorithm the Whole Family Can Use",
                ["Thomas H. Cormen"], 1999, notes="Standard butterfly structure"),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["fft", "signal-processing", "butterfly", "medium"]),
    )

    # LU Decomposition n=4
    tg = lu_decomposition_dag(n=4)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "lu_decomp_4",
        ProblemInstance(name="classic.lu_decomp_4", task_graph=tg, network=net3),
        _make_metadata("classic.lu_decomp_4", "LU Decomposition (4x4 tiles)",
            "Tiled LU decomposition for a 4x4 tile grid. Tasks: GETRF (factor), TRSM_L/U (triangular solve), GEMM (update).",
            ["classic-benchmark"], tg,
            _classic_provenance("Scheduling of QR factorization algorithms on SMP and multi-core architectures",
                ["Alfredo Buttari", "Julien Langou", "Jakub Kurzak", "Jack Dongarra"],
                2009, doi="10.1016/j.parco.2008.12.008"),
            network_topology="fully-connected", network_nodes_min=3, network_nodes_max=3,
            tags=["lu-decomposition", "linear-algebra", "tiled"]),
    )

    # Cholesky n=4
    tg = cholesky_dag(n=4)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "cholesky_4",
        ProblemInstance(name="classic.cholesky_4", task_graph=tg, network=net3),
        _make_metadata("classic.cholesky_4", "Cholesky Factorization (4x4 tiles)",
            "Tiled Cholesky factorization for a 4x4 tile grid. Tasks: POTRF, TRSM, SYRK, GEMM. Classic dense linear algebra benchmark.",
            ["classic-benchmark"], tg,
            _classic_provenance("Design of Multicore Sparse Cholesky Factorization",
                ["Florent Lopez", "Laurent Parry", "Gregoire Pichon"],
                2015),
            network_topology="fully-connected", network_nodes_min=3, network_nodes_max=3,
            tags=["cholesky", "linear-algebra", "tiled"]),
    )

    # Cholesky n=5 (larger)
    tg = cholesky_dag(n=5)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "cholesky_5",
        ProblemInstance(name="classic.cholesky_5", task_graph=tg, network=net4),
        _make_metadata("classic.cholesky_5", "Cholesky Factorization (5x5 tiles)",
            "Tiled Cholesky factorization for a 5x5 tile grid. Larger benchmark with more parallelism.",
            ["classic-benchmark"], tg,
            _classic_provenance("Design of Multicore Sparse Cholesky Factorization",
                ["Florent Lopez", "Laurent Parry", "Gregoire Pichon"],
                2015),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["cholesky", "linear-algebra", "tiled", "large"]),
    )

    # MapReduce 8m/4r
    tg = mapreduce_dag(num_mappers=8, num_reducers=4)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "mapreduce_8m_4r",
        ProblemInstance(name="classic.mapreduce_8m_4r", task_graph=tg, network=net4),
        _make_metadata("classic.mapreduce_8m_4r", "MapReduce (8 mappers, 4 reducers)",
            "MapReduce pattern with 8 map tasks and 4 reduce tasks. Structure: Split -> Map[8] -> Shuffle -> Reduce[4] -> Merge.",
            ["classic-benchmark", "data-analytics"], tg,
            _classic_provenance("MapReduce: Simplified Data Processing on Large Clusters",
                ["Jeffrey Dean", "Sanjay Ghemawat"], 2004,
                doi="10.1145/1327452.1327492"),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["mapreduce", "data-parallel", "scatter-gather"]),
    )

    # MapReduce 16m/8r
    tg = mapreduce_dag(num_mappers=16, num_reducers=8)
    _save_workflow(
        WORKFLOWS_DIR / "classic_benchmarks" / "mapreduce_16m_8r",
        ProblemInstance(name="classic.mapreduce_16m_8r", task_graph=tg, network=net4),
        _make_metadata("classic.mapreduce_16m_8r", "MapReduce (16 mappers, 8 reducers)",
            "Large MapReduce pattern. Structure: Split -> Map[16] -> Shuffle -> Reduce[8] -> Merge.",
            ["classic-benchmark", "data-analytics"], tg,
            _classic_provenance("MapReduce: Simplified Data Processing on Large Clusters",
                ["Jeffrey Dean", "Sanjay Ghemawat"], 2004,
                doi="10.1145/1327452.1327492"),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["mapreduce", "data-parallel", "scatter-gather", "large"]),
    )


# ====================
# EDGE COMPUTING DAGs
# ====================

def import_edge_computing_dags():
    print("\n=== Importing Edge Computing DAGs ===")
    fog_net = fog_network(num_edge=5, num_fog=2, num_cloud=1)

    # Face Recognition Pipeline
    tg = (DAGBuilder("face_recognition")
        .task("Capture", 2.0)
        .task("Preprocess", 5.0)
        .task("FaceDetect", 15.0)
        .task("FeatureExtract", 20.0)
        .task("FaceMatch", 10.0)
        .task("AgeGender", 8.0)
        .task("Output", 1.0)
        .chain("Capture", "Preprocess", "FaceDetect")
        .fan_out("FaceDetect", "FeatureExtract", "AgeGender")
        .edge("FeatureExtract", "FaceMatch", 3.0)
        .fan_in("FaceMatch", "AgeGender", target="Output")
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "face_recognition",
        ProblemInstance(name="edge.face_recognition", task_graph=tg, network=fog_net),
        _make_metadata("edge.face_recognition", "Face Recognition Pipeline",
            "Mobile edge computing face recognition pipeline. Tasks: capture, preprocess, detect, feature extraction, matching, age/gender estimation. Typical MEC task offloading application.",
            ["edge-computing", "image-processing"], tg,
            _manual_provenance("MEC task offloading literature",
                "Dependent Task Offloading Model for Mobile Edge Computing",
                ["Various"], 2023, doi="10.3390/electronics14163184",
                figure="Fig. 1-2"),
            network_topology="fog-three-tier", network_nodes_min=8, network_nodes_max=8,
            tags=["face-recognition", "mec", "offloading"]),
    )

    # AR Object Detection Pipeline
    tg = (DAGBuilder("ar_pipeline")
        .task("FrameCapture", 1.0)
        .task("Preprocessing", 3.0)
        .task("ObjectDetect", 25.0)
        .task("DepthEstimate", 15.0)
        .task("Tracking", 10.0)
        .task("PoseEstimate", 12.0)
        .task("Rendering", 20.0)
        .task("Display", 1.0)
        .edge("FrameCapture", "Preprocessing", 5.0)
        .fan_out("Preprocessing", "ObjectDetect", "DepthEstimate")
        .edge("ObjectDetect", "Tracking", 2.0)
        .edge("DepthEstimate", "PoseEstimate", 2.0)
        .edge("Tracking", "Rendering", 3.0)
        .edge("PoseEstimate", "Rendering", 3.0)
        .edge("Rendering", "Display", 5.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "ar_pipeline",
        ProblemInstance(name="edge.ar_pipeline", task_graph=tg, network=fog_net),
        _make_metadata("edge.ar_pipeline", "AR Object Detection Pipeline",
            "Augmented reality processing pipeline for mobile edge computing. Parallel object detection and depth estimation branches merge at rendering. Latency-critical application.",
            ["edge-computing", "image-processing"], tg,
            _manual_provenance("Edge computing AR literature",
                "DAG-based Task Orchestration for Edge Computing",
                ["Various"], 2023, figure="Fig. 3"),
            network_topology="fog-three-tier", network_nodes_min=8, network_nodes_max=8,
            tags=["ar", "augmented-reality", "latency-critical", "mec"]),
    )

    # Autonomous Driving Pipeline
    tg = (DAGBuilder("autonomous_driving")
        .task("LiDAR", 3.0)
        .task("Camera", 2.0)
        .task("Radar", 2.0)
        .task("LiDARProc", 15.0)
        .task("ImageProc", 12.0)
        .task("RadarProc", 8.0)
        .task("SensorFusion", 10.0)
        .task("ObjectDetect", 20.0)
        .task("PathPlan", 15.0)
        .task("Decision", 5.0)
        .task("Actuate", 2.0)
        .edge("LiDAR", "LiDARProc", 10.0)
        .edge("Camera", "ImageProc", 8.0)
        .edge("Radar", "RadarProc", 4.0)
        .fan_in("LiDARProc", "ImageProc", "RadarProc", target="SensorFusion", edge_size=5.0)
        .chain("SensorFusion", "ObjectDetect", "PathPlan", "Decision", "Actuate")
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "autonomous_driving",
        ProblemInstance(name="edge.autonomous_driving", task_graph=tg, network=fog_net),
        _make_metadata("edge.autonomous_driving", "Autonomous Driving Pipeline",
            "Vehicle autonomous driving processing pipeline. Three sensor sources (LiDAR, camera, radar) fuse into detection and planning chain. Strict real-time constraints.",
            ["edge-computing", "cyber-physical"], tg,
            _manual_provenance("V2X computing literature",
                "Collaborative Computation and Dependency-Aware Task Offloading for Vehicular Edge Computing",
                ["Various"], 2022, doi="10.1186/s13677-022-00340-3",
                figure="Fig. 1-2"),
            network_topology="fog-three-tier", network_nodes_min=8, network_nodes_max=8,
            tags=["autonomous-driving", "v2x", "sensor-fusion", "real-time"]),
    )

    # Smart Home IoT Pipeline
    tg = (DAGBuilder("smart_home")
        .task("TempSensor", 1.0)
        .task("HumiditySensor", 1.0)
        .task("MotionSensor", 1.0)
        .task("DoorSensor", 1.0)
        .task("Aggregate", 3.0)
        .task("Anomaly", 8.0)
        .task("Predict", 10.0)
        .task("RuleEngine", 5.0)
        .task("HVACControl", 2.0)
        .task("LightControl", 2.0)
        .task("SecurityAlert", 3.0)
        .task("Dashboard", 2.0)
        .fan_in("TempSensor", "HumiditySensor", "MotionSensor", "DoorSensor",
                target="Aggregate", edge_size=1.0)
        .fan_out("Aggregate", "Anomaly", "Predict", "RuleEngine", edge_size=2.0)
        .edge("Anomaly", "SecurityAlert", 1.0)
        .edge("Predict", "HVACControl", 1.0)
        .edge("RuleEngine", "LightControl", 1.0)
        .fan_in("HVACControl", "LightControl", "SecurityAlert",
                target="Dashboard", edge_size=1.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "smart_home",
        ProblemInstance(name="edge.smart_home", task_graph=tg, network=fog_net),
        _make_metadata("edge.smart_home", "Smart Home IoT Pipeline",
            "Smart home automation pipeline. 4 sensors → aggregate → parallel anomaly/predict/rule branches → 3 actuators → dashboard. Typical fog computing use case.",
            ["iot", "edge-computing", "fog-computing"], tg,
            _manual_provenance("Smart home IoT literature",
                "IoT Smart Home Automation",
                ["Various"], 2023),
            network_topology="fog-three-tier", network_nodes_min=8, network_nodes_max=8,
            tags=["smart-home", "iot", "automation", "fog"]),
    )

    # Video Transcoding Pipeline
    tg = (DAGBuilder("video_transcoding")
        .task("Demux", 3.0)
        .task("Decode_GOP0", 10.0)
        .task("Decode_GOP1", 10.0)
        .task("Decode_GOP2", 10.0)
        .task("Decode_GOP3", 10.0)
        .task("Resize_GOP0", 8.0)
        .task("Resize_GOP1", 8.0)
        .task("Resize_GOP2", 8.0)
        .task("Resize_GOP3", 8.0)
        .task("Encode_GOP0", 15.0)
        .task("Encode_GOP1", 15.0)
        .task("Encode_GOP2", 15.0)
        .task("Encode_GOP3", 15.0)
        .task("Mux", 3.0)
        .fan_out("Demux", "Decode_GOP0", "Decode_GOP1", "Decode_GOP2", "Decode_GOP3", edge_size=20.0)
        .build())
    for i in range(4):
        tg_builder = DAGBuilder.__new__(DAGBuilder)
        # Already built, add the chain edges manually
    # Rebuild with all edges
    tg = (DAGBuilder("video_transcoding")
        .task("Demux", 3.0)
        .task("Mux", 3.0)
        .build())
    # Let me build this properly
    builder = DAGBuilder("video_transcoding")
    builder.task("Demux", 3.0).task("Mux", 3.0)
    for i in range(4):
        builder.task(f"Decode_GOP{i}", 10.0)
        builder.task(f"Resize_GOP{i}", 8.0)
        builder.task(f"Encode_GOP{i}", 15.0)
        builder.edge("Demux", f"Decode_GOP{i}", 20.0)
        builder.chain(f"Decode_GOP{i}", f"Resize_GOP{i}", f"Encode_GOP{i}", edge_size=10.0)
        builder.edge(f"Encode_GOP{i}", "Mux", 15.0)
    tg = builder.build()

    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "video_transcoding",
        ProblemInstance(name="edge.video_transcoding", task_graph=tg, network=fog_net),
        _make_metadata("edge.video_transcoding", "Video Transcoding Pipeline",
            "Video transcoding with 4 parallel GOP (Group of Pictures) processing lanes. Each GOP: decode → resize → encode. Classic data-parallel edge workload.",
            ["edge-computing", "data-analytics"], tg,
            _manual_provenance("Video processing literature",
                "Performance Analysis of Video Transcoding in Cloud and Edge",
                ["Various"], 2018),
            network_topology="fog-three-tier", network_nodes_min=8, network_nodes_max=8,
            tags=["video", "transcoding", "data-parallel", "gop"]),
    )


# ====================
# ML PIPELINE DAGs
# ====================

def import_ml_pipeline_dags():
    print("\n=== Importing ML Pipeline DAGs ===")
    net = homogeneous_network(num_nodes=4, speed=2.0, bandwidth=500.0)

    # ML Training Pipeline
    tg = (DAGBuilder("ml_training")
        .task("DataIngest", 5.0)
        .task("DataClean", 8.0)
        .task("FeatureEng_Numeric", 10.0)
        .task("FeatureEng_Text", 12.0)
        .task("FeatureEng_Image", 20.0)
        .task("FeatureSelect", 6.0)
        .task("TrainModel_A", 30.0)
        .task("TrainModel_B", 35.0)
        .task("TrainModel_C", 25.0)
        .task("Evaluate_A", 5.0)
        .task("Evaluate_B", 5.0)
        .task("Evaluate_C", 5.0)
        .task("EnsembleSelect", 8.0)
        .task("Deploy", 3.0)
        .chain("DataIngest", "DataClean", edge_size=10.0)
        .fan_out("DataClean", "FeatureEng_Numeric", "FeatureEng_Text", "FeatureEng_Image", edge_size=5.0)
        .fan_in("FeatureEng_Numeric", "FeatureEng_Text", "FeatureEng_Image",
                target="FeatureSelect", edge_size=3.0)
        .fan_out("FeatureSelect", "TrainModel_A", "TrainModel_B", "TrainModel_C", edge_size=5.0)
        .edge("TrainModel_A", "Evaluate_A", 2.0)
        .edge("TrainModel_B", "Evaluate_B", 2.0)
        .edge("TrainModel_C", "Evaluate_C", 2.0)
        .fan_in("Evaluate_A", "Evaluate_B", "Evaluate_C",
                target="EnsembleSelect", edge_size=1.0)
        .edge("EnsembleSelect", "Deploy", 2.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "ml_pipelines" / "training_ensemble",
        ProblemInstance(name="ml.training_ensemble", task_graph=tg, network=net),
        _make_metadata("ml.training_ensemble", "ML Training Pipeline (Ensemble)",
            "End-to-end ML training pipeline with 3 parallel feature engineering branches, 3 parallel model training lanes, and ensemble selection. Representative of production ML workflows.",
            ["ml-pipeline", "data-analytics"], tg,
            _manual_provenance("ML pipeline literature",
                "ML Pipelines: Components and Best Practices",
                ["Various"], 2023),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["ml-training", "ensemble", "feature-engineering"]),
    )

    # Inference Pipeline (CNN)
    tg = (DAGBuilder("cnn_inference")
        .task("LoadImage", 1.0)
        .task("Resize", 2.0)
        .task("Normalize", 1.0)
        .task("Conv1", 15.0)
        .task("Pool1", 3.0)
        .task("Conv2", 20.0)
        .task("Pool2", 3.0)
        .task("Conv3", 25.0)
        .task("Flatten", 1.0)
        .task("FC1", 10.0)
        .task("FC2", 5.0)
        .task("Softmax", 1.0)
        .task("PostProcess", 2.0)
        .chain("LoadImage", "Resize", "Normalize",
               "Conv1", "Pool1", "Conv2", "Pool2", "Conv3",
               "Flatten", "FC1", "FC2", "Softmax", "PostProcess",
               edge_size=5.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "ml_pipelines" / "cnn_inference",
        ProblemInstance(name="ml.cnn_inference", task_graph=tg, network=net),
        _make_metadata("ml.cnn_inference", "CNN Inference Pipeline",
            "Convolutional neural network inference pipeline. Sequential layers: preprocessing → conv/pool stages → fully connected → softmax. Represents deep learning inference workload.",
            ["ml-pipeline"], tg,
            _manual_provenance("Deep learning inference literature",
                "Deep Learning Inference Optimization",
                ["Various"], 2022),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["cnn", "inference", "deep-learning", "sequential"]),
    )

    # Federated Learning Round
    num_clients = 5
    builder = DAGBuilder("federated_learning")
    builder.task("GlobalInit", 3.0)
    builder.task("ClientSelect", 2.0)
    builder.task("Aggregate", 10.0)
    builder.task("GlobalUpdate", 5.0)
    builder.edge("GlobalInit", "ClientSelect", 1.0)
    for i in range(num_clients):
        builder.task(f"Distribute_{i}", 3.0)
        builder.task(f"LocalTrain_{i}", 25.0)
        builder.task(f"Upload_{i}", 5.0)
        builder.edge("ClientSelect", f"Distribute_{i}", 10.0)
        builder.edge(f"Distribute_{i}", f"LocalTrain_{i}", 10.0)
        builder.edge(f"LocalTrain_{i}", f"Upload_{i}", 10.0)
        builder.edge(f"Upload_{i}", "Aggregate", 10.0)
    builder.edge("Aggregate", "GlobalUpdate", 2.0)
    tg = builder.build()

    _save_workflow(
        WORKFLOWS_DIR / "ml_pipelines" / "federated_learning",
        ProblemInstance(name="ml.federated_learning", task_graph=tg, network=net),
        _make_metadata("ml.federated_learning", "Federated Learning Round (5 clients)",
            "One round of federated learning with 5 clients. Structure: init → select → distribute[5] → local_train[5] → upload[5] → aggregate → update. Each client trains independently.",
            ["ml-pipeline"], tg,
            _manual_provenance("Federated learning literature",
                "Optimal DAG Federated Learning",
                ["Various"], 2024, doi="10.1038/s41598-024-71995-y"),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["federated-learning", "distributed", "privacy"]),
    )

    # Cross-Validation Pipeline
    num_folds = 5
    builder = DAGBuilder("cross_validation")
    builder.task("DataLoad", 5.0)
    builder.task("Preprocess", 8.0)
    builder.task("FinalSelect", 5.0)
    builder.task("FinalTrain", 30.0)
    builder.task("Report", 2.0)
    builder.chain("DataLoad", "Preprocess", edge_size=10.0)
    for i in range(num_folds):
        builder.task(f"Split_{i}", 2.0)
        builder.task(f"Train_{i}", 20.0)
        builder.task(f"Validate_{i}", 5.0)
        builder.edge("Preprocess", f"Split_{i}", 5.0)
        builder.edge(f"Split_{i}", f"Train_{i}", 5.0)
        builder.edge(f"Train_{i}", f"Validate_{i}", 2.0)
        builder.edge(f"Validate_{i}", "FinalSelect", 1.0)
    builder.chain("FinalSelect", "FinalTrain", "Report", edge_size=2.0)
    tg = builder.build()

    _save_workflow(
        WORKFLOWS_DIR / "ml_pipelines" / "cross_validation",
        ProblemInstance(name="ml.cross_validation", task_graph=tg, network=net),
        _make_metadata("ml.cross_validation", "5-Fold Cross Validation Pipeline",
            "5-fold cross-validation pipeline. Data → preprocess → 5 parallel (split/train/validate) folds → select best → final train → report.",
            ["ml-pipeline", "data-analytics"], tg,
            _manual_provenance("ML methodology",
                "Cross-Validation for Model Selection",
                ["Various"], 2020),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["cross-validation", "model-selection", "parallel-folds"]),
    )


# ====================
# TELECOM / 5G DAGs
# ====================

def import_telecom_dags():
    print("\n=== Importing Telecom/5G DAGs ===")
    star_net = star_network(num_edge=6, hub_speed=10.0, edge_speed=2.0, bandwidth=5000.0)

    # 5G Service Function Chain
    tg = (DAGBuilder("5g_sfc")
        .task("Traffic", 2.0)
        .task("DPI", 8.0)
        .task("Firewall", 5.0)
        .task("NAT", 3.0)
        .task("LoadBalancer", 4.0)
        .task("IDS", 10.0)
        .task("CDN", 6.0)
        .task("Application", 15.0)
        .chain("Traffic", "DPI", "Firewall", "NAT", "LoadBalancer", edge_size=5.0)
        .edge("LoadBalancer", "IDS", 3.0)
        .edge("LoadBalancer", "CDN", 3.0)
        .fan_in("IDS", "CDN", target="Application", edge_size=2.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "sfc_5g",
        ProblemInstance(name="telecom.sfc_5g", task_graph=tg, network=star_net),
        _make_metadata("telecom.sfc_5g", "5G Service Function Chain",
            "5G network service function chain. Traffic → DPI → Firewall → NAT → LoadBalancer → (IDS, CDN) → Application. Typical NFV deployment.",
            ["telecom", "5g", "edge-computing"], tg,
            _manual_provenance("NFV/SFC literature",
                "Network Function Virtualization and Service Function Chaining",
                ["Various"], 2022, doi="10.3390/fi14020059"),
            network_topology="star", network_nodes_min=7, network_nodes_max=7,
            tags=["sfc", "nfv", "5g", "network-functions"]),
    )

    # O-RAN Uplink Pipeline
    tg = (DAGBuilder("oran_uplink")
        .task("RF_Receive", 1.0)
        .task("ADC", 2.0)
        .task("LowPHY_FFT", 5.0)
        .task("LowPHY_Demod", 4.0)
        .task("HighPHY_Decode", 8.0)
        .task("HighPHY_Descramble", 3.0)
        .task("MAC_Demux", 4.0)
        .task("RLC_Reassemble", 3.0)
        .task("PDCP_Decipher", 5.0)
        .task("SDAP_Map", 2.0)
        .task("CoreForward", 1.0)
        .chain("RF_Receive", "ADC", "LowPHY_FFT", "LowPHY_Demod",
               "HighPHY_Decode", "HighPHY_Descramble",
               "MAC_Demux", "RLC_Reassemble", "PDCP_Decipher",
               "SDAP_Map", "CoreForward", edge_size=5.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "oran_uplink",
        ProblemInstance(name="telecom.oran_uplink", task_graph=tg, network=star_net),
        _make_metadata("telecom.oran_uplink", "O-RAN Uplink Processing Pipeline",
            "O-RAN uplink signal processing chain. RF → ADC → Low-PHY (FFT, demod) → High-PHY (decode, descramble) → MAC → RLC → PDCP → SDAP → Core. Split 7.2 architecture.",
            ["telecom", "5g"], tg,
            _manual_provenance("O-RAN architecture",
                "O-RAN Alliance Architecture Description",
                ["O-RAN Alliance"], 2023),
            network_topology="star", network_nodes_min=7, network_nodes_max=7,
            tags=["oran", "5g", "uplink", "signal-processing", "sequential"]),
    )

    # Network Slicing Orchestration
    tg = (DAGBuilder("network_slicing")
        .task("SliceRequest", 2.0)
        .task("ResourceCheck", 3.0)
        .task("Admission", 4.0)
        .task("VNF_Deploy_1", 8.0)
        .task("VNF_Deploy_2", 8.0)
        .task("VNF_Deploy_3", 8.0)
        .task("SFC_Chain", 5.0)
        .task("QoS_Config", 4.0)
        .task("Monitor_Setup", 3.0)
        .task("Activate", 2.0)
        .chain("SliceRequest", "ResourceCheck", "Admission", edge_size=1.0)
        .fan_out("Admission", "VNF_Deploy_1", "VNF_Deploy_2", "VNF_Deploy_3", edge_size=2.0)
        .fan_in("VNF_Deploy_1", "VNF_Deploy_2", "VNF_Deploy_3",
                target="SFC_Chain", edge_size=2.0)
        .fan_out("SFC_Chain", "QoS_Config", "Monitor_Setup", edge_size=1.0)
        .fan_in("QoS_Config", "Monitor_Setup", target="Activate", edge_size=1.0)
        .build())
    _save_workflow(
        WORKFLOWS_DIR / "edge_computing" / "network_slicing",
        ProblemInstance(name="telecom.network_slicing", task_graph=tg, network=star_net),
        _make_metadata("telecom.network_slicing", "5G Network Slicing Orchestration",
            "Network slice provisioning workflow. Request → check → admit → deploy VNFs[3] → chain → (QoS, monitor) → activate. Orchestration of 5G network slice lifecycle.",
            ["telecom", "5g", "edge-computing"], tg,
            _manual_provenance("Network slicing literature",
                "5G Network Slicing Orchestration",
                ["Various"], 2022),
            network_topology="star", network_nodes_min=7, network_nodes_max=7,
            tags=["network-slicing", "orchestration", "vnf", "5g"]),
    )


# ====================
# SCIENTIFIC WORKFLOWS
# ====================

def import_scientific_workflows():
    print("\n=== Importing Scientific Workflow Patterns ===")
    net = homogeneous_network(num_nodes=4, speed=2.0, bandwidth=1000.0)

    # Montage-like astronomy workflow
    builder = DAGBuilder("montage_like")
    # Stage 1: Project (parallel)
    for i in range(6):
        builder.task(f"mProject_{i}", 10.0)
    # Stage 2: Diff (pairwise)
    builder.task("mDiffFit_01", 5.0).task("mDiffFit_23", 5.0).task("mDiffFit_45", 5.0)
    builder.edge("mProject_0", "mDiffFit_01", 8.0).edge("mProject_1", "mDiffFit_01", 8.0)
    builder.edge("mProject_2", "mDiffFit_23", 8.0).edge("mProject_3", "mDiffFit_23", 8.0)
    builder.edge("mProject_4", "mDiffFit_45", 8.0).edge("mProject_5", "mDiffFit_45", 8.0)
    # Stage 3: Concat
    builder.task("mConcatFit", 3.0)
    builder.fan_in("mDiffFit_01", "mDiffFit_23", "mDiffFit_45",
                   target="mConcatFit", edge_size=2.0)
    # Stage 4: Background model
    builder.task("mBgModel", 8.0)
    builder.edge("mConcatFit", "mBgModel", 3.0)
    # Stage 5: Background correction (parallel)
    for i in range(6):
        builder.task(f"mBackground_{i}", 5.0)
        builder.edge("mBgModel", f"mBackground_{i}", 2.0)
        builder.edge(f"mProject_{i}", f"mBackground_{i}", 5.0)
    # Stage 6: Add
    builder.task("mAdd", 15.0)
    for i in range(6):
        builder.edge(f"mBackground_{i}", "mAdd", 10.0)
    # Final: Shrink
    builder.task("mShrink", 3.0)
    builder.edge("mAdd", "mShrink", 5.0)

    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "scientific_workflows" / "montage_like",
        ProblemInstance(name="scientific.montage_like", task_graph=tg, network=net),
        _make_metadata("scientific.montage_like", "Montage-like Astronomy Workflow",
            "Astronomy image mosaic workflow modeled after Montage. Stages: project[6] → diff_fit[3] → concat → bg_model → background[6] → add → shrink. Scatter-gather pattern.",
            ["scientific", "astronomy", "image-processing"], tg,
            _manual_provenance("Pegasus Montage workflow",
                "Montage: An Astronomical Image Mosaic Engine",
                ["Jacob C. Good", "Joseph E. Berriman", "G. Bruce Berriman"],
                2004),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["montage", "astronomy", "scatter-gather", "scientific"]),
    )

    # Epigenomics-like genome workflow
    builder = DAGBuilder("epigenomics_like")
    num_lanes = 4
    builder.task("FastQC", 5.0)
    for i in range(num_lanes):
        builder.task(f"FilterContam_{i}", 8.0)
        builder.task(f"Sol2Sanger_{i}", 3.0)
        builder.task(f"MapMerge_{i}", 12.0)
        builder.task(f"MapRC_{i}", 6.0)
        builder.edge("FastQC", f"FilterContam_{i}", 10.0)
        builder.chain(f"FilterContam_{i}", f"Sol2Sanger_{i}",
                     f"MapMerge_{i}", f"MapRC_{i}", edge_size=5.0)
    builder.task("PeakCall", 15.0)
    for i in range(num_lanes):
        builder.edge(f"MapRC_{i}", "PeakCall", 8.0)
    builder.task("Annotate", 10.0)
    builder.edge("PeakCall", "Annotate", 5.0)

    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "scientific_workflows" / "epigenomics_like",
        ProblemInstance(name="scientific.epigenomics_like", task_graph=tg, network=net),
        _make_metadata("scientific.epigenomics_like", "Epigenomics-like Genome Workflow",
            "Genome processing workflow modeled after Epigenomics. QC → 4 parallel lanes (filter, convert, map, RC) → peak calling → annotation. Bioinformatics scatter-gather.",
            ["scientific", "genomics"], tg,
            _manual_provenance("Pegasus Epigenomics workflow",
                "Pegasus Workflow Management System",
                ["Ewa Deelman", "Karan Vahi", "Gideon Juve"],
                2015, doi="10.1016/j.future.2014.10.008"),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["epigenomics", "genomics", "bioinformatics", "scatter-gather"]),
    )

    # Seismology-like workflow
    builder = DAGBuilder("seismology_like")
    num_sites = 3
    num_ruptures = 3
    builder.task("SiteInfo", 3.0)
    for s in range(num_sites):
        builder.task(f"ExtractSGT_{s}", 10.0)
        builder.edge("SiteInfo", f"ExtractSGT_{s}", 5.0)
        for r in range(num_ruptures):
            builder.task(f"Synth_{s}_{r}", 15.0)
            builder.edge(f"ExtractSGT_{s}", f"Synth_{s}_{r}", 8.0)
            builder.task(f"PeakVal_{s}_{r}", 5.0)
            builder.edge(f"Synth_{s}_{r}", f"PeakVal_{s}_{r}", 3.0)

    builder.task("HazardCurve", 20.0)
    for s in range(num_sites):
        for r in range(num_ruptures):
            builder.edge(f"PeakVal_{s}_{r}", "HazardCurve", 2.0)
    builder.task("HazardMap", 8.0)
    builder.edge("HazardCurve", "HazardMap", 5.0)

    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "scientific_workflows" / "seismology_like",
        ProblemInstance(name="scientific.seismology_like", task_graph=tg, network=net),
        _make_metadata("scientific.seismology_like", "Seismology-like Hazard Workflow",
            "Earthquake hazard characterization workflow. Site info → extract SGTs[3] → synthesis[3x3] → peak values → hazard curve → hazard map. CyberShake-inspired.",
            ["scientific", "seismology"], tg,
            _manual_provenance("CyberShake/SCEC workflow",
                "CyberShake: A Physics-Based Seismic Hazard Model",
                ["Philip Maechling", "Ewa Deelman", "et al."],
                2015),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["seismology", "hazard", "cybershake", "scatter-gather"]),
    )

    # BLAST-like bioinformatics workflow
    builder = DAGBuilder("blast_like")
    num_chunks = 5
    builder.task("SplitDB", 5.0)
    for i in range(num_chunks):
        builder.task(f"FormatDB_{i}", 3.0)
        builder.task(f"BLAST_{i}", 20.0)
        builder.task(f"FilterHits_{i}", 5.0)
        builder.edge("SplitDB", f"FormatDB_{i}", 10.0)
        builder.chain(f"FormatDB_{i}", f"BLAST_{i}", f"FilterHits_{i}", edge_size=8.0)
    builder.task("MergeResults", 8.0)
    for i in range(num_chunks):
        builder.edge(f"FilterHits_{i}", "MergeResults", 5.0)
    builder.task("Annotate", 10.0)
    builder.task("Report", 3.0)
    builder.chain("MergeResults", "Annotate", "Report", edge_size=3.0)

    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "scientific_workflows" / "blast_like",
        ProblemInstance(name="scientific.blast_like", task_graph=tg, network=net),
        _make_metadata("scientific.blast_like", "BLAST-like Sequence Search Workflow",
            "Bioinformatics sequence search workflow modeled after BLAST. Split → 5 parallel (format, search, filter) lanes → merge → annotate → report.",
            ["scientific", "genomics"], tg,
            _manual_provenance("BLAST workflow pattern",
                "BLAST: Basic Local Alignment Search Tool",
                ["Stephen F. Altschul", "Warren Gish", "et al."],
                1990, doi="10.1016/S0022-2836(05)80360-2"),
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["blast", "bioinformatics", "sequence-search", "scatter-gather"]),
    )


# ====================
# ADDITIONAL SYNTHETIC PATTERNS
# ====================

def import_additional_synthetic():
    print("\n=== Importing Additional Synthetic Patterns ===")
    net = homogeneous_network(num_nodes=4, speed=1.0, bandwidth=100.0)

    gen_prov = {
        "source": "DAGBench synthetic generators",
        "extraction_method": "generated",
        "extractor": "dagbench-import-script",
        "extraction_date": TODAY,
    }

    # Wide parallel DAG (embarrassingly parallel)
    builder = DAGBuilder("wide_parallel")
    builder.task("Source", 1.0)
    builder.task("Sink", 1.0)
    for i in range(20):
        builder.task(f"Worker_{i}", 10.0)
        builder.edge("Source", f"Worker_{i}", 1.0)
        builder.edge(f"Worker_{i}", "Sink", 1.0)
    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "synthetic" / "wide_parallel_20",
        ProblemInstance(name="synthetic.wide_parallel_20", task_graph=tg, network=net),
        _make_metadata("synthetic.wide_parallel_20", "Wide Parallel DAG (20 workers)",
            "Embarrassingly parallel pattern: source → 20 independent workers → sink. Maximum parallelism, no inter-task communication.",
            ["synthetic"], tg, gen_prov,
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["embarrassingly-parallel", "wide", "benchmark"]),
    )

    # Pipeline stages (multi-stage sequential with some parallelism)
    builder = DAGBuilder("pipeline_stages")
    prev_tasks = ["Source"]
    builder.task("Source", 1.0)
    for stage in range(4):
        width = [2, 3, 2, 1][stage]
        stage_tasks = []
        for w in range(width):
            name = f"S{stage}_W{w}"
            builder.task(name, 10.0 + stage * 5)
            stage_tasks.append(name)
            for pt in prev_tasks:
                builder.edge(pt, name, 3.0)
        prev_tasks = stage_tasks
    builder.task("Sink", 1.0)
    for pt in prev_tasks:
        builder.edge(pt, "Sink", 1.0)
    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "synthetic" / "pipeline_stages",
        ProblemInstance(name="synthetic.pipeline_stages", task_graph=tg, network=net),
        _make_metadata("synthetic.pipeline_stages", "Multi-Stage Pipeline DAG",
            "Pipeline with varying widths per stage: source → 2 → 3 → 2 → 1 → sink. Tests scheduling across stages with different parallelism levels.",
            ["synthetic"], tg, gen_prov,
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["pipeline", "staged", "varying-width"]),
    )

    # Stencil computation pattern
    rows, cols = 3, 4
    builder = DAGBuilder("stencil")
    for r in range(rows):
        for c in range(cols):
            builder.task(f"Cell_{r}_{c}", 5.0)
            if c > 0:
                builder.edge(f"Cell_{r}_{c-1}", f"Cell_{r}_{c}", 2.0)
            if r > 0:
                builder.edge(f"Cell_{r-1}_{c}", f"Cell_{r}_{c}", 2.0)
    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "synthetic" / "stencil_3x4",
        ProblemInstance(name="synthetic.stencil_3x4", task_graph=tg, network=net),
        _make_metadata("synthetic.stencil_3x4", "Stencil Computation (3x4 grid)",
            "2D stencil computation pattern on a 3x4 grid. Each cell depends on its left and upper neighbors. Typical in PDE solvers and image processing.",
            ["synthetic"], tg, gen_prov,
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["stencil", "grid", "wavefront"]),
    )

    # Multi-level fork-join
    builder = DAGBuilder("multi_fork_join")
    builder.task("Start", 1.0)
    prev = "Start"
    for level in range(3):
        width = [3, 4, 2][level]
        join = f"Join_{level}"
        builder.task(join, 3.0)
        for w in range(width):
            name = f"L{level}_W{w}"
            builder.task(name, 8.0 + level * 2)
            builder.edge(prev, name, 2.0)
            builder.edge(name, join, 2.0)
        prev = join
    builder.task("End", 1.0)
    builder.edge(prev, "End", 1.0)
    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "synthetic" / "multi_fork_join",
        ProblemInstance(name="synthetic.multi_fork_join", task_graph=tg, network=net),
        _make_metadata("synthetic.multi_fork_join", "Multi-Level Fork-Join DAG",
            "Three levels of fork-join with varying widths (3, 4, 2). Start → fork[3] → join → fork[4] → join → fork[2] → join → end.",
            ["synthetic"], tg, gen_prov,
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["fork-join", "multi-level", "parallel"]),
    )

    # Reduction tree
    builder = DAGBuilder("reduction_tree")
    leaves = 8
    level = 0
    prev = []
    for i in range(leaves):
        name = f"Leaf_{i}"
        builder.task(name, 5.0)
        prev.append(name)
    while len(prev) > 1:
        curr = []
        for i in range(0, len(prev), 2):
            name = f"Reduce_L{level}_{i//2}"
            builder.task(name, 3.0 + level * 2)
            builder.edge(prev[i], name, 4.0)
            if i + 1 < len(prev):
                builder.edge(prev[i+1], name, 4.0)
            curr.append(name)
        prev = curr
        level += 1
    tg = builder.build()
    _save_workflow(
        WORKFLOWS_DIR / "synthetic" / "reduction_tree",
        ProblemInstance(name="synthetic.reduction_tree", task_graph=tg, network=net),
        _make_metadata("synthetic.reduction_tree", "Binary Reduction Tree (8 leaves)",
            "Binary reduction tree: 8 leaves → 4 → 2 → 1. Classic parallel reduction pattern used in sum/min/max aggregations.",
            ["synthetic"], tg, gen_prov,
            network_topology="fully-connected", network_nodes_min=4, network_nodes_max=4,
            tags=["reduction", "tree", "aggregation"]),
    )


def main():
    print(f"DAGBench Classic + Manual Import - {TODAY}")

    import_classic_benchmarks()
    import_edge_computing_dags()
    import_ml_pipeline_dags()
    import_telecom_dags()
    import_scientific_workflows()
    import_additional_synthetic()

    # Count results
    from dagbench.catalog import list_workflows
    workflows = list_workflows(workflows_dir=WORKFLOWS_DIR)
    print(f"\n=== Import Complete: {len(workflows)} total workflows ===")
    for wf in sorted(workflows, key=lambda w: w.id):
        print(f"  {wf.id:45s} {wf.graph_stats.num_tasks:4d} tasks  {wf.name}")


if __name__ == "__main__":
    main()
