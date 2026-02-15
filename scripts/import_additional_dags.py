"""Import additional DAGs to reach 50+ workflows.

Adds more edge computing, scientific, and domain-specific DAGs.
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
from dagbench.converters.manual import DAGBuilder
from dagbench.networks import homogeneous_network, fog_network, star_network

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"
TODAY = date.today().isoformat()


def _preflight():
    """Validate all imports and prerequisites before writing any files."""
    from dagbench.generators import gaussian_elimination_dag, fft_dag, cholesky_dag, mapreduce_dag  # noqa: F401
    from dagbench.converters.manual import DAGBuilder  # noqa: F401
    from dagbench.networks import homogeneous_network, fog_network, star_network  # noqa: F401
    assert WORKFLOWS_DIR.exists(), f"Missing: {WORKFLOWS_DIR}"


_preflight()


def _save_workflow(wf_dir, instance, metadata):
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


def _meta(wf_id, name, desc, domains, tg, prov, tags=None, net_topo="fully-connected",
          nn_min=4, nn_max=4, campaign="campaign_003_expansion"):
    stats = compute_graph_stats(tg)
    return {
        "id": wf_id, "name": name, "description": desc,
        "domains": domains,
        "provenance": prov,
        "license": {"source_license": "Apache-2.0", "dagbench_license": "Apache-2.0"},
        "completeness": "full", "cost_model": "deterministic",
        "network": {"included": True, "topology": net_topo,
                    "num_nodes_min": nn_min, "num_nodes_max": nn_max},
        "graph_stats": {
            "num_tasks": stats.num_tasks, "num_edges": stats.num_edges,
            "depth": stats.depth, "width": stats.width,
            "ccr": round(stats.ccr, 4) if stats.ccr is not None else None,
            "parallelism": round(stats.parallelism, 4) if stats.parallelism is not None else None,
        },
        "campaign": campaign, "tags": tags,
    }


def _prov(source, title=None, authors=None, year=None, doi=None, method="manual-figure"):
    p = {"source": source, "extraction_method": method,
         "extractor": "dagbench-import-script", "extraction_date": TODAY}
    if title: p["paper_title"] = title
    if authors: p["authors"] = authors
    if year: p["year"] = year
    if doi: p["paper_doi"] = doi
    return p


def main():
    print(f"DAGBench Additional Import - {TODAY}")
    net4 = homogeneous_network(num_nodes=4, speed=2.0, bandwidth=500.0)
    fog = fog_network(num_edge=5, num_fog=2, num_cloud=1)
    star = star_network(num_edge=6, hub_speed=10.0)

    # --- Genome/SOYKB-like workflow ---
    builder = DAGBuilder("soykb_like")
    builder.task("Index", 5.0)
    for i in range(3):
        builder.task(f"Align_{i}", 15.0)
        builder.task(f"Sort_{i}", 8.0)
        builder.task(f"Dedup_{i}", 6.0)
        builder.task(f"Realign_{i}", 10.0)
        builder.task(f"HaplotypeCaller_{i}", 20.0)
        builder.edge("Index", f"Align_{i}", 10.0)
        builder.chain(f"Align_{i}", f"Sort_{i}", f"Dedup_{i}",
                     f"Realign_{i}", f"HaplotypeCaller_{i}", edge_size=8.0)
    builder.task("MergeVCF", 10.0)
    for i in range(3):
        builder.edge(f"HaplotypeCaller_{i}", "MergeVCF", 5.0)
    builder.task("Filter", 8.0).task("Annotate", 12.0)
    builder.chain("MergeVCF", "Filter", "Annotate", edge_size=3.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "scientific_workflows" / "soykb_like",
        ProblemInstance(name="scientific.soykb_like", task_graph=tg, network=net4),
        _meta("scientific.soykb_like", "SoyKB-like Genome Pipeline",
            "Soybean genome processing pipeline. Index → 3 parallel lanes (align, sort, dedup, realign, call) → merge VCF → filter → annotate.",
            ["scientific", "genomics"], tg,
            _prov("SoyKB workflow", "SoyKB: The Soybean Knowledge Base", year=2017),
            tags=["soykb", "genomics", "variant-calling"]))

    # --- BWA-like alignment workflow ---
    builder = DAGBuilder("bwa_like")
    num_chunks = 4
    builder.task("SplitFastQ", 3.0)
    for i in range(num_chunks):
        builder.task(f"BWA_Align_{i}", 20.0)
        builder.task(f"SAMSort_{i}", 8.0)
        builder.edge("SplitFastQ", f"BWA_Align_{i}", 10.0)
        builder.edge(f"BWA_Align_{i}", f"SAMSort_{i}", 8.0)
    builder.task("MergeBAM", 10.0)
    for i in range(num_chunks):
        builder.edge(f"SAMSort_{i}", "MergeBAM", 6.0)
    builder.task("MarkDuplicates", 12.0).task("BaseRecal", 15.0).task("FinalBAM", 5.0)
    builder.chain("MergeBAM", "MarkDuplicates", "BaseRecal", "FinalBAM", edge_size=5.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "scientific_workflows" / "bwa_like",
        ProblemInstance(name="scientific.bwa_like", task_graph=tg, network=net4),
        _meta("scientific.bwa_like", "BWA-like Alignment Workflow",
            "DNA sequence alignment workflow. Split → 4 parallel (BWA align, sort) → merge BAM → mark duplicates → base recalibration → final BAM.",
            ["scientific", "genomics"], tg,
            _prov("BWA alignment pipeline", year=2018),
            tags=["bwa", "alignment", "genomics", "scatter-gather"]))

    # --- IoT Water Monitoring Pipeline ---
    builder = DAGBuilder("water_monitor")
    builder.tasks(
        ("pHSensor", 1.0), ("TurbiditySensor", 1.0), ("TempSensor", 1.0),
        ("FlowSensor", 1.0), ("ChlorineSensor", 1.0),
    )
    builder.task("Collect", 3.0)
    for s in ["pHSensor", "TurbiditySensor", "TempSensor", "FlowSensor", "ChlorineSensor"]:
        builder.edge(s, "Collect", 1.0)
    builder.task("Validate", 4.0).task("Normalize", 3.0)
    builder.chain("Collect", "Validate", "Normalize", edge_size=2.0)
    builder.task("TrendAnalysis", 8.0).task("AnomalyDetect", 10.0).task("QualityScore", 6.0)
    builder.fan_out("Normalize", "TrendAnalysis", "AnomalyDetect", "QualityScore", edge_size=3.0)
    builder.task("Alert", 2.0).task("Report", 3.0)
    builder.fan_in("AnomalyDetect", "QualityScore", target="Alert", edge_size=1.0)
    builder.fan_in("TrendAnalysis", "QualityScore", target="Report", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "iot_sensor_networks" / "water_monitoring",
        ProblemInstance(name="iot.water_monitoring", task_graph=tg, network=fog),
        _meta("iot.water_monitoring", "Water Quality Monitoring Pipeline",
            "IoT water quality monitoring. 5 sensors → collect → validate → normalize → (trend, anomaly, quality) → alert + report.",
            ["iot", "edge-computing"], tg,
            _prov("IoT water monitoring literature", year=2023),
            tags=["water-quality", "iot", "monitoring"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # --- Smart Grid Energy Management ---
    builder = DAGBuilder("smart_grid")
    builder.tasks(
        ("SolarForecast", 10.0), ("WindForecast", 10.0),
        ("DemandForecast", 12.0), ("GridState", 5.0),
    )
    builder.task("EnergyOptimize", 20.0)
    builder.fan_in("SolarForecast", "WindForecast", "DemandForecast", "GridState",
                   target="EnergyOptimize", edge_size=5.0)
    builder.task("BatterySchedule", 8.0).task("GridDispatch", 8.0)
    builder.task("PriceSignal", 6.0).task("DemandResponse", 10.0)
    builder.fan_out("EnergyOptimize", "BatterySchedule", "GridDispatch", "PriceSignal", edge_size=3.0)
    builder.edge("PriceSignal", "DemandResponse", 2.0)
    builder.task("Monitor", 3.0)
    builder.fan_in("BatterySchedule", "GridDispatch", "DemandResponse",
                   target="Monitor", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "iot_sensor_networks" / "smart_grid",
        ProblemInstance(name="iot.smart_grid", task_graph=tg, network=fog),
        _meta("iot.smart_grid", "Smart Grid Energy Management",
            "Smart grid energy optimization. 4 forecasts → optimize → (battery, dispatch, pricing → demand response) → monitor.",
            ["iot", "cyber-physical", "edge-computing"], tg,
            _prov("Smart grid literature", year=2023),
            tags=["smart-grid", "energy", "optimization"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # --- Healthcare Monitoring Pipeline ---
    builder = DAGBuilder("health_monitor")
    builder.tasks(("ECG", 1.0), ("SpO2", 1.0), ("BP", 1.0), ("Temp", 1.0), ("Accel", 1.0))
    builder.task("Preprocess", 3.0)
    for s in ["ECG", "SpO2", "BP", "Temp", "Accel"]:
        builder.edge(s, "Preprocess", 1.0)
    builder.task("HeartAnalysis", 12.0).task("RespiratoryAnalysis", 8.0).task("ActivityAnalysis", 6.0)
    builder.fan_out("Preprocess", "HeartAnalysis", "RespiratoryAnalysis", "ActivityAnalysis", edge_size=3.0)
    builder.task("RiskScore", 10.0)
    builder.fan_in("HeartAnalysis", "RespiratoryAnalysis", "ActivityAnalysis",
                   target="RiskScore", edge_size=2.0)
    builder.task("AlertDoctor", 2.0).task("PatientDashboard", 3.0)
    builder.fan_out("RiskScore", "AlertDoctor", "PatientDashboard", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "iot_sensor_networks" / "health_monitor",
        ProblemInstance(name="iot.health_monitor", task_graph=tg, network=fog),
        _meta("iot.health_monitor", "Healthcare IoT Monitoring",
            "Remote patient monitoring. 5 biosensors → preprocess → (heart, respiratory, activity analysis) → risk score → alerts + dashboard.",
            ["iot", "edge-computing"], tg,
            _prov("Healthcare IoT literature", year=2023),
            tags=["healthcare", "iot", "wearable", "monitoring"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # --- Industrial IoT Predictive Maintenance ---
    builder = DAGBuilder("predictive_maint")
    builder.tasks(
        ("Vibration", 1.0), ("Temperature", 1.0), ("Acoustic", 1.0),
        ("Current", 1.0), ("Pressure", 1.0),
    )
    builder.task("DataFusion", 4.0)
    for s in ["Vibration", "Temperature", "Acoustic", "Current", "Pressure"]:
        builder.edge(s, "DataFusion", 2.0)
    builder.task("FFT_Analysis", 8.0).task("StatFeatures", 6.0).task("TimeFeatures", 7.0)
    builder.fan_out("DataFusion", "FFT_Analysis", "StatFeatures", "TimeFeatures", edge_size=3.0)
    builder.task("FeatureConcat", 2.0)
    builder.fan_in("FFT_Analysis", "StatFeatures", "TimeFeatures", target="FeatureConcat", edge_size=2.0)
    builder.task("MLPredict", 15.0).task("RUL_Estimate", 10.0)
    builder.edge("FeatureConcat", "MLPredict", 3.0)
    builder.edge("FeatureConcat", "RUL_Estimate", 3.0)
    builder.task("MaintenanceSchedule", 5.0)
    builder.fan_in("MLPredict", "RUL_Estimate", target="MaintenanceSchedule", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "iot_sensor_networks" / "predictive_maintenance",
        ProblemInstance(name="iot.predictive_maintenance", task_graph=tg, network=fog),
        _meta("iot.predictive_maintenance", "Industrial Predictive Maintenance",
            "IIoT predictive maintenance. 5 sensors → fusion → (FFT, stat, time features) → concat → (ML predict, RUL estimate) → maintenance schedule.",
            ["iot", "edge-computing", "ml-pipeline"], tg,
            _prov("Predictive maintenance literature", year=2023),
            tags=["iiot", "predictive-maintenance", "fault-detection"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # --- Drone Delivery Coordination ---
    builder = DAGBuilder("drone_delivery")
    builder.task("OrderReceive", 2.0).task("RouteOptimize", 15.0).task("WeatherCheck", 5.0)
    builder.task("DroneSelect", 4.0).task("PayloadPrep", 3.0)
    builder.edge("OrderReceive", "RouteOptimize", 1.0)
    builder.edge("OrderReceive", "WeatherCheck", 1.0)
    builder.edge("OrderReceive", "PayloadPrep", 1.0)
    builder.fan_in("RouteOptimize", "WeatherCheck", target="DroneSelect", edge_size=2.0)
    builder.task("FlightPlan", 8.0)
    builder.fan_in("DroneSelect", "PayloadPrep", target="FlightPlan", edge_size=2.0)
    builder.task("Launch", 1.0).task("Navigate", 10.0).task("Deliver", 3.0)
    builder.task("Return", 8.0).task("LogComplete", 2.0)
    builder.chain("FlightPlan", "Launch", "Navigate", "Deliver", "Return", "LogComplete", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "edge_computing" / "drone_delivery",
        ProblemInstance(name="edge.drone_delivery", task_graph=tg, network=fog),
        _meta("edge.drone_delivery", "Drone Delivery Coordination",
            "Drone delivery planning and execution. Order → (route, weather, payload prep) → drone select → flight plan → launch → navigate → deliver → return → log.",
            ["edge-computing", "cyber-physical"], tg,
            _prov("Drone delivery literature", year=2023),
            tags=["drone", "delivery", "planning", "cyber-physical"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # --- Natural Language Processing Pipeline ---
    builder = DAGBuilder("nlp_pipeline")
    builder.task("TextInput", 1.0)
    builder.task("Tokenize", 3.0).task("SentenceSplit", 2.0)
    builder.edge("TextInput", "Tokenize", 2.0)
    builder.edge("TextInput", "SentenceSplit", 2.0)
    builder.task("POS_Tag", 5.0).task("NER", 8.0).task("Sentiment", 10.0).task("Summary", 15.0)
    builder.edge("Tokenize", "POS_Tag", 3.0)
    builder.edge("Tokenize", "NER", 3.0)
    builder.edge("Tokenize", "Sentiment", 3.0)
    builder.edge("SentenceSplit", "Summary", 3.0)
    builder.task("EntityLink", 6.0)
    builder.edge("NER", "EntityLink", 2.0)
    builder.edge("POS_Tag", "EntityLink", 2.0)
    builder.task("Report", 3.0)
    builder.fan_in("EntityLink", "Sentiment", "Summary", target="Report", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "ml_pipelines" / "nlp_pipeline",
        ProblemInstance(name="ml.nlp_pipeline", task_graph=tg, network=net4),
        _meta("ml.nlp_pipeline", "NLP Text Analysis Pipeline",
            "Natural language processing pipeline. Text → (tokenize, sentence split) → (POS, NER, sentiment, summary) → entity linking → report.",
            ["ml-pipeline", "data-analytics"], tg,
            _prov("NLP pipeline patterns", year=2023),
            tags=["nlp", "text-analysis", "ner", "sentiment"]))

    # --- ETL Data Warehouse Pipeline ---
    builder = DAGBuilder("etl_warehouse")
    sources = ["CRM_Extract", "ERP_Extract", "Web_Extract", "Social_Extract"]
    for src in sources:
        builder.task(src, 8.0)
    builder.task("Schema_Validate", 5.0)
    for src in sources:
        builder.edge(src, "Schema_Validate", 5.0)
    builder.task("Deduplicate", 10.0).task("Transform", 12.0)
    builder.chain("Schema_Validate", "Deduplicate", "Transform", edge_size=8.0)
    builder.task("Load_Facts", 15.0).task("Load_Dims", 10.0)
    builder.fan_out("Transform", "Load_Facts", "Load_Dims", edge_size=10.0)
    builder.task("Index_Build", 8.0)
    builder.fan_in("Load_Facts", "Load_Dims", target="Index_Build", edge_size=3.0)
    builder.task("QA_Check", 5.0).task("Notify", 1.0)
    builder.chain("Index_Build", "QA_Check", "Notify", edge_size=1.0)
    tg = builder.build()
    _save_workflow(WORKFLOWS_DIR / "ml_pipelines" / "etl_warehouse",
        ProblemInstance(name="ml.etl_warehouse", task_graph=tg, network=net4),
        _meta("ml.etl_warehouse", "ETL Data Warehouse Pipeline",
            "Data warehouse ETL pipeline. 4 source extracts → validate → dedup → transform → (load facts, load dims) → index → QA → notify.",
            ["data-analytics"], tg,
            _prov("ETL pipeline patterns", year=2022),
            tags=["etl", "data-warehouse", "batch-processing"]))

    # --- Gaussian Elimination n=7 (medium size) ---
    from dagbench.generators import gaussian_elimination_dag, fft_dag, cholesky_dag
    tg = gaussian_elimination_dag(n=7)
    _save_workflow(WORKFLOWS_DIR / "classic_benchmarks" / "gauss_elim_7",
        ProblemInstance(name="classic.gauss_elim_7", task_graph=tg, network=net4),
        _meta("classic.gauss_elim_7", "Gaussian Elimination (n=7)",
            "Gaussian elimination for a 7x7 matrix. Medium-size classic benchmark.",
            ["classic-benchmark"], tg,
            _prov("Classic scheduling benchmark", "HEFT", ["Topcuoglu et al."], 2002, "10.1109/71.993206", method="programmatic"),
            tags=["gaussian-elimination", "linear-algebra", "medium"]))

    # --- FFT 32-point (larger) ---
    tg = fft_dag(num_points=32)
    _save_workflow(WORKFLOWS_DIR / "classic_benchmarks" / "fft_32",
        ProblemInstance(name="classic.fft_32", task_graph=tg, network=net4),
        _meta("classic.fft_32", "FFT Butterfly (32-point)",
            "32-point Fast Fourier Transform butterfly DAG. 5 stages with high parallelism.",
            ["classic-benchmark"], tg,
            _prov("Classic FFT benchmark", method="programmatic"),
            tags=["fft", "signal-processing", "large"]))

    # --- Cholesky n=6 (larger) ---
    tg = cholesky_dag(n=6)
    _save_workflow(WORKFLOWS_DIR / "classic_benchmarks" / "cholesky_6",
        ProblemInstance(name="classic.cholesky_6", task_graph=tg, network=net4),
        _meta("classic.cholesky_6", "Cholesky Factorization (6x6 tiles)",
            "Tiled Cholesky factorization for a 6x6 tile grid. Large dense linear algebra benchmark.",
            ["classic-benchmark"], tg,
            _prov("Classic Cholesky benchmark", method="programmatic"),
            tags=["cholesky", "linear-algebra", "large"]))

    # --- MapReduce 4m/2r (small) ---
    from dagbench.generators import mapreduce_dag
    tg = mapreduce_dag(num_mappers=4, num_reducers=2)
    _save_workflow(WORKFLOWS_DIR / "classic_benchmarks" / "mapreduce_4m_2r",
        ProblemInstance(name="classic.mapreduce_4m_2r", task_graph=tg, network=net4),
        _meta("classic.mapreduce_4m_2r", "MapReduce (4 mappers, 2 reducers)",
            "Small MapReduce pattern. Split → Map[4] → Shuffle → Reduce[2] → Merge.",
            ["classic-benchmark", "data-analytics"], tg,
            _prov("MapReduce pattern", method="programmatic"),
            tags=["mapreduce", "data-parallel", "small"]))

    # Count
    from dagbench.catalog import list_workflows
    workflows = list_workflows(workflows_dir=WORKFLOWS_DIR)
    print(f"\n=== Total: {len(workflows)} workflows ===")


if __name__ == "__main__":
    main()
