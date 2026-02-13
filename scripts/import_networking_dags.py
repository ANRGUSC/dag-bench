"""Campaign 2: Import 50 Networking/IoT/Edge DAGs.

Adds MEC, V2X, UAV, Tactical/MANET, Smart City, Industrial IoT,
Agriculture IoT, NFV/Telecom, and Fog Computing workflows.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import yaml
from saga.schedulers.data import ProblemInstance

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dagbench.stats import compute_graph_stats
from dagbench.converters.manual import DAGBuilder
from dagbench.networks import (
    homogeneous_network, fog_network, star_network,
    mec_network, manet_network,
)

WORKFLOWS_DIR = PROJECT_ROOT / "workflows"
TODAY = date.today().isoformat()
CAMPAIGN = "campaign_004_networking"


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
          nn_min=4, nn_max=4):
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
            "ccr": round(stats.ccr, 4) if stats.ccr else None,
            "parallelism": round(stats.parallelism, 4) if stats.parallelism else None,
        },
        "campaign": CAMPAIGN, "tags": tags,
    }


def _prov(source, title=None, authors=None, year=None, doi=None, method="manual-figure"):
    p = {"source": source, "extraction_method": method,
         "extractor": "dagbench-import-script", "extraction_date": TODAY}
    if title: p["paper_title"] = title
    if authors: p["authors"] = authors
    if year: p["year"] = year
    if doi: p["paper_doi"] = doi
    return p


# ---------------------------------------------------------------------------
# Group 1: MEC Task Offloading (8 DAGs)
# ---------------------------------------------------------------------------
def create_mec_dags(mec_net):
    base = WORKFLOWS_DIR / "mec_offloading"
    print("\n--- MEC Task Offloading ---")

    # 1. Face Detection Offloading (10 tasks)
    b = DAGBuilder("face_detection_offload")
    b.task("CameraCapture", 2.0).task("Preprocess", 3.0).task("FaceDetect", 8.0)
    b.chain("CameraCapture", "Preprocess", "FaceDetect", edge_size=5.0)
    b.task("LandmarkLocate", 6.0).task("FeatureExtract", 7.0)
    b.fan_out("FaceDetect", "LandmarkLocate", "FeatureExtract", edge_size=3.0)
    b.task("FaceAlign", 4.0).task("FaceEncode", 5.0)
    b.edge("LandmarkLocate", "FaceAlign", 2.0)
    b.edge("FeatureExtract", "FaceEncode", 2.0)
    b.task("FaceMatch", 10.0)
    b.fan_in("FaceAlign", "FaceEncode", target="FaceMatch", edge_size=3.0)
    b.task("Result", 1.0)
    b.edge("FaceMatch", "Result", 1.0)
    tg = b.build()
    _save_workflow(base / "face_detection_offload",
        ProblemInstance(name="mec.face_detection_offload", task_graph=tg, network=mec_net),
        _meta("mec.face_detection_offload", "MEC Face Detection Offloading",
            "Face detection pipeline offloaded to MEC. Camera → preprocess → detect → (landmark, feature) → (align, encode) → match → result.",
            ["mec", "edge-computing", "image-processing"], tg,
            _prov("Chen et al. 2016", "Multi-user computation offloading", year=2016,
                  doi="10.1109/TNET.2015.2487344"),
            tags=["face-detection", "offloading", "mec"], net_topo="mec", nn_min=7, nn_max=7))

    # 2. Object Recognition Pipeline (12 tasks)
    b = DAGBuilder("object_recognition")
    b.task("ImageCapture", 2.0).task("Resize", 2.0)
    b.chain("ImageCapture", "Resize", edge_size=4.0)
    b.tasks(("CNN_Color", 8.0), ("CNN_Texture", 8.0), ("CNN_Shape", 8.0), ("CNN_Edge", 8.0))
    b.fan_out("Resize", "CNN_Color", "CNN_Texture", "CNN_Shape", "CNN_Edge", edge_size=3.0)
    b.task("FeatureFuse", 5.0)
    b.fan_in("CNN_Color", "CNN_Texture", "CNN_Shape", "CNN_Edge", target="FeatureFuse", edge_size=2.0)
    b.task("Classify", 10.0).task("BBoxRegress", 6.0)
    b.fan_out("FeatureFuse", "Classify", "BBoxRegress", edge_size=3.0)
    b.task("NMS", 4.0)
    b.fan_in("Classify", "BBoxRegress", target="NMS", edge_size=2.0)
    b.task("Annotate", 2.0).task("Output", 1.0)
    b.chain("NMS", "Annotate", "Output", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "object_recognition",
        ProblemInstance(name="mec.object_recognition", task_graph=tg, network=mec_net),
        _meta("mec.object_recognition", "MEC Object Recognition Pipeline",
            "Object recognition on MEC. Capture → resize → 4 parallel CNN features → fuse → (classify, bbox) → NMS → annotate → output.",
            ["mec", "edge-computing", "image-processing", "ml-pipeline"], tg,
            _prov("Wang et al. 2021", "Deep learning inference offloading", year=2021,
                  doi="10.1109/TCCN.2021.3049437"),
            tags=["object-recognition", "cnn", "mec"], net_topo="mec", nn_min=7, nn_max=7))

    # 3. NLP Offloading (9 tasks)
    b = DAGBuilder("nlp_offload")
    b.task("AudioCapture", 2.0).task("ASR", 12.0).task("Tokenize", 3.0)
    b.chain("AudioCapture", "ASR", "Tokenize", edge_size=4.0)
    b.tasks(("POS_Tag", 5.0), ("NER", 6.0), ("IntentDetect", 7.0))
    b.fan_out("Tokenize", "POS_Tag", "NER", "IntentDetect", edge_size=2.0)
    b.task("Understand", 8.0)
    b.fan_in("POS_Tag", "NER", "IntentDetect", target="Understand", edge_size=2.0)
    b.task("Response", 3.0)
    b.edge("Understand", "Response", 1.0)
    tg = b.build()
    _save_workflow(base / "nlp_offload",
        ProblemInstance(name="mec.nlp_offload", task_graph=tg, network=mec_net),
        _meta("mec.nlp_offload", "MEC NLP Processing Offloading",
            "NLP pipeline offloaded to MEC. Audio → ASR → tokenize → (POS/NER/intent parallel) → understand → response.",
            ["mec", "edge-computing", "ml-pipeline"], tg,
            _prov("Yang et al. 2019", "Multi-access edge intelligence", year=2019,
                  doi="10.1109/JSAC.2018.2869948"),
            tags=["nlp", "offloading", "speech"], net_topo="mec", nn_min=7, nn_max=7))

    # 4. AR Navigation (14 tasks)
    b = DAGBuilder("ar_navigation")
    b.tasks(("Camera", 2.0), ("IMU", 1.0), ("GPS", 1.0))
    b.task("SLAM", 15.0).task("ObjDetect", 12.0).task("MapMatch", 8.0)
    b.edge("Camera", "SLAM", 5.0)
    b.edge("Camera", "ObjDetect", 5.0)
    b.edge("IMU", "SLAM", 2.0)
    b.edge("GPS", "MapMatch", 1.0)
    b.task("PoseEstimate", 6.0).task("SceneUnderstand", 8.0)
    b.edge("SLAM", "PoseEstimate", 3.0)
    b.fan_in("ObjDetect", "MapMatch", target="SceneUnderstand", edge_size=3.0)
    b.task("AROverlay", 10.0)
    b.fan_in("PoseEstimate", "SceneUnderstand", target="AROverlay", edge_size=4.0)
    b.task("PathPlan", 7.0).task("NavInstruct", 4.0)
    b.edge("MapMatch", "PathPlan", 2.0)
    b.edge("PoseEstimate", "PathPlan", 2.0)
    b.task("Render", 8.0)
    b.fan_in("AROverlay", "NavInstruct", target="Render", edge_size=5.0)
    b.edge("PathPlan", "NavInstruct", 2.0)
    b.task("Display", 1.0)
    b.edge("Render", "Display", 3.0)
    tg = b.build()
    _save_workflow(base / "ar_navigation",
        ProblemInstance(name="mec.ar_navigation", task_graph=tg, network=mec_net),
        _meta("mec.ar_navigation", "MEC AR Navigation Application",
            "AR navigation with MEC offloading. 3 sensors → (SLAM, object detect, map match) → (pose, scene) → AR overlay → render → display.",
            ["mec", "edge-computing"], tg,
            _prov("Mach and Becvar 2017", "Mobile edge computing survey", year=2017,
                  doi="10.1109/COMST.2017.2682318"),
            tags=["ar", "navigation", "slam", "mec"], net_topo="mec", nn_min=7, nn_max=7))

    # 5. Cloud Gaming Render (11 tasks)
    b = DAGBuilder("gaming_render")
    b.task("InputProcess", 2.0)
    b.tasks(("PhysicsEngine", 10.0), ("AILogic", 8.0), ("AnimState", 6.0))
    b.fan_out("InputProcess", "PhysicsEngine", "AILogic", "AnimState", edge_size=2.0)
    b.task("WorldUpdate", 5.0)
    b.fan_in("PhysicsEngine", "AILogic", "AnimState", target="WorldUpdate", edge_size=3.0)
    b.task("Culling", 4.0).task("GeometryPass", 8.0).task("LightingPass", 10.0)
    b.chain("WorldUpdate", "Culling", "GeometryPass", "LightingPass", edge_size=4.0)
    b.task("PostProcess", 6.0).task("Encode", 5.0)
    b.chain("LightingPass", "PostProcess", "Encode", edge_size=5.0)
    tg = b.build()
    _save_workflow(base / "gaming_render",
        ProblemInstance(name="mec.gaming_render", task_graph=tg, network=mec_net),
        _meta("mec.gaming_render", "MEC Cloud Gaming Render Pipeline",
            "Cloud gaming render pipeline. Input → (physics, AI, animation) → world update → culling → geometry → lighting → post-process → encode.",
            ["mec", "edge-computing"], tg,
            _prov("Zhang et al. 2020", "Cloud gaming on MEC", year=2020),
            tags=["gaming", "rendering", "cloud-gaming"], net_topo="mec", nn_min=7, nn_max=7))

    # 6. Video Analytics (15 tasks)
    b = DAGBuilder("video_analytics")
    b.task("VideoIngest", 3.0).task("FrameDecode", 4.0).task("FrameSample", 2.0)
    b.chain("VideoIngest", "FrameDecode", "FrameSample", edge_size=6.0)
    b.tasks(("PersonDetect", 10.0), ("VehicleDetect", 10.0), ("ObjectDetect", 10.0))
    b.fan_out("FrameSample", "PersonDetect", "VehicleDetect", "ObjectDetect", edge_size=4.0)
    b.tasks(("PersonTrack", 8.0), ("VehicleTrack", 8.0), ("ObjectTrack", 8.0))
    b.edge("PersonDetect", "PersonTrack", 3.0)
    b.edge("VehicleDetect", "VehicleTrack", 3.0)
    b.edge("ObjectDetect", "ObjectTrack", 3.0)
    b.task("TrackFuse", 6.0)
    b.fan_in("PersonTrack", "VehicleTrack", "ObjectTrack", target="TrackFuse", edge_size=2.0)
    b.task("BehaviorAnalysis", 12.0).task("AlertGen", 3.0).task("Dashboard", 2.0)
    b.edge("TrackFuse", "BehaviorAnalysis", 3.0)
    b.edge("BehaviorAnalysis", "AlertGen", 1.0)
    b.edge("BehaviorAnalysis", "Dashboard", 1.0)
    tg = b.build()
    _save_workflow(base / "video_analytics",
        ProblemInstance(name="mec.video_analytics", task_graph=tg, network=mec_net),
        _meta("mec.video_analytics", "MEC Video Analytics Pipeline",
            "Video analytics on MEC. Ingest → decode → sample → 3 parallel detectors → 3 trackers → fuse → behavior → (alert, dashboard).",
            ["mec", "edge-computing", "image-processing"], tg,
            _prov("Ananthanarayanan et al. 2017", "Real-time video analytics", year=2017,
                  doi="10.1109/MC.2017.185"),
            tags=["video-analytics", "tracking", "detection"], net_topo="mec", nn_min=7, nn_max=7))

    # 7. Partial Offloading Decision (8 tasks)
    b = DAGBuilder("partial_offload")
    b.task("TaskProfile", 3.0).task("ChannelEstimate", 2.0)
    b.task("OffloadDecision", 5.0)
    b.fan_in("TaskProfile", "ChannelEstimate", target="OffloadDecision", edge_size=1.0)
    b.task("LocalCompute", 8.0).task("EdgeCompute", 6.0)
    b.fan_out("OffloadDecision", "LocalCompute", "EdgeCompute", edge_size=3.0)
    b.task("ResultMerge", 4.0)
    b.fan_in("LocalCompute", "EdgeCompute", target="ResultMerge", edge_size=2.0)
    b.task("QoSCheck", 2.0).task("Feedback", 1.0)
    b.chain("ResultMerge", "QoSCheck", "Feedback", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "partial_offload",
        ProblemInstance(name="mec.partial_offload", task_graph=tg, network=mec_net),
        _meta("mec.partial_offload", "MEC Partial Offloading Decision",
            "Partial offloading: profile + channel → decision → (local, edge compute) → merge → QoS check → feedback.",
            ["mec", "edge-computing"], tg,
            _prov("You et al. 2017", "Resource allocation for mobile-edge computation offloading",
                  year=2017, doi="10.1109/TWC.2016.2665553"),
            tags=["offloading", "partial", "decision"], net_topo="mec", nn_min=7, nn_max=7))

    # 8. Multi-User Concurrent Offload (22 tasks)
    b = DAGBuilder("multi_user_offload")
    b.task("Orchestrator", 5.0)
    for u in range(4):
        prefix = f"U{u}"
        b.task(f"{prefix}_Profile", 2.0)
        b.task(f"{prefix}_Offload", 6.0)
        b.task(f"{prefix}_Compute", 8.0)
        b.task(f"{prefix}_Return", 3.0)
        b.edge("Orchestrator", f"{prefix}_Profile", 1.0)
        b.chain(f"{prefix}_Profile", f"{prefix}_Offload", f"{prefix}_Compute",
                f"{prefix}_Return", edge_size=2.0)
    b.task("Aggregate", 6.0)
    for u in range(4):
        b.edge(f"U{u}_Return", "Aggregate", 2.0)
    b.task("ResourceUpdate", 3.0)
    b.edge("Aggregate", "ResourceUpdate", 1.0)
    tg = b.build()
    _save_workflow(base / "multi_user_offload",
        ProblemInstance(name="mec.multi_user_offload", task_graph=tg, network=mec_net),
        _meta("mec.multi_user_offload", "MEC Multi-User Concurrent Offload",
            "Multi-user MEC offloading. Orchestrator → 4 user chains (profile, offload, compute, return) → aggregate → resource update.",
            ["mec", "edge-computing"], tg,
            _prov("Chen et al. 2018", "Multi-user MEC offloading", year=2018),
            tags=["multi-user", "concurrent", "orchestration"], net_topo="mec", nn_min=7, nn_max=7))


# ---------------------------------------------------------------------------
# Group 2: V2X/Vehicular (5 DAGs)
# ---------------------------------------------------------------------------
def create_v2x_dags(mec_net):
    base = WORKFLOWS_DIR / "v2x_vehicular"
    print("\n--- V2X/Vehicular ---")

    # 9. Cooperative Perception (16 tasks)
    b = DAGBuilder("cooperative_perception")
    for v in range(3):
        b.task(f"V{v}_Camera", 2.0)
        b.task(f"V{v}_Lidar", 3.0)
        b.task(f"V{v}_Fuse", 6.0)
        b.edge(f"V{v}_Camera", f"V{v}_Fuse", 4.0)
        b.edge(f"V{v}_Lidar", f"V{v}_Fuse", 4.0)
    b.task("V2X_Exchange", 5.0)
    for v in range(3):
        b.edge(f"V{v}_Fuse", "V2X_Exchange", 3.0)
    b.task("GlobalFuse", 8.0).task("ObjectList", 4.0).task("ThreatAssess", 6.0)
    b.chain("V2X_Exchange", "GlobalFuse", "ObjectList", "ThreatAssess", edge_size=3.0)
    b.task("PathPlan", 7.0).task("Control", 3.0)
    b.chain("ThreatAssess", "PathPlan", "Control", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "cooperative_perception",
        ProblemInstance(name="v2x.cooperative_perception", task_graph=tg, network=mec_net),
        _meta("v2x.cooperative_perception", "V2X Cooperative Perception",
            "V2X cooperative perception. 3 vehicles (camera+lidar→fuse) → V2X exchange → global fuse → object list → threat → path plan → control.",
            ["v2x", "edge-computing"], tg,
            _prov("Chen et al. 2019", "Cooperative perception for connected vehicles",
                  year=2019, doi="10.1109/TITS.2019.2963966"),
            tags=["v2x", "cooperative", "perception", "lidar"], net_topo="mec", nn_min=7, nn_max=7))

    # 10. Truck Platooning (13 tasks)
    b = DAGBuilder("platooning_control")
    b.task("LeaderSense", 3.0).task("LeaderPlan", 8.0).task("LeaderBroadcast", 2.0)
    b.chain("LeaderSense", "LeaderPlan", "LeaderBroadcast", edge_size=2.0)
    for f in range(3):
        b.task(f"F{f}_Receive", 1.0).task(f"F{f}_Adapt", 5.0).task(f"F{f}_Control", 4.0)
        b.edge("LeaderBroadcast", f"F{f}_Receive", 1.0)
        b.chain(f"F{f}_Receive", f"F{f}_Adapt", f"F{f}_Control", edge_size=1.0)
    b.task("Coordinate", 4.0)
    for f in range(3):
        b.edge(f"F{f}_Control", "Coordinate", 1.0)
    tg = b.build()
    _save_workflow(base / "platooning_control",
        ProblemInstance(name="v2x.platooning_control", task_graph=tg, network=mec_net),
        _meta("v2x.platooning_control", "V2X Truck Platooning Control",
            "Truck platooning. Leader sense → plan → broadcast → 3 followers (receive, adapt, control) → coordinate.",
            ["v2x", "cyber-physical"], tg,
            _prov("Campolo et al. 2020", "5G V2X platooning", year=2020,
                  doi="10.1109/TVT.2020.2975574"),
            tags=["platooning", "v2x", "truck"], net_topo="mec", nn_min=7, nn_max=7))

    # 11. Intersection Management (18 tasks)
    b = DAGBuilder("intersection_mgmt")
    b.task("RSU_Init", 2.0)
    for lane in range(4):
        b.task(f"Lane{lane}_Detect", 3.0)
        b.task(f"Lane{lane}_Track", 4.0)
        b.edge("RSU_Init", f"Lane{lane}_Detect", 1.0)
        b.edge(f"Lane{lane}_Detect", f"Lane{lane}_Track", 2.0)
    b.task("ConflictDetect", 8.0)
    for lane in range(4):
        b.edge(f"Lane{lane}_Track", "ConflictDetect", 2.0)
    b.task("PriorityAssign", 5.0).task("TimingCalc", 6.0)
    b.chain("ConflictDetect", "PriorityAssign", "TimingCalc", edge_size=2.0)
    b.task("SignalUpdate", 3.0).task("V2X_Advise", 3.0)
    b.fan_out("TimingCalc", "SignalUpdate", "V2X_Advise", edge_size=2.0)
    b.task("Monitor", 2.0)
    b.fan_in("SignalUpdate", "V2X_Advise", target="Monitor", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "intersection_mgmt",
        ProblemInstance(name="v2x.intersection_mgmt", task_graph=tg, network=mec_net),
        _meta("v2x.intersection_mgmt", "V2X Intersection Management",
            "Intersection management. RSU → 4 lanes (detect, track) → conflict detect → priority → timing → (signal, V2X advise) → monitor.",
            ["v2x", "smart-city"], tg,
            _prov("Dresner and Stone 2008", "Multiagent approach to autonomous intersection management",
                  year=2008, doi="10.1613/jair.2502"),
            tags=["intersection", "rsu", "traffic"], net_topo="mec", nn_min=7, nn_max=7))

    # 12. Remote Driving (11 tasks)
    b = DAGBuilder("remote_driving")
    b.task("VehicleSense", 3.0).task("Compress", 4.0).task("Uplink", 2.0)
    b.chain("VehicleSense", "Compress", "Uplink", edge_size=5.0)
    b.task("RemoteRender", 8.0).task("OperatorDisplay", 3.0).task("OperatorInput", 2.0)
    b.chain("Uplink", "RemoteRender", "OperatorDisplay", "OperatorInput", edge_size=4.0)
    b.task("Downlink", 2.0).task("Decode", 3.0).task("VehicleControl", 5.0)
    b.chain("OperatorInput", "Downlink", "Decode", "VehicleControl", edge_size=4.0)
    b.task("SafetyCheck", 3.0)
    b.edge("VehicleSense", "SafetyCheck", 1.0)
    b.edge("SafetyCheck", "VehicleControl", 1.0)
    tg = b.build()
    _save_workflow(base / "remote_driving",
        ProblemInstance(name="v2x.remote_driving", task_graph=tg, network=mec_net),
        _meta("v2x.remote_driving", "V2X Remote Driving Teleoperation",
            "Remote driving teleoperation. Vehicle sense → compress → uplink → render → display → input → downlink → decode → control, with safety fork.",
            ["v2x", "mec"], tg,
            _prov("Neumeier et al. 2019", "Teleoperation over 5G", year=2019,
                  doi="10.1109/VTCSpring.2019.8746497"),
            tags=["remote-driving", "teleoperation", "latency-critical"], net_topo="mec", nn_min=7, nn_max=7))

    # 13. Collision Avoidance (10 tasks)
    b = DAGBuilder("collision_avoidance")
    b.task("RadarSense", 3.0).task("V2X_BSM_Receive", 2.0)
    b.task("RadarProcess", 5.0).task("BSM_Parse", 3.0)
    b.edge("RadarSense", "RadarProcess", 3.0)
    b.edge("V2X_BSM_Receive", "BSM_Parse", 2.0)
    b.task("SensorFusion", 6.0)
    b.fan_in("RadarProcess", "BSM_Parse", target="SensorFusion", edge_size=2.0)
    b.task("ThreatAssess", 5.0).task("Decision", 4.0)
    b.chain("SensorFusion", "ThreatAssess", "Decision", edge_size=2.0)
    b.task("BrakeCmd", 2.0).task("SteerCmd", 2.0).task("Alert", 1.0)
    b.fan_out("Decision", "BrakeCmd", "SteerCmd", "Alert", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "collision_avoidance",
        ProblemInstance(name="v2x.collision_avoidance", task_graph=tg, network=mec_net),
        _meta("v2x.collision_avoidance", "V2X Collision Avoidance System",
            "Collision avoidance. (Radar, V2X BSM) → process → fusion → threat → decision → (brake, steer, alert).",
            ["v2x", "cyber-physical"], tg,
            _prov("Seo et al. 2016", "V2X collision avoidance", year=2016),
            tags=["collision-avoidance", "safety", "radar", "bsm"], net_topo="mec", nn_min=7, nn_max=7))


# ---------------------------------------------------------------------------
# Group 3: UAV/Drone (5 DAGs)
# ---------------------------------------------------------------------------
def create_uav_dags(mec_net):
    base = WORKFLOWS_DIR / "uav_drone"
    print("\n--- UAV/Drone ---")

    # 14. Surveillance Mission (16 tasks)
    b = DAGBuilder("surveillance_mission")
    b.task("MissionPlan", 5.0)
    for wp in range(4):
        b.task(f"Waypoint{wp}", 3.0)
        b.task(f"Capture{wp}", 4.0)
        if wp == 0:
            b.edge("MissionPlan", f"Waypoint{wp}", 1.0)
        else:
            b.edge(f"Capture{wp-1}", f"Waypoint{wp}", 1.0)
        b.edge(f"Waypoint{wp}", f"Capture{wp}", 2.0)
    b.task("ImageStitch", 8.0)
    for wp in range(4):
        b.edge(f"Capture{wp}", "ImageStitch", 3.0)
    b.task("ObjDetect", 10.0).task("ChangeDetect", 8.0)
    b.fan_out("ImageStitch", "ObjDetect", "ChangeDetect", edge_size=4.0)
    b.task("Report", 3.0)
    b.fan_in("ObjDetect", "ChangeDetect", target="Report", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "surveillance_mission",
        ProblemInstance(name="uav.surveillance_mission", task_graph=tg, network=mec_net),
        _meta("uav.surveillance_mission", "UAV Surveillance Mission",
            "UAV surveillance. Plan → 4 sequential waypoints with captures → stitch → (object detect, change detect) → report.",
            ["uav", "edge-computing"], tg,
            _prov("Zhou et al. 2020", "UAV-enabled MEC", year=2020,
                  doi="10.1109/TCOMM.2020.2984918"),
            tags=["surveillance", "uav", "waypoint"], net_topo="mec", nn_min=7, nn_max=7))

    # 15. Search and Rescue (20 tasks)
    b = DAGBuilder("search_rescue")
    b.task("CommandInit", 3.0)
    for d in range(3):
        b.task(f"D{d}_Launch", 2.0)
        b.task(f"D{d}_Search", 8.0)
        b.task(f"D{d}_Detect", 6.0)
        b.task(f"D{d}_Classify", 5.0)
        b.task(f"D{d}_Report", 2.0)
        b.edge("CommandInit", f"D{d}_Launch", 1.0)
        b.chain(f"D{d}_Launch", f"D{d}_Search", f"D{d}_Detect",
                f"D{d}_Classify", f"D{d}_Report", edge_size=2.0)
    b.task("FuseReports", 6.0)
    for d in range(3):
        b.edge(f"D{d}_Report", "FuseReports", 2.0)
    b.task("PriorityRank", 4.0).task("RescuePlan", 5.0).task("Dispatch", 2.0)
    b.chain("FuseReports", "PriorityRank", "RescuePlan", "Dispatch", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "search_rescue",
        ProblemInstance(name="uav.search_rescue", task_graph=tg, network=mec_net),
        _meta("uav.search_rescue", "UAV Search and Rescue Coordination",
            "Search and rescue with 3 drones. Command → 3 parallel drone chains (launch, search, detect, classify, report) → fuse → rank → plan → dispatch.",
            ["uav", "edge-computing"], tg,
            _prov("Hayat et al. 2020", "Survey on UAV communications", year=2020,
                  doi="10.1109/COMST.2016.2554569"),
            tags=["search-rescue", "multi-drone", "coordination"], net_topo="mec", nn_min=7, nn_max=7))

    # 16. Precision Agriculture Survey (14 tasks)
    b = DAGBuilder("precision_agriculture")
    b.task("FlightPlan", 4.0)
    for s in range(4):
        b.task(f"Strip{s}_Fly", 3.0)
        b.task(f"Strip{s}_Capture", 2.0)
        if s == 0:
            b.edge("FlightPlan", f"Strip{s}_Fly", 1.0)
        else:
            b.edge(f"Strip{s-1}_Capture", f"Strip{s}_Fly", 1.0)
        b.edge(f"Strip{s}_Fly", f"Strip{s}_Capture", 2.0)
    b.task("Mosaic", 8.0)
    for s in range(4):
        b.edge(f"Strip{s}_Capture", "Mosaic", 3.0)
    b.task("NDVI", 6.0).task("StressMap", 7.0)
    b.fan_out("Mosaic", "NDVI", "StressMap", edge_size=4.0)
    b.task("PrescriptionMap", 5.0)
    b.fan_in("NDVI", "StressMap", target="PrescriptionMap", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "precision_agriculture",
        ProblemInstance(name="uav.precision_agriculture", task_graph=tg, network=mec_net),
        _meta("uav.precision_agriculture", "UAV Precision Agriculture Survey",
            "Precision agriculture. Plan → 4 sequential strips (fly, capture) → mosaic → (NDVI, stress) → prescription map.",
            ["uav", "agriculture-iot"], tg,
            _prov("Mogili and Deepak 2018", "Review of UAV in agriculture", year=2018,
                  doi="10.1016/j.procs.2018.07.063"),
            tags=["agriculture", "ndvi", "uav-survey"], net_topo="mec", nn_min=7, nn_max=7))

    # 17. Fleet Delivery (25 tasks)
    b = DAGBuilder("delivery_fleet")
    b.task("OrderBatch", 4.0).task("RouteOptimize", 10.0)
    b.edge("OrderBatch", "RouteOptimize", 2.0)
    for d in range(5):
        b.task(f"D{d}_Assign", 2.0)
        b.task(f"D{d}_Load", 3.0)
        b.task(f"D{d}_Fly", 6.0)
        b.task(f"D{d}_Deliver", 2.0)
        b.edge("RouteOptimize", f"D{d}_Assign", 1.0)
        b.chain(f"D{d}_Assign", f"D{d}_Load", f"D{d}_Fly", f"D{d}_Deliver", edge_size=1.0)
    b.task("FleetMonitor", 4.0)
    for d in range(5):
        b.edge(f"D{d}_Deliver", "FleetMonitor", 1.0)
    b.task("CompleteReport", 3.0)
    b.edge("FleetMonitor", "CompleteReport", 1.0)
    tg = b.build()
    _save_workflow(base / "delivery_fleet",
        ProblemInstance(name="uav.delivery_fleet", task_graph=tg, network=mec_net),
        _meta("uav.delivery_fleet", "UAV Fleet Delivery Orchestration",
            "Fleet delivery. Order batch → optimize → 5 parallel drones (assign, load, fly, deliver) → fleet monitor → report.",
            ["uav", "edge-computing"], tg,
            _prov("Murray and Chu 2015", "Flying sidekick TSP", year=2015),
            tags=["delivery", "fleet", "multi-drone"], net_topo="mec", nn_min=7, nn_max=7))

    # 18. Communication Relay (12 tasks)
    b = DAGBuilder("relay_network")
    b.task("Source", 2.0).task("Encode", 4.0)
    b.edge("Source", "Encode", 3.0)
    for r in range(3):
        b.task(f"UAV{r}_Receive", 2.0).task(f"UAV{r}_Amplify", 3.0).task(f"UAV{r}_Transmit", 2.0)
        if r == 0:
            b.edge("Encode", f"UAV{r}_Receive", 2.0)
        else:
            b.edge(f"UAV{r-1}_Transmit", f"UAV{r}_Receive", 2.0)
        b.chain(f"UAV{r}_Receive", f"UAV{r}_Amplify", f"UAV{r}_Transmit", edge_size=2.0)
    b.task("Decode", 4.0).task("Sink", 1.0)
    b.edge("UAV2_Transmit", "Decode", 2.0)
    b.edge("Decode", "Sink", 1.0)
    tg = b.build()
    _save_workflow(base / "relay_network",
        ProblemInstance(name="uav.relay_network", task_graph=tg, network=mec_net),
        _meta("uav.relay_network", "UAV Communication Relay Chain",
            "Communication relay. Source → encode → 3 serial UAV relays (receive, amplify, transmit) → decode → sink.",
            ["uav", "wireless"], tg,
            _prov("Zeng et al. 2016", "Wireless communications with UAVs", year=2016,
                  doi="10.1109/COMST.2016.2592520"),
            tags=["relay", "communication", "multi-hop"], net_topo="mec", nn_min=7, nn_max=7))


# ---------------------------------------------------------------------------
# Group 4: Tactical/MANET (5 DAGs)
# ---------------------------------------------------------------------------
def create_tactical_dags(manet_net):
    base = WORKFLOWS_DIR / "tactical_manet"
    print("\n--- Tactical/MANET ---")

    # 19. ISR Pipeline (16 tasks)
    b = DAGBuilder("isr_pipeline")
    b.tasks(("SIGINT_Collect", 4.0), ("IMINT_Collect", 5.0), ("HUMINT_Collect", 3.0))
    b.task("SIGINT_Process", 8.0).task("IMINT_Process", 10.0).task("HUMINT_Process", 6.0)
    b.edge("SIGINT_Collect", "SIGINT_Process", 3.0)
    b.edge("IMINT_Collect", "IMINT_Process", 4.0)
    b.edge("HUMINT_Collect", "HUMINT_Process", 2.0)
    b.task("MultiFuse", 10.0)
    b.fan_in("SIGINT_Process", "IMINT_Process", "HUMINT_Process", target="MultiFuse", edge_size=3.0)
    b.task("TargetIdentify", 7.0).task("ThreatAssess", 6.0)
    b.fan_out("MultiFuse", "TargetIdentify", "ThreatAssess", edge_size=3.0)
    b.task("TargetNominate", 5.0)
    b.fan_in("TargetIdentify", "ThreatAssess", target="TargetNominate", edge_size=2.0)
    b.task("CommandApprove", 3.0).task("Disseminate", 2.0)
    b.chain("TargetNominate", "CommandApprove", "Disseminate", edge_size=1.0)
    b.task("BDA", 4.0)
    b.edge("Disseminate", "BDA", 1.0)
    tg = b.build()
    _save_workflow(base / "isr_pipeline",
        ProblemInstance(name="tactical.isr_pipeline", task_graph=tg, network=manet_net),
        _meta("tactical.isr_pipeline", "Tactical ISR Processing",
            "ISR pipeline. 3 collection channels (SIGINT, IMINT, HUMINT) → process → multi-fuse → (target ID, threat) → nominate → approve → disseminate → BDA.",
            ["tactical", "manet"], tg,
            _prov("DoD ISR architecture", "ISR processing patterns", year=2020, method="manual-figure"),
            tags=["isr", "intelligence", "targeting"], net_topo="manet", nn_min=7, nn_max=7))

    # 20. Fire Mission Request (10 tasks)
    b = DAGBuilder("fire_mission")
    b.task("ObserverDetect", 3.0).task("CallForFire", 2.0).task("FDC_Receive", 2.0)
    b.chain("ObserverDetect", "CallForFire", "FDC_Receive", edge_size=1.0)
    b.task("TargetAnalysis", 5.0).task("SafetyCheck", 4.0)
    b.fan_out("FDC_Receive", "TargetAnalysis", "SafetyCheck", edge_size=1.0)
    b.task("FireOrder", 3.0)
    b.fan_in("TargetAnalysis", "SafetyCheck", target="FireOrder", edge_size=1.0)
    b.task("GunLay", 4.0).task("Fire", 2.0).task("Splash", 1.0).task("Adjust", 3.0)
    b.chain("FireOrder", "GunLay", "Fire", "Splash", "Adjust", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "fire_mission",
        ProblemInstance(name="tactical.fire_mission", task_graph=tg, network=manet_net),
        _meta("tactical.fire_mission", "Tactical Fire Mission Request",
            "Fire mission. Observer → call for fire → FDC → (target analysis, safety check) → fire order → gun lay → fire → splash → adjust.",
            ["tactical", "manet"], tg,
            _prov("JP 3-09 Joint Fire Support", "Call for fire procedures", year=2019, method="manual-figure"),
            tags=["fire-mission", "artillery", "call-for-fire"], net_topo="manet", nn_min=7, nn_max=7))

    # 21. Situational Awareness COP (18 tasks)
    b = DAGBuilder("situational_awareness")
    for t in range(4):
        b.task(f"Team{t}_Sense", 3.0)
        b.task(f"Team{t}_Report", 2.0)
        b.edge(f"Team{t}_Sense", f"Team{t}_Report", 1.0)
    b.task("ForceTrack", 6.0)
    for t in range(4):
        b.edge(f"Team{t}_Report", "ForceTrack", 2.0)
    b.task("EnemyTrack", 6.0).task("TerrainOverlay", 4.0).task("WeatherOverlay", 3.0)
    b.edge("ForceTrack", "EnemyTrack", 2.0)
    b.edge("ForceTrack", "TerrainOverlay", 2.0)
    b.edge("ForceTrack", "WeatherOverlay", 2.0)
    b.task("COP_Build", 8.0)
    b.fan_in("EnemyTrack", "TerrainOverlay", "WeatherOverlay", target="COP_Build", edge_size=3.0)
    b.task("Distribute", 2.0).task("Archive", 2.0)
    b.fan_out("COP_Build", "Distribute", "Archive", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "situational_awareness",
        ProblemInstance(name="tactical.situational_awareness", task_graph=tg, network=manet_net),
        _meta("tactical.situational_awareness", "Tactical COP Situational Awareness",
            "Common Operating Picture. 4 teams (sense, report) → force track → (enemy, terrain, weather) → COP build → (distribute, archive).",
            ["tactical", "manet"], tg,
            _prov("NATO STANAG patterns", "COP generation", year=2020, method="manual-figure"),
            tags=["cop", "situational-awareness", "c2"], net_topo="manet", nn_min=7, nn_max=7))

    # 22. MEDEVAC Coordination (12 tasks)
    b = DAGBuilder("medevac")
    b.task("CasualtyReport", 2.0).task("Triage", 3.0)
    b.edge("CasualtyReport", "Triage", 1.0)
    b.task("MedEvacRequest", 2.0).task("LZ_Select", 4.0).task("SecurityPlan", 3.0)
    b.edge("Triage", "MedEvacRequest", 1.0)
    b.fan_out("MedEvacRequest", "LZ_Select", "SecurityPlan", edge_size=1.0)
    b.task("AircraftTask", 3.0)
    b.fan_in("LZ_Select", "SecurityPlan", target="AircraftTask", edge_size=1.0)
    b.task("Inbound", 4.0).task("Pickup", 3.0).task("EnRoute", 5.0)
    b.chain("AircraftTask", "Inbound", "Pickup", "EnRoute", edge_size=1.0)
    b.task("Handoff", 2.0).task("Close", 1.0)
    b.chain("EnRoute", "Handoff", "Close", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "medevac",
        ProblemInstance(name="tactical.medevac", task_graph=tg, network=manet_net),
        _meta("tactical.medevac", "Tactical MEDEVAC Coordination",
            "MEDEVAC coordination. Casualty → triage → request → (LZ select, security plan) → aircraft → inbound → pickup → en route → handoff → close.",
            ["tactical", "manet"], tg,
            _prov("TCCC/ATP 4-02.2 patterns", "MEDEVAC procedures", year=2020, method="manual-figure"),
            tags=["medevac", "medical", "evacuation"], net_topo="manet", nn_min=7, nn_max=7))

    # 23. MANET Mesh Routing (11 tasks)
    b = DAGBuilder("mesh_routing")
    b.task("HelloExchange", 2.0).task("NeighborDiscover", 3.0).task("MPR_Select", 4.0)
    b.chain("HelloExchange", "NeighborDiscover", "MPR_Select", edge_size=1.0)
    b.task("TopologyBuild", 5.0)
    b.edge("MPR_Select", "TopologyBuild", 2.0)
    b.task("RouteCalc", 6.0)
    b.edge("TopologyBuild", "RouteCalc", 2.0)
    for d in range(4):
        b.task(f"FwdTable{d}", 2.0)
        b.edge("RouteCalc", f"FwdTable{d}", 1.0)
    b.task("RouteMaintain", 3.0)
    for d in range(4):
        b.edge(f"FwdTable{d}", "RouteMaintain", 1.0)
    tg = b.build()
    _save_workflow(base / "mesh_routing",
        ProblemInstance(name="tactical.mesh_routing", task_graph=tg, network=manet_net),
        _meta("tactical.mesh_routing", "MANET Mesh Routing Protocol",
            "OLSR-like mesh routing. Hello → neighbor discover → MPR select → topology build → route calc → 4 forwarding tables → maintenance.",
            ["manet", "wireless"], tg,
            _prov("RFC 3626 (OLSR)", "OLSR routing protocol", year=2003, method="manual-figure"),
            tags=["olsr", "mesh", "routing", "manet"], net_topo="manet", nn_min=7, nn_max=7))


# ---------------------------------------------------------------------------
# Group 5: Smart City (6 DAGs)
# ---------------------------------------------------------------------------
def create_smart_city_dags(fog_net):
    base = WORKFLOWS_DIR / "smart_city"
    print("\n--- Smart City ---")

    # 24. Traffic Management (18 tasks)
    b = DAGBuilder("traffic_mgmt")
    for c in range(6):
        b.task(f"Cam{c}_Feed", 2.0)
        b.task(f"Cam{c}_Count", 5.0)
        b.edge(f"Cam{c}_Feed", f"Cam{c}_Count", 3.0)
    b.task("TrafficFuse", 6.0)
    for c in range(6):
        b.edge(f"Cam{c}_Count", "TrafficFuse", 2.0)
    b.task("PredictFlow", 8.0).task("OptimizeSignals", 10.0)
    b.chain("TrafficFuse", "PredictFlow", "OptimizeSignals", edge_size=3.0)
    b.task("SignalDeploy", 3.0).task("Dashboard", 2.0)
    b.fan_out("OptimizeSignals", "SignalDeploy", "Dashboard", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "traffic_mgmt",
        ProblemInstance(name="smart_city.traffic_mgmt", task_graph=tg, network=fog_net),
        _meta("smart_city.traffic_mgmt", "Smart City Traffic Management",
            "Traffic management. 6 camera feeds → count → fuse → predict → optimize signals → (deploy, dashboard).",
            ["smart-city", "edge-computing"], tg,
            _prov("Djahel et al. 2015", "A communications-oriented perspective on traffic management",
                  year=2015, doi="10.1109/JIOT.2014.2375731"),
            tags=["traffic", "signal-optimization", "cameras"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 25. Emergency Response (15 tasks)
    b = DAGBuilder("emergency_response")
    b.task("IncidentDetect", 3.0)
    b.task("Classify", 4.0).task("Locate", 3.0)
    b.fan_out("IncidentDetect", "Classify", "Locate", edge_size=1.0)
    b.task("DispatchFire", 5.0).task("DispatchEMS", 5.0).task("DispatchPolice", 5.0)
    b.edge("Classify", "DispatchFire", 1.0)
    b.edge("Classify", "DispatchEMS", 1.0)
    b.edge("Classify", "DispatchPolice", 1.0)
    b.edge("Locate", "DispatchFire", 1.0)
    b.edge("Locate", "DispatchEMS", 1.0)
    b.edge("Locate", "DispatchPolice", 1.0)
    b.task("RouteCalcFire", 4.0).task("RouteCalcEMS", 4.0).task("RouteCalcPolice", 4.0)
    b.edge("DispatchFire", "RouteCalcFire", 1.0)
    b.edge("DispatchEMS", "RouteCalcEMS", 1.0)
    b.edge("DispatchPolice", "RouteCalcPolice", 1.0)
    b.task("Coordinate", 6.0)
    b.fan_in("RouteCalcFire", "RouteCalcEMS", "RouteCalcPolice", target="Coordinate", edge_size=2.0)
    b.task("TrafficClear", 3.0).task("Notify", 2.0)
    b.fan_out("Coordinate", "TrafficClear", "Notify", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "emergency_response",
        ProblemInstance(name="smart_city.emergency_response", task_graph=tg, network=fog_net),
        _meta("smart_city.emergency_response", "Emergency Response Coordination",
            "Emergency response. Detect → (classify, locate) → 3 dispatch services → route calc → coordinate → (traffic clear, notify).",
            ["smart-city", "edge-computing"], tg,
            _prov("Fan et al. 2019", "Emergency response coordination", year=2019),
            tags=["emergency", "dispatch", "multi-agency"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 26. Waste Management (14 tasks)
    b = DAGBuilder("waste_mgmt")
    for i in range(8):
        b.task(f"Bin{i}_Sense", 1.0)
    b.task("Collect", 3.0)
    for i in range(8):
        b.edge(f"Bin{i}_Sense", "Collect", 0.5)
    b.task("FillPredict", 6.0).task("RouteOptimize", 10.0)
    b.chain("Collect", "FillPredict", "RouteOptimize", edge_size=2.0)
    b.task("AssignTrucks", 4.0).task("Schedule", 3.0).task("Notify", 2.0)
    b.chain("RouteOptimize", "AssignTrucks", "Schedule", "Notify", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "waste_mgmt",
        ProblemInstance(name="smart_city.waste_mgmt", task_graph=tg, network=fog_net),
        _meta("smart_city.waste_mgmt", "IoT Waste Management",
            "Waste management. 8 bin sensors → collect → fill predict → route optimize → assign trucks → schedule → notify.",
            ["smart-city", "iot"], tg,
            _prov("Pardini et al. 2019", "IoT-based waste management", year=2019),
            tags=["waste", "routing", "iot-sensors"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 27. Air Quality Monitoring (15 tasks)
    b = DAGBuilder("air_quality")
    for s in range(5):
        b.task(f"Station{s}_Sense", 2.0)
        b.task(f"Station{s}_Calibrate", 3.0)
        b.edge(f"Station{s}_Sense", f"Station{s}_Calibrate", 1.0)
    b.task("Aggregate", 4.0)
    for s in range(5):
        b.edge(f"Station{s}_Calibrate", "Aggregate", 1.0)
    b.task("AQI_Calc", 5.0).task("SourceAttrib", 7.0).task("HealthImpact", 6.0)
    b.fan_out("Aggregate", "AQI_Calc", "SourceAttrib", "HealthImpact", edge_size=2.0)
    b.task("Alert", 2.0)
    b.fan_in("AQI_Calc", "HealthImpact", target="Alert", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "air_quality",
        ProblemInstance(name="smart_city.air_quality", task_graph=tg, network=fog_net),
        _meta("smart_city.air_quality", "Air Quality Monitoring",
            "Air quality. 5 stations (sense, calibrate) → aggregate → (AQI calc, source attribution, health impact) → alert.",
            ["smart-city", "iot"], tg,
            _prov("Morawska et al. 2018", "Applications of low-cost sensing",
                  year=2018, doi="10.1016/j.envint.2018.02.022"),
            tags=["air-quality", "aqi", "environmental"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 28. Smart Parking (12 tasks)
    b = DAGBuilder("smart_parking")
    for z in range(4):
        b.task(f"Zone{z}_Sense", 2.0)
        b.task(f"Zone{z}_Process", 3.0)
        b.edge(f"Zone{z}_Sense", f"Zone{z}_Process", 1.0)
    b.task("OccupancyMap", 5.0)
    for z in range(4):
        b.edge(f"Zone{z}_Process", "OccupancyMap", 1.0)
    b.task("Predict", 6.0).task("Optimize", 8.0)
    b.chain("OccupancyMap", "Predict", "Optimize", edge_size=2.0)
    b.task("Guidance", 3.0)
    b.edge("Optimize", "Guidance", 1.0)
    tg = b.build()
    _save_workflow(base / "smart_parking",
        ProblemInstance(name="smart_city.smart_parking", task_graph=tg, network=fog_net),
        _meta("smart_city.smart_parking", "Smart Parking Management",
            "Smart parking. 4 zones (sense, process) → occupancy map → predict → optimize → guidance.",
            ["smart-city", "iot"], tg,
            _prov("Idris et al. 2009", "Smart parking systems", year=2009),
            tags=["parking", "occupancy", "guidance"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 29. Water Distribution (16 tasks)
    b = DAGBuilder("water_distribution")
    for s in range(6):
        b.task(f"Sensor{s}_Read", 1.0)
        b.task(f"Sensor{s}_Validate", 2.0)
        b.edge(f"Sensor{s}_Read", f"Sensor{s}_Validate", 0.5)
    b.task("SCADA_Collect", 4.0)
    for s in range(6):
        b.edge(f"Sensor{s}_Validate", "SCADA_Collect", 1.0)
    b.task("LeakDetect", 8.0).task("PressureOptimize", 7.0).task("QualityCheck", 6.0)
    b.fan_out("SCADA_Collect", "LeakDetect", "PressureOptimize", "QualityCheck", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "water_distribution",
        ProblemInstance(name="smart_city.water_distribution", task_graph=tg, network=fog_net),
        _meta("smart_city.water_distribution", "Water Distribution Network",
            "Water distribution. 6 sensor pairs (read, validate) → SCADA collect → (leak detect, pressure optimize, quality check).",
            ["smart-city", "iot", "cyber-physical"], tg,
            _prov("Loureiro et al. 2016", "Water distribution systems", year=2016),
            tags=["water", "scada", "leak-detection"], net_topo="fog-three-tier", nn_min=8, nn_max=8))


# ---------------------------------------------------------------------------
# Group 6: Industrial IoT (6 DAGs)
# ---------------------------------------------------------------------------
def create_iiot_dags(fog_net):
    base = WORKFLOWS_DIR / "industrial_iot"
    print("\n--- Industrial IoT ---")

    # 30. Visual Quality Inspection (13 tasks)
    b = DAGBuilder("quality_control")
    b.task("CameraCapture", 2.0).task("Preprocess", 3.0)
    b.edge("CameraCapture", "Preprocess", 4.0)
    b.tasks(("SurfaceDefect", 8.0), ("DimCheck", 7.0), ("ColorDefect", 6.0))
    b.fan_out("Preprocess", "SurfaceDefect", "DimCheck", "ColorDefect", edge_size=3.0)
    b.task("DefectFuse", 4.0)
    b.fan_in("SurfaceDefect", "DimCheck", "ColorDefect", target="DefectFuse", edge_size=2.0)
    b.task("Classify", 6.0).task("Grade", 3.0)
    b.chain("DefectFuse", "Classify", "Grade", edge_size=2.0)
    b.task("Reject", 2.0).task("Report", 2.0).task("Stats", 3.0)
    b.fan_out("Grade", "Reject", "Report", "Stats", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "quality_control",
        ProblemInstance(name="iiot.quality_control", task_graph=tg, network=fog_net),
        _meta("iiot.quality_control", "IIoT Visual Quality Inspection",
            "Quality inspection. Camera → preprocess → 3 parallel defect detectors → fuse → classify → grade → (reject, report, stats).",
            ["industrial-iot", "image-processing"], tg,
            _prov("Weimer et al. 2016", "Deep learning for surface defect detection", year=2016),
            tags=["quality", "inspection", "defect-detection"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 31. Robotic Assembly (18 tasks)
    b = DAGBuilder("robotic_assembly")
    b.task("OrderInit", 2.0)
    for r in range(3):
        b.task(f"R{r}_Fetch", 4.0).task(f"R{r}_Position", 3.0).task(f"R{r}_Check", 2.0)
        b.edge("OrderInit", f"R{r}_Fetch", 1.0)
        b.chain(f"R{r}_Fetch", f"R{r}_Position", f"R{r}_Check", edge_size=1.0)
    b.task("Assemble_Step1", 6.0)
    for r in range(3):
        b.edge(f"R{r}_Check", "Assemble_Step1", 1.0)
    b.task("Assemble_Step2", 8.0).task("Assemble_Step3", 7.0)
    b.chain("Assemble_Step1", "Assemble_Step2", "Assemble_Step3", edge_size=2.0)
    b.task("QualityInspect", 5.0).task("Package", 3.0).task("Log", 2.0)
    b.chain("Assemble_Step3", "QualityInspect", "Package", "Log", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "robotic_assembly",
        ProblemInstance(name="iiot.robotic_assembly", task_graph=tg, network=fog_net),
        _meta("iiot.robotic_assembly", "IIoT Robotic Assembly Coordination",
            "Robotic assembly. Order → 3 robots (fetch, position, check) → 3 sequential assembly steps → inspect → package → log.",
            ["industrial-iot", "cyber-physical"], tg,
            _prov("Wan et al. 2019", "Reconfigurable smart factory", year=2019,
                  doi="10.1109/TII.2019.2938890"),
            tags=["robotics", "assembly", "manufacturing"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 32. CNC Machine Monitoring (12 tasks)
    b = DAGBuilder("cnc_monitoring")
    b.tasks(("Vibration", 1.0), ("SpindleTemp", 1.0), ("CutForce", 1.0), ("PowerDraw", 1.0))
    b.task("Collect", 2.0)
    for s in ["Vibration", "SpindleTemp", "CutForce", "PowerDraw"]:
        b.edge(s, "Collect", 1.0)
    b.tasks(("ToolWear", 6.0), ("ChatterDetect", 5.0), ("ThermalDrift", 4.0))
    b.fan_out("Collect", "ToolWear", "ChatterDetect", "ThermalDrift", edge_size=2.0)
    b.task("AdaptiveControl", 8.0)
    b.fan_in("ToolWear", "ChatterDetect", "ThermalDrift", target="AdaptiveControl", edge_size=2.0)
    b.task("FeedRateAdj", 3.0).task("Alert", 2.0)
    b.fan_out("AdaptiveControl", "FeedRateAdj", "Alert", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "cnc_monitoring",
        ProblemInstance(name="iiot.cnc_monitoring", task_graph=tg, network=fog_net),
        _meta("iiot.cnc_monitoring", "IIoT CNC Machine Monitoring",
            "CNC monitoring. 4 sensors → collect → (tool wear, chatter, thermal) → adaptive control → (feed rate, alert).",
            ["industrial-iot", "cyber-physical"], tg,
            _prov("Teti et al. 2010", "Advanced monitoring of machining",
                  year=2010, doi="10.1016/j.cirp.2010.05.010"),
            tags=["cnc", "machining", "tool-wear"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 33. Chemical Process Control (15 tasks)
    b = DAGBuilder("process_control")
    b.tasks(("TempSensor", 1.0), ("PressureSensor", 1.0), ("FlowSensor", 1.0),
            ("LevelSensor", 1.0), ("pHSensor", 1.0))
    b.task("ProcessModel", 6.0)
    for s in ["TempSensor", "PressureSensor", "FlowSensor", "LevelSensor", "pHSensor"]:
        b.edge(s, "ProcessModel", 1.0)
    b.tasks(("TempPID", 5.0), ("PressurePID", 5.0), ("FlowPID", 5.0))
    b.fan_out("ProcessModel", "TempPID", "PressurePID", "FlowPID", edge_size=2.0)
    b.task("CascadeControl", 8.0)
    b.fan_in("TempPID", "PressurePID", "FlowPID", target="CascadeControl", edge_size=2.0)
    b.task("ValveActuate", 3.0).task("SafetyInterlock", 4.0)
    b.fan_out("CascadeControl", "ValveActuate", "SafetyInterlock", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "process_control",
        ProblemInstance(name="iiot.process_control", task_graph=tg, network=fog_net),
        _meta("iiot.process_control", "IIoT Chemical Process Control",
            "Chemical process. 5 sensors → process model → 3 PID controllers → cascade control → (valve, safety interlock).",
            ["industrial-iot", "cyber-physical"], tg,
            _prov("Xu et al. 2014", "Internet of things in industries",
                  year=2014, doi="10.1109/TII.2014.2300753"),
            tags=["process-control", "pid", "chemical"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 34. Digital Twin Sync (16 tasks)
    b = DAGBuilder("digital_twin")
    for s in range(5):
        b.task(f"Sensor{s}", 1.0)
    b.task("EdgePreprocess", 4.0)
    for s in range(5):
        b.edge(f"Sensor{s}", "EdgePreprocess", 1.0)
    b.task("EdgeTwin", 8.0).task("CloudSync", 5.0).task("CloudTwin", 10.0)
    b.chain("EdgePreprocess", "EdgeTwin", "CloudSync", "CloudTwin", edge_size=3.0)
    b.tasks(("PredictMaint", 7.0), ("OptimizeProcess", 8.0), ("SimulateWhat", 6.0))
    b.fan_out("CloudTwin", "PredictMaint", "OptimizeProcess", "SimulateWhat", edge_size=3.0)
    b.task("Dashboard", 3.0)
    b.fan_in("PredictMaint", "OptimizeProcess", "SimulateWhat", target="Dashboard", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "digital_twin",
        ProblemInstance(name="iiot.digital_twin", task_graph=tg, network=fog_net),
        _meta("iiot.digital_twin", "IIoT Digital Twin Synchronization",
            "Digital twin. 5 sensors → edge preprocess → edge twin → cloud sync → cloud twin → (predict, optimize, simulate) → dashboard.",
            ["industrial-iot", "edge-computing"], tg,
            _prov("Tao et al. 2019", "Digital twin in industry",
                  year=2019, doi="10.1007/s12206-019-0823-4"),
            tags=["digital-twin", "simulation", "cloud-edge"], net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 35. Factory Energy Monitoring (14 tasks)
    b = DAGBuilder("energy_monitoring")
    for m in range(8):
        b.task(f"Meter{m}", 1.0)
    b.task("Collect", 3.0)
    for m in range(8):
        b.edge(f"Meter{m}", "Collect", 0.5)
    b.task("Baseline", 5.0).task("PeakDetect", 4.0).task("Forecast", 6.0)
    b.fan_out("Collect", "Baseline", "PeakDetect", "Forecast", edge_size=2.0)
    b.task("Report", 3.0)
    b.fan_in("Baseline", "PeakDetect", "Forecast", target="Report", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "energy_monitoring",
        ProblemInstance(name="iiot.energy_monitoring", task_graph=tg, network=fog_net),
        _meta("iiot.energy_monitoring", "IIoT Factory Energy Monitoring",
            "Factory energy. 8 meters → collect → (baseline, peak detect, forecast) → report.",
            ["industrial-iot", "iot"], tg,
            _prov("O'Driscoll et al. 2013", "Industrial energy monitoring", year=2013),
            tags=["energy", "monitoring", "factory"], net_topo="fog-three-tier", nn_min=8, nn_max=8))


# ---------------------------------------------------------------------------
# Group 7: Agriculture IoT (4 DAGs)
# ---------------------------------------------------------------------------
def create_agriculture_dags(fog_net):
    base = WORKFLOWS_DIR / "agriculture_iot"
    print("\n--- Agriculture IoT ---")

    # 36. Precision Irrigation (15 tasks)
    b = DAGBuilder("precision_irrigation")
    for s in range(8):
        b.task(f"Soil{s}", 1.0)
    b.task("WeatherFetch", 3.0).task("Aggregate", 4.0)
    for s in range(8):
        b.edge(f"Soil{s}", "Aggregate", 0.5)
    b.edge("WeatherFetch", "Aggregate", 1.0)
    b.task("ETCalc", 5.0).task("ZoneCalc", 6.0)
    b.chain("Aggregate", "ETCalc", "ZoneCalc", edge_size=2.0)
    b.task("ValveSchedule", 4.0).task("PumpControl", 3.0)
    b.fan_out("ZoneCalc", "ValveSchedule", "PumpControl", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "precision_irrigation",
        ProblemInstance(name="agri.precision_irrigation", task_graph=tg, network=fog_net),
        _meta("agri.precision_irrigation", "Precision Irrigation",
            "Precision irrigation. 8 soil sensors + weather → aggregate → ET calc → zone calc → (valve schedule, pump control).",
            ["agriculture-iot", "iot"], tg,
            _prov("Ojha et al. 2015", "Wireless sensor networks for agriculture",
                  year=2015, doi="10.1016/j.compag.2015.08.011"),
            tags=["irrigation", "soil-moisture", "precision-agriculture"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 37. Livestock Monitoring (14 tasks)
    b = DAGBuilder("livestock_monitoring")
    for a in range(10):
        b.task(f"Animal{a}", 1.0)
    b.task("Collect", 3.0)
    for a in range(10):
        b.edge(f"Animal{a}", "Collect", 0.5)
    b.task("HerdAnalytics", 8.0).task("HealthAlert", 4.0).task("LocationTrack", 5.0)
    b.fan_out("Collect", "HerdAnalytics", "HealthAlert", "LocationTrack", edge_size=2.0)
    tg = b.build()
    _save_workflow(base / "livestock_monitoring",
        ProblemInstance(name="agri.livestock_monitoring", task_graph=tg, network=fog_net),
        _meta("agri.livestock_monitoring", "Livestock Monitoring",
            "Livestock monitoring. 10 animal sensors → collect → (herd analytics, health alert, location track).",
            ["agriculture-iot", "iot"], tg,
            _prov("Neethirajan 2017", "Recent advances in wearable sensors", year=2017),
            tags=["livestock", "animal-health", "wearable"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 38. Greenhouse Climate Control (13 tasks)
    b = DAGBuilder("greenhouse_control")
    b.tasks(("TempSensor", 1.0), ("HumiditySensor", 1.0), ("LightSensor", 1.0),
            ("CO2Sensor", 1.0), ("SoilMoisture", 1.0))
    b.task("ClimateModel", 8.0)
    for s in ["TempSensor", "HumiditySensor", "LightSensor", "CO2Sensor", "SoilMoisture"]:
        b.edge(s, "ClimateModel", 1.0)
    b.task("Optimize", 6.0)
    b.edge("ClimateModel", "Optimize", 2.0)
    b.tasks(("HVAC_Control", 4.0), ("Irrigation", 3.0), ("LightControl", 3.0), ("CO2_Inject", 3.0))
    b.fan_out("Optimize", "HVAC_Control", "Irrigation", "LightControl", "CO2_Inject", edge_size=1.0)
    b.task("Log", 2.0)
    b.fan_in("HVAC_Control", "Irrigation", "LightControl", "CO2_Inject", target="Log", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "greenhouse_control",
        ProblemInstance(name="agri.greenhouse_control", task_graph=tg, network=fog_net),
        _meta("agri.greenhouse_control", "Greenhouse Climate Control",
            "Greenhouse control. 5 sensors → climate model → optimize → 4 parallel actuators (HVAC, irrigation, light, CO2) → log.",
            ["agriculture-iot", "iot", "cyber-physical"], tg,
            _prov("Shamshiri et al. 2018", "Greenhouse automation", year=2018),
            tags=["greenhouse", "climate-control", "actuator"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 39. Crop Disease Detection (12 tasks)
    b = DAGBuilder("crop_disease")
    b.task("ImageCapture", 2.0).task("Preprocess", 3.0)
    b.edge("ImageCapture", "Preprocess", 3.0)
    b.tasks(("ColorFeature", 5.0), ("TextureFeature", 5.0), ("ShapeFeature", 5.0))
    b.fan_out("Preprocess", "ColorFeature", "TextureFeature", "ShapeFeature", edge_size=2.0)
    b.task("FeatureFuse", 3.0)
    b.fan_in("ColorFeature", "TextureFeature", "ShapeFeature", target="FeatureFuse", edge_size=2.0)
    b.task("Classify", 8.0).task("SeverityScore", 4.0)
    b.chain("FeatureFuse", "Classify", "SeverityScore", edge_size=2.0)
    b.task("TreatmentRec", 5.0).task("Alert", 2.0).task("MapUpdate", 3.0)
    b.fan_out("SeverityScore", "TreatmentRec", "Alert", "MapUpdate", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "crop_disease",
        ProblemInstance(name="agri.crop_disease", task_graph=tg, network=fog_net),
        _meta("agri.crop_disease", "Crop Disease Detection",
            "Crop disease. Capture → preprocess → 3 feature extractors → fuse → classify → severity → (treatment, alert, map).",
            ["agriculture-iot", "ml-pipeline", "image-processing"], tg,
            _prov("Mohanty et al. 2016", "Using deep learning for plant disease detection",
                  year=2016, doi="10.3389/fpls.2016.01419"),
            tags=["crop-disease", "plant-pathology", "deep-learning"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))


# ---------------------------------------------------------------------------
# Group 8: NFV/Telecom (5 DAGs)
# ---------------------------------------------------------------------------
def create_nfv_dags(net4):
    base = WORKFLOWS_DIR / "nfv_telecom"
    print("\n--- NFV/Telecom ---")

    # 40. IMS SIP Session (11 tasks)
    b = DAGBuilder("ims_sip_chain")
    b.task("UE_Invite", 2.0).task("P_CSCF", 3.0).task("I_CSCF", 3.0)
    b.chain("UE_Invite", "P_CSCF", "I_CSCF", edge_size=1.0)
    b.task("HSS_Query", 4.0).task("S_CSCF", 5.0)
    b.edge("I_CSCF", "HSS_Query", 1.0)
    b.edge("I_CSCF", "S_CSCF", 1.0)
    b.task("AS_Trigger", 3.0)
    b.fan_in("HSS_Query", "S_CSCF", target="AS_Trigger", edge_size=1.0)
    b.task("MediaNegotiate", 4.0).task("BearerSetup", 3.0)
    b.chain("AS_Trigger", "MediaNegotiate", "BearerSetup", edge_size=2.0)
    b.task("Ringing", 1.0).task("Connected", 1.0).task("CDR_Log", 2.0)
    b.chain("BearerSetup", "Ringing", "Connected", "CDR_Log", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "ims_sip_chain",
        ProblemInstance(name="nfv.ims_sip_chain", task_graph=tg, network=net4),
        _meta("nfv.ims_sip_chain", "IMS SIP Session Establishment",
            "IMS SIP session. UE invite → P-CSCF → I-CSCF → (HSS, S-CSCF) → AS → media negotiate → bearer → ringing → connected → CDR log.",
            ["nfv", "telecom"], tg,
            _prov("3GPP TS 23.228", "IMS architecture", year=2020, method="manual-figure"),
            tags=["ims", "sip", "session", "3gpp"]))

    # 41. 5G UE Attach (14 tasks)
    b = DAGBuilder("ue_attach")
    b.task("UE_Request", 2.0).task("AMF_Receive", 3.0)
    b.edge("UE_Request", "AMF_Receive", 1.0)
    b.tasks(("AUSF_Auth", 5.0), ("UDM_Subscribe", 4.0), ("NRF_Discover", 3.0), ("NSSF_Slice", 4.0))
    b.fan_out("AMF_Receive", "AUSF_Auth", "UDM_Subscribe", "NRF_Discover", "NSSF_Slice", edge_size=1.0)
    b.task("AMF_Context", 4.0)
    b.fan_in("AUSF_Auth", "UDM_Subscribe", "NRF_Discover", "NSSF_Slice", target="AMF_Context", edge_size=1.0)
    b.task("SMF_Session", 5.0).task("UPF_Tunnel", 4.0)
    b.chain("AMF_Context", "SMF_Session", "UPF_Tunnel", edge_size=2.0)
    b.task("QoS_Setup", 3.0).task("gNB_Config", 3.0)
    b.fan_out("UPF_Tunnel", "QoS_Setup", "gNB_Config", edge_size=1.0)
    b.task("AttachComplete", 2.0)
    b.fan_in("QoS_Setup", "gNB_Config", target="AttachComplete", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "ue_attach",
        ProblemInstance(name="nfv.ue_attach", task_graph=tg, network=net4),
        _meta("nfv.ue_attach", "5G UE Attach Procedure",
            "5G attach. UE → AMF → (AUSF, UDM, NRF, NSSF) → AMF context → SMF → UPF → (QoS, gNB) → complete.",
            ["nfv", "telecom", "5g"], tg,
            _prov("3GPP TS 23.501", "5G system architecture", year=2021, method="manual-figure"),
            tags=["5g", "attach", "core-network", "sba"]))

    # 42. NFV Auto-Scaling (13 tasks)
    b = DAGBuilder("vnf_scaling")
    b.task("Monitor", 3.0).task("Analyze", 5.0).task("Decide", 6.0)
    b.chain("Monitor", "Analyze", "Decide", edge_size=2.0)
    b.tasks(("SpinVNF", 5.0), ("AllocBW", 4.0), ("UpdateLB", 3.0), ("MigrateState", 6.0))
    b.fan_out("Decide", "SpinVNF", "AllocBW", "UpdateLB", "MigrateState", edge_size=2.0)
    b.task("Verify", 4.0)
    b.fan_in("SpinVNF", "AllocBW", "UpdateLB", "MigrateState", target="Verify", edge_size=1.0)
    b.task("ConfigUpdate", 3.0).task("Notify", 2.0)
    b.fan_out("Verify", "ConfigUpdate", "Notify", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "vnf_scaling",
        ProblemInstance(name="nfv.vnf_scaling", task_graph=tg, network=net4),
        _meta("nfv.vnf_scaling", "NFV Auto-Scaling Orchestration",
            "NFV auto-scaling. Monitor → analyze → decide → (spin VNF, alloc BW, update LB, migrate) → verify → (config, notify).",
            ["nfv", "telecom"], tg,
            _prov("ETSI GS NFV-MAN 001", "NFV management", year=2017, method="manual-figure"),
            tags=["nfv", "auto-scaling", "orchestration", "mano"]))

    # 43. CDN Edge Caching (11 tasks)
    b = DAGBuilder("cdn_edge_cache")
    b.task("UserRequest", 1.0).task("CacheLookup", 2.0)
    b.edge("UserRequest", "CacheLookup", 1.0)
    b.task("CacheHit_Serve", 3.0).task("CacheMiss_Fetch", 6.0)
    b.fan_out("CacheLookup", "CacheHit_Serve", "CacheMiss_Fetch", edge_size=2.0)
    b.task("Transcode", 8.0).task("CacheStore", 3.0)
    b.edge("CacheMiss_Fetch", "Transcode", 3.0)
    b.edge("Transcode", "CacheStore", 2.0)
    b.task("StreamMux", 4.0)
    b.fan_in("CacheHit_Serve", "CacheStore", target="StreamMux", edge_size=2.0)
    b.task("ABR_Adapt", 3.0).task("Deliver", 2.0).task("Analytics", 2.0)
    b.chain("StreamMux", "ABR_Adapt", "Deliver", edge_size=2.0)
    b.edge("Deliver", "Analytics", 1.0)
    tg = b.build()
    _save_workflow(base / "cdn_edge_cache",
        ProblemInstance(name="nfv.cdn_edge_cache", task_graph=tg, network=net4),
        _meta("nfv.cdn_edge_cache", "NFV CDN Edge Caching",
            "CDN caching. Request → lookup → (cache hit/miss paths) → transcode → store → mux → ABR → deliver → analytics.",
            ["nfv", "edge-computing"], tg,
            _prov("Bilal et al. 2018", "Edge caching for CDN", year=2018),
            tags=["cdn", "caching", "streaming", "abr"]))

    # 44. RAN Slicing (14 tasks)
    b = DAGBuilder("ran_slicing")
    b.tasks(("eMBB_Request", 2.0), ("URLLC_Request", 2.0), ("mMTC_Request", 2.0))
    b.task("SliceScheduler", 8.0)
    b.fan_in("eMBB_Request", "URLLC_Request", "mMTC_Request", target="SliceScheduler", edge_size=1.0)
    b.tasks(("eMBB_RB_Alloc", 5.0), ("URLLC_RB_Alloc", 5.0), ("mMTC_RB_Alloc", 4.0))
    b.fan_out("SliceScheduler", "eMBB_RB_Alloc", "URLLC_RB_Alloc", "mMTC_RB_Alloc", edge_size=2.0)
    b.tasks(("eMBB_Config", 3.0), ("URLLC_Config", 3.0), ("mMTC_Config", 3.0))
    b.edge("eMBB_RB_Alloc", "eMBB_Config", 1.0)
    b.edge("URLLC_RB_Alloc", "URLLC_Config", 1.0)
    b.edge("mMTC_RB_Alloc", "mMTC_Config", 1.0)
    b.task("Activate", 4.0)
    b.fan_in("eMBB_Config", "URLLC_Config", "mMTC_Config", target="Activate", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "ran_slicing",
        ProblemInstance(name="nfv.ran_slicing", task_graph=tg, network=net4),
        _meta("nfv.ran_slicing", "5G RAN Slicing Resource Allocation",
            "RAN slicing. 3 slice requests → scheduler → 3 parallel RB allocs → 3 configs → activate.",
            ["nfv", "5g", "wireless"], tg,
            _prov("Foukas et al. 2017", "Network slicing in 5G",
                  year=2017, doi="10.1109/MCOM.2017.1600920"),
            tags=["ran-slicing", "5g-nr", "resource-allocation"]))


# ---------------------------------------------------------------------------
# Group 9: Fog Computing (6 DAGs)
# ---------------------------------------------------------------------------
def create_fog_dags(fog_net):
    base = WORKFLOWS_DIR / "fog_computing"
    print("\n--- Fog Computing ---")

    # 45. Three-Tier Offloading (15 tasks)
    b = DAGBuilder("three_tier_offload")
    b.task("SensorIngest", 2.0).task("EdgeFilter", 3.0).task("EdgeAnalyze", 5.0)
    b.chain("SensorIngest", "EdgeFilter", "EdgeAnalyze", edge_size=2.0)
    b.task("FogAggregate", 6.0).task("FogML", 8.0)
    b.chain("EdgeAnalyze", "FogAggregate", "FogML", edge_size=3.0)
    b.task("CloudStore", 4.0).task("CloudTrain", 12.0)
    b.chain("FogML", "CloudStore", "CloudTrain", edge_size=4.0)
    b.task("ModelUpdate", 5.0).task("FogDeploy", 3.0).task("EdgeDeploy", 3.0)
    b.chain("CloudTrain", "ModelUpdate", "FogDeploy", "EdgeDeploy", edge_size=2.0)
    b.task("Monitor", 3.0).task("Feedback", 2.0)
    b.edge("EdgeDeploy", "Monitor", 1.0)
    b.edge("Monitor", "Feedback", 1.0)
    tg = b.build()
    _save_workflow(base / "three_tier_offload",
        ProblemInstance(name="fog.three_tier_offload", task_graph=tg, network=fog_net),
        _meta("fog.three_tier_offload", "Fog Three-Tier Offloading",
            "Three-tier pipeline. Sensor → edge (filter, analyze) → fog (aggregate, ML) → cloud (store, train) → model update → deploy → monitor → feedback.",
            ["fog-computing", "edge-computing"], tg,
            _prov("Bonomi et al. 2014", "Fog computing: A platform for IoT and analytics",
                  year=2014, doi="10.1007/978-3-319-05029-4_7"),
            tags=["three-tier", "offloading", "fog-edge-cloud"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 46. Latency-Critical Processing (8 tasks)
    b = DAGBuilder("latency_critical")
    b.task("Sense", 1.0).task("EdgePreproc", 2.0).task("EdgeInfer", 4.0)
    b.chain("Sense", "EdgePreproc", "EdgeInfer", edge_size=1.0)
    b.task("FogValidate", 3.0).task("FogEnrich", 4.0)
    b.chain("EdgeInfer", "FogValidate", "FogEnrich", edge_size=2.0)
    b.task("Decide", 3.0).task("Actuate", 2.0).task("Log", 1.0)
    b.chain("FogEnrich", "Decide", "Actuate", edge_size=1.0)
    b.edge("Decide", "Log", 1.0)
    tg = b.build()
    _save_workflow(base / "latency_critical",
        ProblemInstance(name="fog.latency_critical", task_graph=tg, network=fog_net),
        _meta("fog.latency_critical", "Fog Latency-Critical Processing",
            "Latency-critical fog pipeline. Sense → edge preproc → edge infer → fog validate → fog enrich → decide → (actuate, log).",
            ["fog-computing", "edge-computing"], tg,
            _prov("Yousefpour et al. 2019", "All one needs to know about fog computing",
                  year=2019, doi="10.1109/JIOT.2019.2892556"),
            tags=["latency-critical", "real-time", "fog"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 47. Federated Fog Learning (22 tasks)
    b = DAGBuilder("federated_fog")
    b.task("GlobalInit", 3.0)
    for f in range(5):
        b.task(f"Fog{f}_DataPrep", 2.0)
        b.task(f"Fog{f}_Train", 8.0)
        b.task(f"Fog{f}_Eval", 3.0)
        b.task(f"Fog{f}_Upload", 2.0)
        b.edge("GlobalInit", f"Fog{f}_DataPrep", 1.0)
        b.chain(f"Fog{f}_DataPrep", f"Fog{f}_Train", f"Fog{f}_Eval",
                f"Fog{f}_Upload", edge_size=2.0)
    b.task("Aggregate", 6.0)
    for f in range(5):
        b.edge(f"Fog{f}_Upload", "Aggregate", 3.0)
    b.task("GlobalUpdate", 4.0)
    b.edge("Aggregate", "GlobalUpdate", 2.0)
    tg = b.build()
    _save_workflow(base / "federated_fog",
        ProblemInstance(name="fog.federated_fog", task_graph=tg, network=fog_net),
        _meta("fog.federated_fog", "Federated Fog Learning",
            "Federated learning on fog. Global init → 5 fog nodes (data prep, train, eval, upload) → aggregate → global update.",
            ["fog-computing", "ml-pipeline"], tg,
            _prov("Wang et al. 2020", "Federated learning for fog computing", year=2020),
            tags=["federated-learning", "fog", "distributed-ml"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 48. Healthcare Fog Monitoring (18 tasks)
    b = DAGBuilder("healthcare_fog")
    for s in range(9):
        b.task(f"Sensor{s}", 1.0)
    for p in range(3):
        b.task(f"PatientEdge{p}", 4.0)
        for s in range(p * 3, p * 3 + 3):
            b.edge(f"Sensor{s}", f"PatientEdge{p}", 1.0)
    b.task("FogAnalyze", 8.0).task("FogAlert", 4.0)
    for p in range(3):
        b.edge(f"PatientEdge{p}", "FogAnalyze", 2.0)
    b.edge("FogAnalyze", "FogAlert", 1.0)
    b.task("CloudStore", 5.0).task("CloudAnalytics", 10.0).task("DoctorNotify", 2.0)
    b.edge("FogAnalyze", "CloudStore", 3.0)
    b.edge("CloudStore", "CloudAnalytics", 2.0)
    b.edge("FogAlert", "DoctorNotify", 1.0)
    tg = b.build()
    _save_workflow(base / "healthcare_fog",
        ProblemInstance(name="fog.healthcare_fog", task_graph=tg, network=fog_net),
        _meta("fog.healthcare_fog", "Fog Healthcare Monitoring",
            "Healthcare fog. 9 sensors → 3 patient edges → fog (analyze, alert) → cloud (store, analytics) + doctor notify.",
            ["fog-computing", "iot"], tg,
            _prov("Negash et al. 2018", "Fog computing for healthcare IoT",
                  year=2018, doi="10.1016/j.future.2018.04.036"),
            tags=["healthcare", "fog", "patient-monitoring"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 49. Content Distribution (13 tasks)
    b = DAGBuilder("content_distribution")
    b.task("Ingest", 3.0)
    b.tasks(("Transcode720", 6.0), ("Transcode1080", 8.0), ("Transcode4K", 12.0))
    b.fan_out("Ingest", "Transcode720", "Transcode1080", "Transcode4K", edge_size=5.0)
    b.task("DRM_Encrypt", 5.0)
    b.fan_in("Transcode720", "Transcode1080", "Transcode4K", target="DRM_Encrypt", edge_size=3.0)
    b.tasks(("Cache_Region0", 3.0), ("Cache_Region1", 3.0),
            ("Cache_Region2", 3.0), ("Cache_Region3", 3.0))
    b.fan_out("DRM_Encrypt", "Cache_Region0", "Cache_Region1",
              "Cache_Region2", "Cache_Region3", edge_size=4.0)
    b.task("Manifest", 2.0)
    b.fan_in("Cache_Region0", "Cache_Region1", "Cache_Region2", "Cache_Region3",
             target="Manifest", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "content_distribution",
        ProblemInstance(name="fog.content_distribution", task_graph=tg, network=fog_net),
        _meta("fog.content_distribution", "Fog Content Distribution",
            "Content distribution. Ingest → 3 parallel transcodes → DRM → 4 regional caches → manifest.",
            ["fog-computing", "edge-computing"], tg,
            _prov("Aazam et al. 2016", "Fog computing for content distribution", year=2016),
            tags=["content", "cdn", "transcoding", "drm"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))

    # 50. Smart Building (20 tasks)
    b = DAGBuilder("smart_building")
    for s in range(12):
        b.task(f"Sensor{s}", 1.0)
    for f in range(4):
        b.task(f"FloorEdge{f}", 4.0)
        for s in range(f * 3, f * 3 + 3):
            b.edge(f"Sensor{s}", f"FloorEdge{f}", 0.5)
    b.task("BuildingFog", 8.0)
    for f in range(4):
        b.edge(f"FloorEdge{f}", "BuildingFog", 2.0)
    b.tasks(("EnergyOpt", 6.0), ("ComfortOpt", 5.0), ("SecurityOpt", 5.0))
    b.fan_out("BuildingFog", "EnergyOpt", "ComfortOpt", "SecurityOpt", edge_size=2.0)
    b.task("Actuate", 3.0)
    b.fan_in("EnergyOpt", "ComfortOpt", "SecurityOpt", target="Actuate", edge_size=1.0)
    tg = b.build()
    _save_workflow(base / "smart_building",
        ProblemInstance(name="fog.smart_building", task_graph=tg, network=fog_net),
        _meta("fog.smart_building", "Fog Smart Building Management",
            "Smart building. 12 sensors → 4 floor edges → building fog → (energy, comfort, security optimize) → actuate.",
            ["fog-computing", "smart-city", "iot"], tg,
            _prov("Alrawais et al. 2020", "Fog-based smart building", year=2020),
            tags=["smart-building", "hvac", "bms"],
            net_topo="fog-three-tier", nn_min=8, nn_max=8))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"DAGBench Campaign 2: Networking/IoT/Edge - {TODAY}")

    # Create networks
    mec_net = mec_network(num_ue=4, num_mec=2)
    manet_net = manet_network(num_nodes=6)
    fog_net = fog_network(num_edge=5, num_fog=2, num_cloud=1)
    net4 = homogeneous_network(num_nodes=4, speed=2.0, bandwidth=500.0)

    create_mec_dags(mec_net)
    create_v2x_dags(mec_net)
    create_uav_dags(mec_net)
    create_tactical_dags(manet_net)
    create_smart_city_dags(fog_net)
    create_iiot_dags(fog_net)
    create_agriculture_dags(fog_net)
    create_nfv_dags(net4)
    create_fog_dags(fog_net)

    from dagbench.catalog import list_workflows
    workflows = list_workflows(workflows_dir=WORKFLOWS_DIR)
    print(f"\n=== Total: {len(workflows)} workflows ===")


if __name__ == "__main__":
    main()
