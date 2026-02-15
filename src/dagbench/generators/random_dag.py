"""Random layered DAG generator.

Clean reimplementation of the random layered DAG generation algorithm from
ANRGUSC's Automatic-DAG-Generator (https://github.com/ANRGUSC/Automatic-DAG-Generator,
Sep 2018, BSD license). Generates random DAGs with configurable depth, width,
connectivity, and cost distributions.

Unlike SAGA's built-in generators (diamond, chain, fork) which produce fixed
topologies with random weights, this randomizes both structure and weights.
"""
from __future__ import annotations

import random

from saga import TaskGraph, TaskGraphNode, TaskGraphEdge


def random_layered_dag(
    depth: int = 4,
    width_min: int = 2,
    width_max: int = 5,
    deg_mu: float = 4.0,
    deg_sigma: float = 1.0,
    delta_lvl: int = 2,
    comp_mu: float = 10.0,
    comp_sigma: float = 2.0,
    ccr: float = 0.5,
    comm_sigma_factor: float = 0.1,
    seed: int | None = None,
) -> TaskGraph:
    """Generate a random layered DAG with configurable structure and costs.

    The DAG has a single source node, ``depth`` interior levels with random
    widths, and a single sink node.  Edges connect nodes across adjacent
    levels (up to ``delta_lvl`` apart) with degree sampled from a normal
    distribution.

    Args:
        depth: Number of interior levels (total levels = depth + 2).
        width_min: Minimum nodes per interior level.
        width_max: Maximum nodes per interior level.
        deg_mu: Mean half-degree for each node (controls edge density).
        deg_sigma: Standard deviation of half-degree.
        delta_lvl: Maximum level span for an edge (1 = only adjacent levels).
        comp_mu: Mean task computation cost.
        comp_sigma: Standard deviation of computation cost.
        ccr: Communication-to-computation ratio. Mean comm cost = ccr * comp_mu.
        comm_sigma_factor: Communication std dev as fraction of mean comm cost.
        seed: Random seed for reproducibility.

    Returns:
        A SAGA TaskGraph.
    """
    rng = random.Random(seed)

    # --- Level widths: [1, random interior levels, 1] ---
    level_widths = [1]
    for _ in range(depth):
        level_widths.append(rng.randint(width_min, width_max))
    level_widths.append(1)

    # --- Assign node IDs by level ---
    levels: list[list[str]] = []
    node_id = 0
    for lvl_width in level_widths:
        lvl_nodes = []
        for _ in range(lvl_width):
            lvl_nodes.append(f"T{node_id}")
            node_id += 1
        levels.append(lvl_nodes)

    num_levels = len(levels)

    # --- Build edges via top-down + bottom-up wiring ---
    edge_set: set[tuple[str, str]] = set()

    # Map node -> level index for quick lookup
    node_level: dict[str, int] = {}
    for lvl_idx, lvl_nodes in enumerate(levels):
        for n in lvl_nodes:
            node_level[n] = lvl_idx

    # Top-down pass: each node tries to connect to descendants
    for lvl_idx in range(num_levels - 1):
        for node in levels[lvl_idx]:
            half_deg = max(1, int(round(rng.gauss(deg_mu, deg_sigma))))
            half_deg = min(half_deg, 20)
            # Collect candidate targets within delta_lvl
            candidates = []
            for d in range(1, min(delta_lvl, num_levels - lvl_idx - 1) + 1):
                candidates.extend(levels[lvl_idx + d])
            if not candidates:
                continue
            k = min(half_deg, len(candidates))
            targets = rng.sample(candidates, k)
            for t in targets:
                edge_set.add((node, t))

    # Bottom-up pass: each node tries to connect to ancestors
    for lvl_idx in range(1, num_levels):
        for node in levels[lvl_idx]:
            half_deg = max(1, int(round(rng.gauss(deg_mu, deg_sigma))))
            half_deg = min(half_deg, 20)
            candidates = []
            for d in range(1, min(delta_lvl, lvl_idx) + 1):
                candidates.extend(levels[lvl_idx - d])
            if not candidates:
                continue
            k = min(half_deg, len(candidates))
            sources = rng.sample(candidates, k)
            for s in sources:
                edge_set.add((s, node))

    # --- Connectivity guarantee: ensure no orphan nodes ---
    # Every non-source node must have at least one incoming edge
    source_node = levels[0][0]
    sink_node = levels[-1][0]

    for lvl_idx in range(1, num_levels):
        for node in levels[lvl_idx]:
            has_incoming = any(
                (s, node) in edge_set
                for d in range(1, min(delta_lvl, lvl_idx) + 1)
                for s in levels[lvl_idx - d]
            )
            if not has_incoming:
                # Connect from a random node in the nearest ancestor level
                parent_lvl = max(0, lvl_idx - 1)
                parent = rng.choice(levels[parent_lvl])
                edge_set.add((parent, node))

    # Every non-sink node must have at least one outgoing edge
    for lvl_idx in range(num_levels - 1):
        for node in levels[lvl_idx]:
            has_outgoing = any(
                (node, t) in edge_set
                for d in range(1, min(delta_lvl, num_levels - lvl_idx - 1) + 1)
                for t in levels[lvl_idx + d]
            )
            if not has_outgoing:
                child_lvl = min(num_levels - 1, lvl_idx + 1)
                child = rng.choice(levels[child_lvl])
                edge_set.add((node, child))

    # --- Assign costs ---
    comm_mu = ccr * comp_mu
    comm_sigma = comm_sigma_factor * comm_mu if comm_mu > 0 else 0.1

    task_nodes = []
    for lvl_nodes in levels:
        for name in lvl_nodes:
            cost = max(0.01, rng.gauss(comp_mu, comp_sigma))
            task_nodes.append(TaskGraphNode(name=name, cost=cost))

    task_edges = []
    for src, tgt in edge_set:
        size = max(0.01, rng.gauss(comm_mu, comm_sigma))
        task_edges.append(TaskGraphEdge(source=src, target=tgt, size=size))

    return TaskGraph(tasks=frozenset(task_nodes), dependencies=frozenset(task_edges))
