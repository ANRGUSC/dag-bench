"""Compute DAG metrics: CCR, depth, width, parallelism."""
from __future__ import annotations

from collections import defaultdict

import networkx as nx
from saga import TaskGraph, Network

from dagbench.schema import GraphStats


def compute_depth(G: nx.DiGraph) -> int:
    """Length of the longest path in the DAG (number of nodes, not weighted)."""
    if len(G) == 0:
        return 0
    # Use default_weight=1 to get structural depth (hop count), not weighted path
    return nx.dag_longest_path_length(G, weight=None) + 1  # +1 to count nodes, not edges


def compute_width(G: nx.DiGraph) -> int:
    """Maximum number of nodes at any topological level."""
    if len(G) == 0:
        return 0
    # Assign levels via longest path from any source
    levels: dict[str, int] = {}
    for node in nx.topological_sort(G):
        preds = list(G.predecessors(node))
        if not preds:
            levels[node] = 0
        else:
            levels[node] = max(levels[p] for p in preds) + 1

    level_counts: dict[int, int] = defaultdict(int)
    for level in levels.values():
        level_counts[level] += 1
    return max(level_counts.values())


def compute_ccr(task_graph: TaskGraph) -> float | None:
    """Communication-to-Computation Ratio.

    CCR = total_edge_weight / total_node_weight.
    Returns None if all costs are zero (structure-only DAG).
    """
    total_computation = sum(t.cost for t in task_graph.tasks)
    total_communication = sum(d.size for d in task_graph.dependencies)
    if total_computation == 0:
        return None
    return total_communication / total_computation


def compute_graph_stats(task_graph: TaskGraph) -> GraphStats:
    """Compute all graph statistics for a TaskGraph."""
    G = task_graph.graph
    num_tasks = len(task_graph.tasks)
    num_edges = len(task_graph.dependencies)
    depth = compute_depth(G)
    width = compute_width(G)
    ccr = compute_ccr(task_graph)
    parallelism = num_tasks / depth if depth > 0 else None

    return GraphStats(
        num_tasks=num_tasks,
        num_edges=num_edges,
        depth=depth,
        width=width,
        ccr=ccr,
        parallelism=parallelism,
    )
