"""Converter for CSV adjacency list format.

Supports two CSV formats:

1. Edge list: source,target[,weight]
2. Adjacency with node costs: task_file + edge_file

Task CSV: name,cost
Edge CSV: source,target,size
"""
from __future__ import annotations

import csv
from pathlib import Path

from saga import TaskGraph, TaskGraphNode, TaskGraphEdge


def parse_edge_list_csv(
    text: str,
    has_header: bool = True,
    default_node_cost: float = 0.0,
) -> TaskGraph:
    """Parse an edge-list CSV string.

    Format: source,target[,weight]

    Args:
        text: CSV content
        has_header: Whether the first line is a header
        default_node_cost: Cost assigned to auto-discovered nodes

    Returns:
        SAGA TaskGraph
    """
    lines = text.strip().splitlines()
    reader = csv.reader(lines)

    if has_header:
        next(reader, None)

    nodes: dict[str, float] = {}
    edges: list[TaskGraphEdge] = []

    for row in reader:
        if len(row) < 2:
            continue
        src, tgt = row[0].strip(), row[1].strip()
        size = float(row[2].strip()) if len(row) > 2 else 0.0

        nodes.setdefault(src, default_node_cost)
        nodes.setdefault(tgt, default_node_cost)
        edges.append(TaskGraphEdge(source=src, target=tgt, size=size))

    task_nodes = frozenset(
        TaskGraphNode(name=name, cost=cost) for name, cost in nodes.items()
    )
    return TaskGraph(tasks=task_nodes, dependencies=frozenset(edges))


def parse_task_and_edge_csv(
    task_csv: str,
    edge_csv: str,
    has_header: bool = True,
) -> TaskGraph:
    """Parse separate task and edge CSV files.

    Task CSV format: name,cost
    Edge CSV format: source,target,size

    Args:
        task_csv: CSV content for tasks (name, cost)
        edge_csv: CSV content for edges (source, target, size)
        has_header: Whether the first line is a header

    Returns:
        SAGA TaskGraph
    """
    # Parse tasks
    task_lines = task_csv.strip().splitlines()
    task_reader = csv.reader(task_lines)
    if has_header:
        next(task_reader, None)

    tasks = []
    for row in task_reader:
        if len(row) < 2:
            continue
        tasks.append(TaskGraphNode(name=row[0].strip(), cost=float(row[1].strip())))

    # Parse edges
    edge_lines = edge_csv.strip().splitlines()
    edge_reader = csv.reader(edge_lines)
    if has_header:
        next(edge_reader, None)

    edges = []
    for row in edge_reader:
        if len(row) < 2:
            continue
        size = float(row[2].strip()) if len(row) > 2 else 0.0
        edges.append(TaskGraphEdge(source=row[0].strip(), target=row[1].strip(), size=size))

    return TaskGraph(tasks=frozenset(tasks), dependencies=frozenset(edges))


def load_edge_list_csv(path: Path, **kwargs) -> TaskGraph:
    """Load an edge-list CSV file."""
    with open(path, "r", encoding="utf-8") as f:
        return parse_edge_list_csv(f.read(), **kwargs)
