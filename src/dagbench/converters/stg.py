"""Converter for Standard Task Graph Set (STG) text format.

STG format (Kasahara Lab, Waseda University):
  Line 1: number_of_tasks (excluding entry/exit)
  Subsequent lines: task_id  processing_time  num_predecessors  [pred_id ...]

With communication costs variant:
  task_id  processing_time  num_predecessors
  pred_id_1  comm_cost_1
  pred_id_2  comm_cost_2
  ...
"""
from __future__ import annotations

from pathlib import Path

from saga import TaskGraph, TaskGraphNode, TaskGraphEdge


def parse_stg(text: str, has_comm_costs: bool = False) -> TaskGraph:
    """Parse an STG-format string into a SAGA TaskGraph.

    Args:
        text: STG format text content
        has_comm_costs: If True, parse communication costs on separate lines

    Returns:
        SAGA TaskGraph
    """
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]

    if not has_comm_costs:
        return _parse_stg_simple(lines)
    else:
        return _parse_stg_with_comm(lines)


def _parse_stg_simple(lines: list[str]) -> TaskGraph:
    """Parse simple STG format (costs on same line)."""
    # First line is the number of inner tasks
    tokens = lines[0].split()
    # Remaining lines are task definitions
    tasks = []
    dependencies = []

    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 3:
            continue
        task_id = int(parts[0])
        cost = float(parts[1])
        num_preds = int(parts[2])
        preds = [int(parts[3 + i]) for i in range(num_preds)]

        tasks.append(TaskGraphNode(name=f"T{task_id}", cost=cost))
        for pred_id in preds:
            dependencies.append(
                TaskGraphEdge(source=f"T{pred_id}", target=f"T{task_id}", size=0.0)
            )

    return TaskGraph(tasks=frozenset(tasks), dependencies=frozenset(dependencies))


def _parse_stg_with_comm(lines: list[str]) -> TaskGraph:
    """Parse STG format with communication costs on separate lines."""
    tasks = []
    dependencies = []

    i = 0
    # Skip the first line (number of tasks)
    if lines[i].split()[0].isdigit() and len(lines[i].split()) == 1:
        i += 1

    while i < len(lines):
        parts = lines[i].split()
        if len(parts) < 3:
            i += 1
            continue

        task_id = int(parts[0])
        cost = float(parts[1])
        num_preds = int(parts[2])

        tasks.append(TaskGraphNode(name=f"T{task_id}", cost=cost))

        for j in range(num_preds):
            i += 1
            dep_parts = lines[i].split()
            pred_id = int(dep_parts[0])
            comm_cost = float(dep_parts[1]) if len(dep_parts) > 1 else 0.0
            dependencies.append(
                TaskGraphEdge(source=f"T{pred_id}", target=f"T{task_id}", size=comm_cost)
            )
        i += 1

    return TaskGraph(tasks=frozenset(tasks), dependencies=frozenset(dependencies))


def load_stg_file(path: Path, has_comm_costs: bool = False) -> TaskGraph:
    """Load an STG file from disk."""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    return parse_stg(text, has_comm_costs=has_comm_costs)
