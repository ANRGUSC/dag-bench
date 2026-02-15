"""Converter for Graphviz DOT format → structure-only DAGBench TaskGraph.

Parses simple DOT digraphs to extract nodes and edges.
Node labels become task names; weights are extracted from attributes if present.
"""
from __future__ import annotations

import re
from pathlib import Path

from saga import TaskGraph, TaskGraphNode, TaskGraphEdge


def parse_dot(text: str) -> TaskGraph:
    """Parse a DOT digraph string into a SAGA TaskGraph.

    Extracts nodes and edges. Node/edge weights come from 'weight' or 'label'
    attributes if present, otherwise default to 0.0.

    Args:
        text: DOT format text content

    Returns:
        SAGA TaskGraph (structure-only if no weights found)
    """
    nodes: dict[str, float] = {}
    edges: list[tuple[str, str, float]] = []

    # Remove comments
    text = re.sub(r'//.*', '', text)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)

    # Find edges: "A" -> "B" or A -> B
    edge_pattern = re.compile(
        r'"?(\w+)"?\s*->\s*"?(\w+)"?\s*(?:\[([^\]]*)\])?'
    )
    for match in edge_pattern.finditer(text):
        src, tgt = match.group(1), match.group(2)
        attrs = match.group(3) or ""
        weight = _extract_weight(attrs)
        edges.append((src, tgt, weight))
        # Auto-register nodes from edges
        nodes.setdefault(src, 0.0)
        nodes.setdefault(tgt, 0.0)

    # Find standalone node definitions: A [label="...", weight=5]
    node_pattern = re.compile(
        r'^\s*"?(\w+)"?\s*\[([^\]]*)\]\s*;?\s*$', re.MULTILINE
    )
    for match in node_pattern.finditer(text):
        name = match.group(1)
        attrs = match.group(2)
        # Skip if this looks like an edge attribute
        if "->" in text[max(0, match.start() - 50):match.start()]:
            continue
        weight = _extract_weight(attrs)
        nodes[name] = weight

    task_nodes = frozenset(
        TaskGraphNode(name=name, cost=cost) for name, cost in nodes.items()
    )
    task_edges = frozenset(
        TaskGraphEdge(source=src, target=tgt, size=size) for src, tgt, size in edges
    )

    return TaskGraph(tasks=task_nodes, dependencies=task_edges)


def _extract_weight(attrs: str) -> float:
    """Extract weight from DOT attribute string."""
    # Try 'weight' attribute first
    m = re.search(r'weight\s*=\s*"?([0-9.]+)"?', attrs)
    if m:
        return float(m.group(1))
    # Try 'cost' attribute
    m = re.search(r'cost\s*=\s*"?([0-9.]+)"?', attrs)
    if m:
        return float(m.group(1))
    return 0.0


def load_dot_file(path: Path) -> TaskGraph:
    """Load a DOT file from disk."""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    return parse_dot(text)
