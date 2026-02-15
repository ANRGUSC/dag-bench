"""Helper for hand-coding DAGs from paper figures and tables.

Provides a fluent builder API for constructing TaskGraphs from
descriptions found in research papers.
"""
from __future__ import annotations

from saga import TaskGraph, TaskGraphNode, TaskGraphEdge


class DAGBuilder:
    """Fluent builder for constructing TaskGraphs manually.

    Usage:
        dag = (DAGBuilder("my_dag")
            .task("A", 10.0)
            .task("B", 20.0)
            .task("C", 15.0)
            .edge("A", "B", 5.0)
            .edge("A", "C", 3.0)
            .build())
    """

    def __init__(self, name: str = "manual_dag"):
        self.name = name
        self._tasks: dict[str, float] = {}
        self._edges: list[tuple[str, str, float]] = []

    def task(self, name: str, cost: float = 0.0) -> DAGBuilder:
        """Add a task node."""
        self._tasks[name] = cost
        return self

    def tasks(self, *task_defs: tuple[str, float]) -> DAGBuilder:
        """Add multiple tasks at once. Each is (name, cost)."""
        for name, cost in task_defs:
            self._tasks[name] = cost
        return self

    def edge(self, source: str, target: str, size: float = 0.0) -> DAGBuilder:
        """Add a dependency edge."""
        self._edges.append((source, target, size))
        # Auto-register nodes
        self._tasks.setdefault(source, 0.0)
        self._tasks.setdefault(target, 0.0)
        return self

    def edges(self, *edge_defs: tuple[str, str, float]) -> DAGBuilder:
        """Add multiple edges at once. Each is (source, target, size)."""
        for src, tgt, size in edge_defs:
            self.edge(src, tgt, size)
        return self

    def chain(self, *task_names: str, edge_size: float = 0.0) -> DAGBuilder:
        """Add a linear chain of tasks: A -> B -> C -> ..."""
        for i in range(len(task_names) - 1):
            self.edge(task_names[i], task_names[i + 1], edge_size)
        return self

    def fan_out(self, source: str, *targets: str, edge_size: float = 0.0) -> DAGBuilder:
        """Fan out from one source to multiple targets."""
        for tgt in targets:
            self.edge(source, tgt, edge_size)
        return self

    def fan_in(self, *sources: str, target: str, edge_size: float = 0.0) -> DAGBuilder:
        """Fan in from multiple sources to one target."""
        for src in sources:
            self.edge(src, target, edge_size)
        return self

    def build(self) -> TaskGraph:
        """Build the TaskGraph."""
        task_nodes = frozenset(
            TaskGraphNode(name=name, cost=cost)
            for name, cost in self._tasks.items()
        )
        task_edges = frozenset(
            TaskGraphEdge(source=src, target=tgt, size=size)
            for src, tgt, size in self._edges
        )
        return TaskGraph(tasks=task_nodes, dependencies=task_edges)
