"""Synthetic cost generators for structure-only DAGs."""
from __future__ import annotations

import random
from abc import ABC, abstractmethod

from saga import TaskGraph, TaskGraphNode, TaskGraphEdge


class CostGenerator(ABC):
    """Base class for cost generators."""

    @abstractmethod
    def task_cost(self) -> float:
        """Generate a single task cost."""

    @abstractmethod
    def edge_cost(self) -> float:
        """Generate a single edge (communication) cost."""

    def apply(self, task_graph: TaskGraph) -> TaskGraph:
        """Return a new TaskGraph with generated costs applied."""
        new_tasks = frozenset(
            TaskGraphNode(name=t.name, cost=self.task_cost())
            for t in task_graph.tasks
        )
        new_deps = frozenset(
            TaskGraphEdge(source=d.source, target=d.target, size=self.edge_cost())
            for d in task_graph.dependencies
        )
        return TaskGraph(tasks=new_tasks, dependencies=new_deps)


class UniformCostGenerator(CostGenerator):
    """Uniform random costs in [low, high]."""

    def __init__(self, low: float = 1.0, high: float = 100.0, seed: int | None = None):
        self.low = low
        self.high = high
        self._rng = random.Random(seed)

    def task_cost(self) -> float:
        return self._rng.uniform(self.low, self.high)

    def edge_cost(self) -> float:
        return self._rng.uniform(self.low, self.high)


class GaussianCostGenerator(CostGenerator):
    """Clipped Gaussian costs."""

    def __init__(
        self,
        mean: float = 50.0,
        std: float = 15.0,
        min_val: float = 1.0,
        max_val: float = 100.0,
        seed: int | None = None,
    ):
        self.mean = mean
        self.std = std
        self.min_val = min_val
        self.max_val = max_val
        self._rng = random.Random(seed)

    def _sample(self) -> float:
        val = self._rng.gauss(self.mean, self.std)
        return max(self.min_val, min(self.max_val, val))

    def task_cost(self) -> float:
        return self._sample()

    def edge_cost(self) -> float:
        return self._sample()
