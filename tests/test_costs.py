"""Tests for dagbench.costs module."""
from saga import TaskGraph

from dagbench.costs import UniformCostGenerator, GaussianCostGenerator


def _structure_only() -> TaskGraph:
    return TaskGraph.create(
        tasks=[("A", 0.0), ("B", 0.0), ("C", 0.0)],
        dependencies=[("A", "B", 0.0), ("B", "C", 0.0)],
    )


class TestUniformCostGenerator:
    def test_apply(self):
        gen = UniformCostGenerator(low=10.0, high=100.0, seed=42)
        tg = _structure_only()
        result = gen.apply(tg)
        for task in result.tasks:
            assert 10.0 <= task.cost <= 100.0
        for dep in result.dependencies:
            assert 10.0 <= dep.size <= 100.0

    def test_deterministic_with_seed(self):
        gen1 = UniformCostGenerator(low=1.0, high=50.0, seed=123)
        gen2 = UniformCostGenerator(low=1.0, high=50.0, seed=123)
        tg = _structure_only()
        r1 = gen1.apply(tg)
        r2 = gen2.apply(tg)
        costs1 = sorted(t.cost for t in r1.tasks)
        costs2 = sorted(t.cost for t in r2.tasks)
        assert costs1 == costs2


class TestGaussianCostGenerator:
    def test_apply(self):
        gen = GaussianCostGenerator(mean=50.0, std=15.0, min_val=1.0, max_val=100.0, seed=42)
        tg = _structure_only()
        result = gen.apply(tg)
        for task in result.tasks:
            assert 1.0 <= task.cost <= 100.0
        for dep in result.dependencies:
            assert 1.0 <= dep.size <= 100.0
