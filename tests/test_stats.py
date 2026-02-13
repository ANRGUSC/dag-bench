"""Tests for dagbench.stats module."""
from saga import TaskGraph

from dagbench.stats import compute_graph_stats, compute_depth, compute_width, compute_ccr


def _diamond_task_graph() -> TaskGraph:
    """A -> (B, C) -> D"""
    return TaskGraph.create(
        tasks=[("A", 10.0), ("B", 20.0), ("C", 15.0), ("D", 10.0)],
        dependencies=[("A", "B", 5.0), ("A", "C", 3.0), ("B", "D", 4.0), ("C", "D", 2.0)],
    )


def _chain_task_graph() -> TaskGraph:
    """A -> B -> C -> D"""
    return TaskGraph.create(
        tasks=[("A", 10.0), ("B", 20.0), ("C", 15.0), ("D", 10.0)],
        dependencies=[("A", "B", 5.0), ("B", "C", 3.0), ("C", "D", 4.0)],
    )


def _structure_only_task_graph() -> TaskGraph:
    """All costs zero (structure-only)."""
    return TaskGraph.create(
        tasks=[("A", 0.0), ("B", 0.0)],
        dependencies=[("A", "B", 0.0)],
    )


class TestComputeDepth:
    def test_diamond(self):
        tg = _diamond_task_graph()
        depth = compute_depth(tg.graph)
        assert depth == 3  # A -> B/C -> D

    def test_chain(self):
        tg = _chain_task_graph()
        depth = compute_depth(tg.graph)
        assert depth == 4


class TestComputeWidth:
    def test_diamond(self):
        tg = _diamond_task_graph()
        width = compute_width(tg.graph)
        assert width == 2  # B and C are parallel

    def test_chain(self):
        tg = _chain_task_graph()
        width = compute_width(tg.graph)
        assert width == 1


class TestComputeCCR:
    def test_with_costs(self):
        tg = _diamond_task_graph()
        ccr = compute_ccr(tg)
        # total_comm = 5+3+4+2 = 14, total_comp = 10+20+15+10 = 55
        assert ccr is not None
        assert abs(ccr - 14.0 / 55.0) < 1e-6

    def test_structure_only(self):
        tg = _structure_only_task_graph()
        ccr = compute_ccr(tg)
        assert ccr is None


class TestComputeGraphStats:
    def test_diamond(self):
        tg = _diamond_task_graph()
        stats = compute_graph_stats(tg)
        assert stats.num_tasks == 4
        assert stats.num_edges == 4
        assert stats.depth == 3
        assert stats.width == 2
        assert stats.ccr is not None
        assert stats.parallelism is not None
        assert abs(stats.parallelism - 4.0 / 3.0) < 1e-6
