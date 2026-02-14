"""Tests for dagbench.generators.random_dag module."""
import networkx as nx
import pytest

from dagbench.generators.random_dag import random_layered_dag


class TestRandomLayeredDag:
    def test_valid_dag(self):
        """Generated graph must be a valid DAG (acyclic)."""
        tg = random_layered_dag(depth=4, seed=100)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)

    def test_seed_reproducibility(self):
        """Same seed produces identical graphs."""
        tg1 = random_layered_dag(depth=4, seed=42)
        tg2 = random_layered_dag(depth=4, seed=42)
        assert {t.name for t in tg1.tasks} == {t.name for t in tg2.tasks}
        assert {(e.source, e.target) for e in tg1.dependencies} == \
               {(e.source, e.target) for e in tg2.dependencies}
        # Costs should also match
        costs1 = {t.name: t.cost for t in tg1.tasks}
        costs2 = {t.name: t.cost for t in tg2.tasks}
        assert costs1 == costs2

    def test_different_seeds_differ(self):
        """Different seeds produce different graphs."""
        tg1 = random_layered_dag(depth=4, seed=42)
        tg2 = random_layered_dag(depth=4, seed=99)
        edges1 = {(e.source, e.target) for e in tg1.dependencies}
        edges2 = {(e.source, e.target) for e in tg2.dependencies}
        assert edges1 != edges2

    def test_connectivity_no_orphans(self):
        """Every non-source node has at least one incoming edge,
        every non-sink node has at least one outgoing edge."""
        tg = random_layered_dag(depth=6, width_min=3, width_max=8, seed=101)
        G = tg.graph
        sources = [n for n in G.nodes if G.in_degree(n) == 0]
        sinks = [n for n in G.nodes if G.out_degree(n) == 0]
        # Should have exactly one source and one sink
        assert len(sources) == 1, f"Expected 1 source, got {len(sources)}: {sources}"
        assert len(sinks) == 1, f"Expected 1 sink, got {len(sinks)}: {sinks}"

    def test_all_costs_positive(self):
        """All task costs and edge sizes must be positive."""
        tg = random_layered_dag(depth=5, seed=102)
        for t in tg.tasks:
            assert t.cost > 0, f"Task {t.name} has non-positive cost {t.cost}"
        for e in tg.dependencies:
            assert e.size > 0, f"Edge {e.source}->{e.target} has non-positive size {e.size}"

    def test_delta_lvl_one(self):
        """delta_lvl=1 should still produce a valid connected DAG."""
        tg = random_layered_dag(depth=4, delta_lvl=1, seed=103)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) > 0

    def test_minimal_dag(self):
        """depth=1, width_min=width_max=1 should give exactly 3 nodes."""
        tg = random_layered_dag(depth=1, width_min=1, width_max=1, seed=104)
        assert len(tg.tasks) == 3  # source + 1 interior + sink

    def test_heft_schedulable(self):
        """Generated DAGs should be schedulable with HEFT."""
        from saga import Network
        from saga.schedulers.data import ProblemInstance
        from saga.schedulers.heft import HeftScheduler

        scheduler = HeftScheduler()
        net = Network.create(
            nodes=[("N0", 1.0), ("N1", 2.0), ("N2", 3.0)],
            edges=[("N0", "N1", 100.0), ("N0", "N2", 100.0), ("N1", "N2", 100.0)],
        )

        for seed in [42, 43, 44, 45]:
            tg = random_layered_dag(depth=4, seed=seed)
            schedule = scheduler.schedule(net, tg)
            assert schedule.makespan > 0

    def test_large_dag(self):
        """Large DAG should still be valid."""
        tg = random_layered_dag(depth=15, width_min=6, width_max=15, seed=50)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) > 50

    def test_high_ccr(self):
        """High CCR should produce larger comm costs relative to comp costs."""
        tg = random_layered_dag(depth=4, ccr=5.0, seed=105)
        avg_comp = sum(t.cost for t in tg.tasks) / len(tg.tasks)
        avg_comm = sum(e.size for e in tg.dependencies) / len(tg.dependencies)
        # With CCR=5.0, comm should be much larger than comp
        assert avg_comm > avg_comp
