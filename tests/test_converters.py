"""Tests for dagbench.converters module."""
import networkx as nx

from dagbench.converters.stg import parse_stg
from dagbench.converters.dot import parse_dot
from dagbench.converters.csv_conv import parse_edge_list_csv, parse_task_and_edge_csv
from dagbench.converters.manual import DAGBuilder
from dagbench.generators.classic import (
    gaussian_elimination_dag,
    fft_dag,
    lu_decomposition_dag,
    cholesky_dag,
    mapreduce_dag,
)


class TestSTGParser:
    def test_simple_format(self):
        stg_text = """4
0 0 0
1 10 1 0
2 20 1 0
3 15 2 1 2
4 0 1 3"""
        tg = parse_stg(stg_text)
        assert len(tg.tasks) == 5
        # T0->T1, T0->T2, T1->T3, T2->T3, T3->T4
        assert len(tg.dependencies) == 5

    def test_with_comm_costs(self):
        stg_text = """2
0 0 0
1 10 1
0 5
2 20 1
1 3"""
        tg = parse_stg(stg_text, has_comm_costs=True)
        assert len(tg.tasks) == 3
        # Check comm cost was parsed
        for dep in tg.dependencies:
            if dep.source == "T0" and dep.target == "T1":
                assert dep.size == 5.0


class TestDOTParser:
    def test_simple_digraph(self):
        dot_text = """digraph test {
    A -> B;
    A -> C;
    B -> D;
    C -> D;
}"""
        tg = parse_dot(dot_text)
        assert len(tg.tasks) == 4
        assert len(tg.dependencies) == 4

    def test_with_weights(self):
        dot_text = """digraph test {
    A [weight=10];
    B [weight=20];
    A -> B [weight=5];
}"""
        tg = parse_dot(dot_text)
        for t in tg.tasks:
            if t.name == "A":
                assert t.cost == 10.0
            if t.name == "B":
                assert t.cost == 20.0


class TestCSVParser:
    def test_edge_list(self):
        csv_text = """source,target,weight
A,B,5
A,C,3
B,D,4
C,D,2"""
        tg = parse_edge_list_csv(csv_text)
        assert len(tg.tasks) == 4
        assert len(tg.dependencies) == 4

    def test_task_and_edge(self):
        task_csv = """name,cost
A,10
B,20
C,15"""
        edge_csv = """source,target,size
A,B,5
A,C,3"""
        tg = parse_task_and_edge_csv(task_csv, edge_csv)
        assert len(tg.tasks) == 3
        assert len(tg.dependencies) == 2
        for t in tg.tasks:
            if t.name == "A":
                assert t.cost == 10.0


class TestDAGBuilder:
    def test_fluent_api(self):
        dag = (
            DAGBuilder("test")
            .task("A", 10.0)
            .task("B", 20.0)
            .edge("A", "B", 5.0)
            .build()
        )
        assert len(dag.tasks) == 2

    def test_chain(self):
        dag = (
            DAGBuilder("test")
            .task("A", 1.0)
            .task("B", 2.0)
            .task("C", 3.0)
            .chain("A", "B", "C")
            .build()
        )
        assert len(dag.dependencies) == 2

    def test_fan_out_fan_in(self):
        dag = (
            DAGBuilder("test")
            .task("S", 1.0)
            .task("A", 2.0)
            .task("B", 2.0)
            .task("T", 1.0)
            .fan_out("S", "A", "B")
            .fan_in("A", "B", target="T")
            .build()
        )
        assert len(dag.dependencies) == 4


class TestClassicDAGs:
    def test_gaussian_elimination(self):
        tg = gaussian_elimination_dag(n=5)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) > 0

    def test_fft(self):
        tg = fft_dag(num_points=8)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) > 0

    def test_lu_decomposition(self):
        tg = lu_decomposition_dag(n=4)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) > 0

    def test_cholesky(self):
        tg = cholesky_dag(n=4)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) > 0

    def test_mapreduce(self):
        tg = mapreduce_dag(num_mappers=4, num_reducers=2)
        G = tg.graph
        assert nx.is_directed_acyclic_graph(G)
        assert len(tg.tasks) == 4 + 2 + 3  # mappers + reducers + split/shuffle/merge

    def test_all_schedulable(self):
        """All classic DAGs should be schedulable with HEFT."""
        from saga import Network
        from saga.schedulers.data import ProblemInstance
        from saga.schedulers.heft import HeftScheduler

        scheduler = HeftScheduler()
        net = Network.create(
            nodes=[("N0", 1.0), ("N1", 2.0), ("N2", 3.0)],
            edges=[("N0", "N1", 100.0), ("N0", "N2", 100.0), ("N1", "N2", 100.0)],
        )

        dags = [
            gaussian_elimination_dag(n=4),
            fft_dag(num_points=8),
            lu_decomposition_dag(n=3),
            cholesky_dag(n=3),
            mapreduce_dag(num_mappers=3, num_reducers=2),
        ]

        for tg in dags:
            schedule = scheduler.schedule(net, tg)
            assert schedule.makespan > 0
