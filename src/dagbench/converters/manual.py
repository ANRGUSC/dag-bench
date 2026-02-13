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


def gaussian_elimination_dag(n: int = 5) -> TaskGraph:
    """Generate a Gaussian Elimination DAG for an n x n matrix.

    Classic benchmark from Topcuoglu et al. (HEFT, 2002).
    The DAG has n rounds. Round k has one pivot task and (n-k-1) elimination tasks.

    Total tasks: n + n*(n-1)/2 = n*(n+1)/2
    """
    builder = DAGBuilder(f"gauss_elim_{n}")
    task_id = 0

    # Track tasks per round for dependency wiring
    pivot_tasks: list[str] = []
    elim_tasks: list[list[str]] = []

    for k in range(n):
        # Pivot task for round k
        pivot_name = f"pivot_{k}"
        # Pivot cost scales with remaining columns
        pivot_cost = float(2 * (n - k) - 1)
        builder.task(pivot_name, pivot_cost)
        pivot_tasks.append(pivot_name)

        # Pivot depends on previous round's pivot
        if k > 0:
            builder.edge(pivot_tasks[k - 1], pivot_name, float(n - k))

        # Pivot depends on previous round's elimination tasks that feed into this column
        if k > 0 and elim_tasks[k - 1]:
            for prev_elim in elim_tasks[k - 1]:
                builder.edge(prev_elim, pivot_name, float(n - k))

        # Elimination tasks for round k
        round_elims = []
        for j in range(k + 1, n):
            elim_name = f"elim_{k}_{j}"
            elim_cost = float(2 * (n - k) - 1)
            builder.task(elim_name, elim_cost)
            round_elims.append(elim_name)

            # Depends on this round's pivot
            builder.edge(pivot_name, elim_name, float(n - k))

            # Depends on previous round's elimination for same column
            if k > 0:
                prev_elim_name = f"elim_{k-1}_{j}"
                if prev_elim_name in builder._tasks:
                    builder.edge(prev_elim_name, elim_name, float(n - k))

        elim_tasks.append(round_elims)

    return builder.build()


def fft_dag(num_points: int = 8) -> TaskGraph:
    """Generate an FFT butterfly DAG.

    The FFT DAG has log2(num_points) stages with num_points/2 butterfly
    operations per stage.

    Args:
        num_points: Must be a power of 2 (8, 16, 32, ...)
    """
    import math

    assert num_points > 0 and (num_points & (num_points - 1)) == 0, \
        "num_points must be a power of 2"

    stages = int(math.log2(num_points))
    builder = DAGBuilder(f"fft_{num_points}")

    # Input tasks
    for i in range(num_points):
        builder.task(f"in_{i}", 1.0)

    # Butterfly stages
    prev_tasks = [f"in_{i}" for i in range(num_points)]

    for s in range(stages):
        curr_tasks = []
        block_size = 2 ** (s + 1)
        half = block_size // 2

        for b in range(0, num_points, block_size):
            for i in range(half):
                bf_name = f"bf_s{s}_b{b}_i{i}"
                builder.task(bf_name, 2.0)  # Butterfly: multiply + add
                curr_tasks.append(bf_name)

                # Each butterfly takes two inputs
                idx1 = b + i
                idx2 = b + i + half
                builder.edge(prev_tasks[idx1], bf_name, 1.0)
                builder.edge(prev_tasks[idx2], bf_name, 1.0)

        # Map butterfly outputs to positions for next stage
        next_tasks = [""] * num_points
        bf_idx = 0
        for b in range(0, num_points, block_size):
            for i in range(half):
                # Each butterfly produces two outputs (top and bottom)
                # For simplicity, we model each butterfly as producing one output
                # that feeds both positions
                next_tasks[b + i] = curr_tasks[bf_idx]
                next_tasks[b + i + half] = curr_tasks[bf_idx]
                bf_idx += 1
        prev_tasks = next_tasks

    # Output tasks
    for i in range(num_points):
        out_name = f"out_{i}"
        builder.task(out_name, 1.0)
        builder.edge(prev_tasks[i], out_name, 1.0)

    return builder.build()


def lu_decomposition_dag(n: int = 4) -> TaskGraph:
    """Generate a tiled LU decomposition DAG.

    For an n x n tile grid, tasks are:
    - GETRF(k): Factor diagonal block
    - TRSM_L(k,j): Solve L panel (row)
    - TRSM_U(k,i): Solve U panel (column)
    - GEMM(k,i,j): Update trailing submatrix
    """
    builder = DAGBuilder(f"lu_decomp_{n}")

    for k in range(n):
        getrf = f"GETRF_{k}"
        builder.task(getrf, 10.0)

        # GETRF depends on previous GEMM updates to diagonal
        if k > 0:
            builder.edge(f"GEMM_{k-1}_{k}_{k}", getrf, 2.0)

        for j in range(k + 1, n):
            trsm_l = f"TRSM_L_{k}_{j}"
            builder.task(trsm_l, 6.0)
            builder.edge(getrf, trsm_l, 2.0)
            if k > 0:
                builder.edge(f"GEMM_{k-1}_{k}_{j}", trsm_l, 2.0)

        for i in range(k + 1, n):
            trsm_u = f"TRSM_U_{k}_{i}"
            builder.task(trsm_u, 6.0)
            builder.edge(getrf, trsm_u, 2.0)
            if k > 0:
                builder.edge(f"GEMM_{k-1}_{i}_{k}", trsm_u, 2.0)

        for i in range(k + 1, n):
            for j in range(k + 1, n):
                gemm = f"GEMM_{k}_{i}_{j}"
                builder.task(gemm, 8.0)
                builder.edge(f"TRSM_L_{k}_{j}", gemm, 2.0)
                builder.edge(f"TRSM_U_{k}_{i}", gemm, 2.0)

    return builder.build()


def cholesky_dag(n: int = 4) -> TaskGraph:
    """Generate a tiled Cholesky factorization DAG.

    For an n x n tile grid:
    - POTRF(k): Cholesky factorization of diagonal tile
    - TRSM(k,i): Triangular solve
    - SYRK(k,i): Symmetric rank-k update
    - GEMM(k,i,j): General matrix multiply
    """
    builder = DAGBuilder(f"cholesky_{n}")

    for k in range(n):
        potrf = f"POTRF_{k}"
        builder.task(potrf, 10.0)

        if k > 0:
            builder.edge(f"SYRK_{k-1}_{k}", potrf, 2.0)

        for i in range(k + 1, n):
            trsm = f"TRSM_{k}_{i}"
            builder.task(trsm, 6.0)
            builder.edge(potrf, trsm, 2.0)
            if k > 0:
                builder.edge(f"GEMM_{k-1}_{k}_{i}", trsm, 2.0)

        for i in range(k + 1, n):
            syrk = f"SYRK_{k}_{i}"
            builder.task(syrk, 4.0)
            builder.edge(f"TRSM_{k}_{i}", syrk, 2.0)

            for j in range(k + 1, i):
                gemm = f"GEMM_{k}_{j}_{i}"
                builder.task(gemm, 8.0)
                builder.edge(f"TRSM_{k}_{j}", gemm, 2.0)
                builder.edge(f"TRSM_{k}_{i}", gemm, 2.0)

    return builder.build()


def mapreduce_dag(
    num_mappers: int = 4,
    num_reducers: int = 2,
    map_cost: float = 10.0,
    reduce_cost: float = 20.0,
    shuffle_cost: float = 5.0,
) -> TaskGraph:
    """Generate a MapReduce DAG.

    Structure: Split -> Map[N] -> Shuffle -> Reduce[M] -> Merge
    """
    builder = DAGBuilder(f"mapreduce_{num_mappers}m_{num_reducers}r")

    builder.task("Split", 2.0)
    builder.task("Merge", 2.0)

    # Map phase
    for i in range(num_mappers):
        mapper = f"Map_{i}"
        builder.task(mapper, map_cost)
        builder.edge("Split", mapper, 1.0)

    # Shuffle barrier
    builder.task("Shuffle", shuffle_cost)
    for i in range(num_mappers):
        builder.edge(f"Map_{i}", "Shuffle", 2.0)

    # Reduce phase
    for j in range(num_reducers):
        reducer = f"Reduce_{j}"
        builder.task(reducer, reduce_cost)
        builder.edge("Shuffle", reducer, 2.0)
        builder.edge(reducer, "Merge", 1.0)

    return builder.build()
