"""Converters for importing DAGs from various formats."""
from dagbench.converters.stg import parse_stg, load_stg_file
from dagbench.converters.dot import parse_dot, load_dot_file
from dagbench.converters.csv_conv import (
    parse_edge_list_csv,
    parse_task_and_edge_csv,
    load_edge_list_csv,
)
from dagbench.converters.manual import DAGBuilder

# Backward compat: generators moved to dagbench.generators
# Lazy import to avoid circular dependency (generators.classic -> converters.manual)
_GENERATOR_NAMES = {
    "gaussian_elimination_dag",
    "fft_dag",
    "lu_decomposition_dag",
    "cholesky_dag",
    "mapreduce_dag",
}


def __getattr__(name):
    if name in _GENERATOR_NAMES:
        from dagbench.generators import classic
        return getattr(classic, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "parse_stg",
    "load_stg_file",
    "parse_dot",
    "load_dot_file",
    "parse_edge_list_csv",
    "parse_task_and_edge_csv",
    "load_edge_list_csv",
    "DAGBuilder",
    "gaussian_elimination_dag",
    "fft_dag",
    "lu_decomposition_dag",
    "cholesky_dag",
    "mapreduce_dag",
]
