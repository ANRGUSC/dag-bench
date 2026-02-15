"""Tests for generator import paths (new and backward-compat)."""


class TestGeneratorsImport:
    def test_import_from_generators(self):
        """Canonical import from dagbench.generators works."""
        from dagbench.generators import (
            gaussian_elimination_dag,
            fft_dag,
            lu_decomposition_dag,
            cholesky_dag,
            mapreduce_dag,
            random_layered_dag,
        )
        # Verify they're callable
        assert callable(gaussian_elimination_dag)
        assert callable(random_layered_dag)

    def test_backward_compat_from_converters(self):
        """Old import from dagbench.converters still works."""
        from dagbench.converters import (
            gaussian_elimination_dag,
            fft_dag,
            lu_decomposition_dag,
            cholesky_dag,
            mapreduce_dag,
        )
        assert callable(gaussian_elimination_dag)

    def test_backward_compat_from_converters_manual(self):
        """Old import from dagbench.converters.manual still works for DAGBuilder."""
        from dagbench.converters.manual import DAGBuilder
        assert callable(DAGBuilder)

    def test_generators_match_converters(self):
        """Both import paths return the same function objects."""
        from dagbench.generators import gaussian_elimination_dag as gen_gauss
        from dagbench.converters import gaussian_elimination_dag as conv_gauss
        assert gen_gauss is conv_gauss
