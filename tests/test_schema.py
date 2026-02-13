"""Tests for dagbench.schema Pydantic models."""
from datetime import date

import pytest
from pydantic import ValidationError

from dagbench.schema import (
    WorkflowMetadata,
    Domain,
    Completeness,
    CostModel,
    ExtractionMethod,
    Provenance,
    LicenseInfo,
    NetworkInfo,
    GraphStats,
)


def _make_metadata(**overrides) -> dict:
    """Helper to create a valid metadata dict with overrides."""
    base = {
        "id": "test.workflow",
        "name": "Test Workflow",
        "description": "A test workflow",
        "domains": ["iot"],
        "provenance": {
            "source": "test",
            "extraction_method": "programmatic",
            "extractor": "test-script",
            "extraction_date": "2025-01-01",
        },
        "license": {"source_license": "Apache-2.0"},
        "completeness": "full",
        "cost_model": "deterministic",
        "network": {"included": True, "topology": "fully-connected"},
        "graph_stats": {
            "num_tasks": 10,
            "num_edges": 12,
            "depth": 4,
            "width": 3,
        },
    }
    base.update(overrides)
    return base


class TestWorkflowMetadata:
    def test_valid_metadata(self):
        data = _make_metadata()
        wf = WorkflowMetadata.model_validate(data)
        assert wf.id == "test.workflow"
        assert wf.graph_stats.num_tasks == 10
        assert Domain.IOT in wf.domains

    def test_all_domains(self):
        for domain in Domain:
            data = _make_metadata(domains=[domain.value])
            wf = WorkflowMetadata.model_validate(data)
            assert domain in wf.domains

    def test_all_completeness_values(self):
        for c in Completeness:
            data = _make_metadata(completeness=c.value)
            wf = WorkflowMetadata.model_validate(data)
            assert wf.completeness == c

    def test_all_cost_models(self):
        for cm in CostModel:
            data = _make_metadata(cost_model=cm.value)
            wf = WorkflowMetadata.model_validate(data)
            assert wf.cost_model == cm

    def test_missing_required_field(self):
        data = _make_metadata()
        del data["id"]
        with pytest.raises(ValidationError):
            WorkflowMetadata.model_validate(data)

    def test_optional_fields(self):
        data = _make_metadata(
            campaign="campaign_001",
            quality_issues=["missing edge weights"],
            tags=["small", "iot"],
        )
        wf = WorkflowMetadata.model_validate(data)
        assert wf.campaign == "campaign_001"
        assert len(wf.quality_issues) == 1
        assert "small" in wf.tags

    def test_provenance_with_paper(self):
        data = _make_metadata()
        data["provenance"]["paper_doi"] = "10.1234/test"
        data["provenance"]["paper_arxiv"] = "2403.07120"
        data["provenance"]["authors"] = ["Alice", "Bob"]
        data["provenance"]["year"] = 2024
        wf = WorkflowMetadata.model_validate(data)
        assert wf.provenance.paper_doi == "10.1234/test"
        assert wf.provenance.year == 2024


    def test_ai_generated_extraction_method(self):
        data = _make_metadata()
        data["provenance"]["extraction_method"] = "ai-generated"
        data["provenance"]["notes"] = "Structure was AI-generated."
        wf = WorkflowMetadata.model_validate(data)
        assert wf.provenance.extraction_method == ExtractionMethod.AI_GENERATED
        assert wf.provenance.notes == "Structure was AI-generated."

    def test_provenance_notes_optional(self):
        data = _make_metadata()
        wf = WorkflowMetadata.model_validate(data)
        assert wf.provenance.notes is None


class TestGraphStats:
    def test_basic(self):
        stats = GraphStats(num_tasks=10, num_edges=12, depth=4, width=3)
        assert stats.ccr is None
        assert stats.parallelism is None

    def test_with_ccr(self):
        stats = GraphStats(
            num_tasks=10, num_edges=12, depth=4, width=3, ccr=1.5, parallelism=2.5
        )
        assert stats.ccr == 1.5
        assert stats.parallelism == 2.5
