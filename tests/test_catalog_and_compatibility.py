"""Tests for catalog metadata, solution packs, and backward-compatibility shims."""

from brazil_data_gateway import BrazilData
from public_data_gateway import PublicData
from public_data_gateway.core.catalog import CatalogRegistry
from public_data_gateway.core.models import SourceEntry


def test_list_sources_pack_and_legacy_product_alias():
    """Verify pack_id and legacy product_id return identical curated catalog slices."""
    registry = CatalogRegistry()
    by_pack = registry.list_sources(pack_id="P01")
    by_product = registry.list_sources(product_id="P01")

    assert len(by_pack) >= 49
    assert [s.source_id for s in by_pack] == [s.source_id for s in by_product]


def test_sdk_facades_preserve_pack_and_product_filters():
    """Verify PublicData and BrazilData shims behave identically for pack/product queries."""
    pdg = PublicData()
    bdg = BrazilData()

    pdg_sources = pdg.list_sources(pack_id="P02")
    bdg_sources = bdg.list_sources(product_id="P02")

    assert len(pdg_sources) > 0
    assert [s.source_id for s in pdg_sources] == [s.source_id for s in bdg_sources]


def test_source_entry_accepts_solution_and_legacy_product_packs():
    """Verify SourceEntry accepts both solution_packs and product_packs field names."""
    base_kwargs = {
        "source_id": "test_source",
        "name": "Test Source",
        "agency": "TEST",
        "official_url": "https://example.gov.br",
        "segment_id": "01_teste",
        "segment_name": "Teste",
        "access_mode": "rest_api",
        "protocol": "https_json",
    }

    entry_new = SourceEntry(**base_kwargs, solution_packs=["P01", "P05"])
    entry_legacy = SourceEntry(**base_kwargs, product_packs=["P01", "P05"])

    assert entry_new.solution_packs == ["P01", "P05"]
    assert entry_legacy.solution_packs == ["P01", "P05"]
