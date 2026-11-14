"""Live fail-hard integration tests (1-line probe) for Wave 10.5B Block 3 connectors."""

from __future__ import annotations

import pytest

from public_data_gateway.core.connector import get_connector

pytestmark = pytest.mark.live


@pytest.mark.parametrize(
    "source_id",
    [
        "overpass_osm",
        "osrm_routing",
        "open_meteo",
        "nasa_power",
        "open_elevation",
        "huggingface_inference",
        "alpha_vantage",
        "nasa_youtube_open_ml",
        "upgini_adapter",
    ],
)
def test_wave10_5b_live_endpoint_reachable(source_id: str) -> None:
    """Every Wave 10.5B connector must pass live HTTP 200 test_connection()."""
    conn = get_connector(source_id)
    assert conn.test_connection() is True, f"Live connection failed for {source_id}"
