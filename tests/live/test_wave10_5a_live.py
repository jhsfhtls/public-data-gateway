"""Live fail-hard integration tests (1-line probe) for Wave 10.5A Block 2 connectors."""

from __future__ import annotations

import pytest

from public_data_gateway.core.connector import get_connector

pytestmark = pytest.mark.live


@pytest.mark.parametrize(
    "source_id",
    [
        "ipeadata_odata",
        "brapi_b3",
        "awesomeapi_fx",
        "aneel_anp_ckan",
        "camara_senado_dados_abertos",
        "world_bank_v2",
        "who_gho_odata",
    ],
)
def test_wave10_5a_live_endpoint_reachable(source_id: str) -> None:
    """Every Wave 10.5A connector must pass live HTTP 200 test_connection()."""
    conn = get_connector(source_id)
    assert conn.test_connection() is True, f"Live connection failed for {source_id}"
