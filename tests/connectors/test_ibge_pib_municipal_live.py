"""One-observation live probe for the official IBGE SIDRA endpoint."""

import pytest

from public_data_gateway.connectors.demografico.ibge_pib_municipal import (
    IBGEPibMunicipalConnector,
)


@pytest.mark.live
def test_ibge_pib_municipal_verify_live_reads_exactly_one_observation() -> None:
    report = IBGEPibMunicipalConnector().verify_live()
    assert report["source_id"] == "ibge_pib_municipal"
    assert report["status"] == "PASS", report
    assert report["observation_count"] == 1
    assert report["id_municipio"] == "3550308"
    assert report["ano"] == 2023
    assert report["valor_amostra"] > 0
