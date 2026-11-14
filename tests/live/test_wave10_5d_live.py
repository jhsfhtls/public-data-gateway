"""Live 1-line connectivity tests for all 3 Wave 10.5D Block 4 Medallion connectors."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.agregadores.gdelt_doc_api import GdeltDocApiConnector
from public_data_gateway.connectors.demografico.rest_countries import RestCountriesConnector
from public_data_gateway.connectors.educacao.openalex_api import OpenAlexApiConnector


@pytest.mark.live
def test_rest_countries_live() -> None:
    assert RestCountriesConnector().test_connection() is True


@pytest.mark.live
def test_openalex_api_live() -> None:
    assert OpenAlexApiConnector().test_connection() is True


@pytest.mark.live
def test_gdelt_doc_api_live() -> None:
    assert GdeltDocApiConnector().test_connection() is True
