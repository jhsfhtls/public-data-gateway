"""Live 1-line connectivity tests for all 10 Wave 10.5C Block 1 Medallion connectors."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.compliance.bgpview_ip_intel import BgpviewIpIntelConnector
from public_data_gateway.connectors.compliance.rdap_registro_br import RdapRegistroBrConnector
from public_data_gateway.connectors.empresarial.cnpja_open import CnpjaOpenConnector
from public_data_gateway.connectors.empresarial.company_enrich_apollo_bdc import (
    CompanyEnrichConnector,
)
from public_data_gateway.connectors.empresarial.gleif_lei import GleifLeiConnector
from public_data_gateway.connectors.empresarial.minha_receita import MinhaReceitaConnector
from public_data_gateway.connectors.empresarial.receitaws import ReceitaWsConnector
from public_data_gateway.connectors.geografico.nominatim_osm import NominatimOsmConnector
from public_data_gateway.connectors.geografico.opencep import OpenCepConnector
from public_data_gateway.connectors.geografico.viacep import ViaCepConnector


@pytest.mark.live
def test_minha_receita_live() -> None:
    assert MinhaReceitaConnector().test_connection() is True


@pytest.mark.live
def test_cnpja_open_live() -> None:
    assert CnpjaOpenConnector().test_connection() is True


@pytest.mark.live
def test_receitaws_live() -> None:
    assert ReceitaWsConnector().test_connection() is True


@pytest.mark.live
def test_gleif_lei_live() -> None:
    assert GleifLeiConnector().test_connection() is True


@pytest.mark.live
def test_company_enrich_live() -> None:
    assert CompanyEnrichConnector().test_connection() is True


@pytest.mark.live
def test_viacep_live() -> None:
    assert ViaCepConnector().test_connection() is True


@pytest.mark.live
def test_opencep_live() -> None:
    assert OpenCepConnector().test_connection() is True


@pytest.mark.live
def test_nominatim_osm_live() -> None:
    assert NominatimOsmConnector().test_connection() is True


@pytest.mark.live
def test_rdap_registro_br_live() -> None:
    assert RdapRegistroBrConnector().test_connection() is True


@pytest.mark.live
def test_bgpview_ip_intel_live() -> None:
    assert BgpviewIpIntelConnector().test_connection() is True
