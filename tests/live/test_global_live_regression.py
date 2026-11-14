"""Global live regression battery with systematic ID rotation across core connectors.

Run explicitly via:
    pytest tests/live/test_global_live_regression.py -v
"""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.agregadores.brasilapi_core.connector import (
    BrasilAPICoreConnector,
)
from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
    DadosGovCkanConnector,
)
from public_data_gateway.connectors.compras_publicas.pncp_contratacoes.connector import (
    PNCPContratacoesConnector,
)
from public_data_gateway.connectors.demografico.ibge_localidades.connector import (
    IBGELocalidadesConnector,
)
from public_data_gateway.connectors.macrofinanceiro.bcb_olinda_ptax.connector import (
    BCBOlindaPTAXConnector,
)
from public_data_gateway.connectors.macrofinanceiro.bcb_sgs.connector import (
    BCBSGSConnector,
)
from public_data_gateway.core.config import GatewaySettings


@pytest.mark.live
@pytest.mark.parametrize(
    ("cep", "expected_uf"),
    [("01001000", "SP"), ("20040020", "RJ"), ("70040010", "DF")],
)
def test_live_rotation_brasilapi_cep(tmp_path, cep: str, expected_uf: str) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    conn = BrasilAPICoreConnector(settings=settings)
    assert conn.test_connection() is True

    resp = httpx.get(f"https://brasilapi.com.br/api/cep/v2/{cep}", timeout=20.0)
    resp.raise_for_status()
    payload = resp.json()
    assert payload.get("state") == expected_uf


@pytest.mark.live
@pytest.mark.parametrize("id_municipio", ["3550308", "3304557", "5300108"])
def test_live_rotation_ibge_localidades(tmp_path, id_municipio: str) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    loc = IBGELocalidadesConnector(settings=settings)
    mun = loc.get_municipio(id_municipio)
    assert mun is not None
    assert str(mun.get("id_municipio", mun.get("id", ""))) == id_municipio


@pytest.mark.live
@pytest.mark.parametrize("moeda", ["USD", "EUR"])
def test_live_rotation_bcb_ptax(tmp_path, moeda: str) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    ptax = BCBOlindaPTAXConnector(settings=settings)
    status = ptax.test_live_endpoints(fail_hard=True)
    assert all(status.values())

    url = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        f"CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@di,dataFinalCotacao=@df)"
        f"?@moeda='{moeda}'&@di='01-02-2025'&@df='01-10-2025'&$top=5&$format=json"
    )
    resp = httpx.get(url, timeout=20.0)
    resp.raise_for_status()
    assert len(resp.json().get("value", [])) >= 1


@pytest.mark.live
@pytest.mark.parametrize("series_code", ["432", "433"])
def test_live_rotation_bcb_sgs(tmp_path, series_code: str) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    sgs = BCBSGSConnector(settings=settings)
    status = sgs.test_live_endpoints(fail_hard=True)
    assert all(status.values())

    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados/ultimos/3?formato=json"
    resp = httpx.get(url, timeout=20.0)
    resp.raise_for_status()
    assert len(resp.json()) == 3


@pytest.mark.live
@pytest.mark.parametrize("id_municipio", ["3550308", "4106902"])
def test_live_rotation_pncp_and_dadosgov(tmp_path, id_municipio: str) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    pncp = PNCPContratacoesConnector(settings=settings)
    assert all(pncp.test_live_endpoints(fail_hard=True).values())

    ckan = DadosGovCkanConnector(settings=settings)
    res_ckan = ckan.fetch(id_municipio)
    assert isinstance(res_ckan, dict)
