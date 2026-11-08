"""Live fail-hard integration tests for Compras.gov.br CATMAT/CATSER connector.

Verifies:
1. Live request to official Compras.gov.br CATMAT items endpoint
2. Live request to official Compras.gov.br CATSER services endpoint
3. Complete verify_live() probe status
"""

import pytest
import requests

from public_data_gateway.connectors.compras_publicas.comprasgov_catmat import (
    ComprasGovCatmatConnector,
)


@pytest.mark.live
def test_comprasgov_live_catmat_endpoint() -> None:
    """Validate live connectivity and response payload from Compras.gov.br CATMAT items."""
    url = "https://dadosabertos.compras.gov.br/modulo-material/4_consultarItemMaterial?pagina=1&tamanhoPagina=10"
    headers = {"User-Agent": "PublicDataGateway/0.1.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=20, verify=False)
        assert resp.status_code == 200, f"HTTP Error {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "resultado" in data
        assert data.get("totalRegistros", 0) > 0
        items = data["resultado"]
        assert len(items) > 0
        sample = items[0]
        assert "codigoItem" in sample
        assert "nomePdm" in sample
    except requests.exceptions.RequestException as exc:
        pytest.fail(f"Live CATMAT probe failed against Compras.gov.br: {exc}")


@pytest.mark.live
def test_comprasgov_live_catser_endpoint() -> None:
    """Validate live connectivity and response payload from Compras.gov.br CATSER services."""
    url = "https://dadosabertos.compras.gov.br/modulo-servico/6_consultarItemServico?pagina=1&tamanhoPagina=10"
    headers = {"User-Agent": "PublicDataGateway/0.1.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=20, verify=False)
        assert resp.status_code == 200, f"HTTP Error {resp.status_code}: {resp.text}"
        data = resp.json()
        assert "resultado" in data
        assert data.get("totalRegistros", 0) > 0
        items = data["resultado"]
        assert len(items) > 0
        sample = items[0]
        assert "codigoServico" in sample
        assert "nomeServico" in sample
    except requests.exceptions.RequestException as exc:
        pytest.fail(f"Live CATSER probe failed against Compras.gov.br: {exc}")


@pytest.mark.live
def test_comprasgov_verify_live_method() -> None:
    """Validate ComprasGovCatmatConnector.verify_live() fail-hard report."""
    conn = ComprasGovCatmatConnector()
    report = conn.verify_live()
    assert report["source_id"] == "comprasgov_catmat"
    assert report["status"] == "PASS", f"verify_live failed: {report}"
    assert "catmat_itens" in report["endpoints"]
    assert report["endpoints"]["catmat_itens"]["passed"] is True
    assert "catser_itens" in report["endpoints"]
    assert report["endpoints"]["catser_itens"]["passed"] is True
