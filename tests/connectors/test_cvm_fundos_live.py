"""Live fail-hard tests against official CVM Open Data portal endpoints."""

from __future__ import annotations

import pytest
import requests
import urllib3

from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
    CVM_CADASTRO_URL,
    CVM_INF_DIARIO_PORTAL_URL,
    CvmFundosConnector,
)

urllib3.disable_warnings()


@pytest.mark.live
def test_cvm_fundos_live_smoke_connection() -> None:
    """Verify live connectivity against CVM Open Data portal and fail-hard on HTTP 5xx."""
    conn = CvmFundosConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["cadastro_status"] == 200
    assert report["informe_status"] == 200


@pytest.mark.live
def test_cvm_cad_fi_live_stream_header() -> None:
    """Stream first bytes of official CVM cad_fi.csv and ensure mandatory regulatory headers exist."""
    headers = {
        "User-Agent": "PublicDataGateway/1.0 (Government Integration; contact@publicdata.gateway)"
    }
    resp = requests.get(CVM_CADASTRO_URL, headers=headers, stream=True, timeout=30, verify=False)
    assert resp.status_code == 200, f"CVM Cadastro returned unexpected HTTP {resp.status_code}"

    chunk = next(resp.iter_content(chunk_size=4096))
    decoded = chunk.decode("latin-1")
    header_line = decoded.splitlines()[0]
    cols = header_line.split(";")

    assert "CNPJ_FUNDO" in cols, "CNPJ_FUNDO must exist in official CVM header"
    assert "DENOM_SOCIAL" in cols, "DENOM_SOCIAL must exist in official CVM header"
    assert "SIT" in cols, "SIT must exist in official CVM header"
    assert "CLASSE" in cols, "CLASSE must exist in official CVM header"
    assert "GESTOR" in cols, "GESTOR must exist in official CVM header"


@pytest.mark.live
def test_cvm_inf_diario_live_bulk_zip() -> None:
    """Verify official CVM daily report monthly bulk ZIP file exists and has size > 1MB."""
    headers = {
        "User-Agent": "PublicDataGateway/1.0 (Government Integration; contact@publicdata.gateway)"
    }
    url = f"{CVM_INF_DIARIO_PORTAL_URL}inf_diario_fi_202409.zip"
    resp = requests.head(url, headers=headers, timeout=30, verify=False)
    assert resp.status_code == 200, f"CVM Informe Diário returned HTTP {resp.status_code}"

    content_length = int(resp.headers.get("Content-Length", 0))
    assert content_length > 1_000_000, f"Expected > 1MB for daily report zip, got {content_length} bytes"
