"""IBGE Localidades & Malha Territorial Connector.

Ingests, normalizes, validates, and materializes all 5 official territorial tables
from IBGE (municípios, distritos, subdistritos, estados, regiões) across isolated
Medallion Bronze, Silver, and Gold layers per POP_T_177, POP_T_070, and POP_T_074.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector
from public_data_gateway.core.contracts import apply_sentinel_cleaning, load_contract
from public_data_gateway.core.http_client import ResilientHttpClient
from public_data_gateway.core.normalizer import CanonicalNormalizer
from public_data_gateway.core.storage import HybridStorageEngine

logger = logging.getLogger(__name__)

IBGE_BASE_API = "https://servicodados.ibge.gov.br/api/v1/localidades"

IBGE_ENDPOINTS: dict[str, str] = {
    "municipios": f"{IBGE_BASE_API}/municipios",
    "distritos": f"{IBGE_BASE_API}/distritos?view=nivelado",
    "subdistritos": f"{IBGE_BASE_API}/subdistritos?view=nivelado",
    "estados": f"{IBGE_BASE_API}/estados?view=nivelado",
    "regioes": f"{IBGE_BASE_API}/regioes",
}

# Live 1-line verification targets for fail-hard testing (low latency, ~1KB response)
IBGE_LIVE_1LINE_ENDPOINTS: dict[str, dict[str, Any]] = {
    "regioes": {
        "url": f"{IBGE_BASE_API}/regioes/1",
        "expected_key": "sigla",
        "expected_val": "N",
    },
    "estados": {
        "url": f"{IBGE_BASE_API}/estados/13",
        "expected_key": "sigla",
        "expected_val": "AM",
    },
    "municipios": {
        "url": f"{IBGE_BASE_API}/municipios/1302603",
        "expected_key": "nome",
        "expected_val": "Manaus",
    },
    "distritos": {
        "url": f"{IBGE_BASE_API}/distritos/130260305",
        "expected_key": "nome",
        "expected_val": "Manaus",
    },
    "subdistritos": {
        "url": f"{IBGE_BASE_API}/subdistritos/43004060506",
        "expected_key": "nome",
        "expected_val": "Zona Central",
    },
}

CAPITAIS_IBGE_7: frozenset[str] = frozenset(
    {
        "1100205",  # Porto Velho
        "1200401",  # Rio Branco
        "1302603",  # Manaus
        "1400100",  # Boa Vista
        "1501402",  # Belém
        "1600303",  # Macapá
        "1721000",  # Palmas
        "2111300",  # São Luís
        "2211001",  # Teresina
        "2304400",  # Fortaleza
        "2408102",  # Natal
        "2507507",  # João Pessoa
        "2611606",  # Recife
        "2704302",  # Maceió
        "2800308",  # Aracaju
        "2927408",  # Salvador
        "3106200",  # Belo Horizonte
        "3205309",  # Vitória
        "3304557",  # Rio de Janeiro
        "3550308",  # São Paulo
        "4106902",  # Curitiba
        "4205407",  # Florianópolis
        "4314902",  # Porto Alegre
        "5002704",  # Campo Grande
        "5103403",  # Cuiabá
        "5208707",  # Goiânia
        "5300108",  # Brasília
    }
)

REGIOES_CODIGO: dict[str, int] = {
    "Norte": 1,
    "Nordeste": 2,
    "Sudeste": 3,
    "Sul": 4,
    "Centro-Oeste": 5,
}

REGIAO_SIGLA_MAP: dict[str, str] = {
    "Norte": "N",
    "Nordeste": "NE",
    "Sudeste": "SE",
    "Sul": "S",
    "Centro-Oeste": "CO",
}


class IBGELocalidadesConnector(BaseConnector):
    """Production connector for IBGE Municipalities, Districts, Subdistricts, UFs, and Regions."""

    def __init__(
        self,
        source_id: str = "ibge_localidades",
        settings: GatewaySettings | None = None,
        storage: HybridStorageEngine | None = None,
        http_client: ResilientHttpClient | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        self.storage = storage or HybridStorageEngine(settings=self.settings)
        self.http_client = http_client or ResilientHttpClient(
            storage_engine=self.storage, settings=self.settings
        )
        self.normalizer = CanonicalNormalizer(settings=self.settings)
        self.contract_path = Path(__file__).resolve().parent / "contract.yaml"
        self._contract = None

    @property
    def contract(self):
        """Lazy load ODCS contract."""
        if self._contract is None:
            self._contract = load_contract(self.contract_path)
        return self._contract

    def test_connection(self) -> bool:
        """Run connectivity check against official IBGE API."""
        try:
            with self.http_client as client:
                resp = client.get(
                    f"{IBGE_BASE_API}/estados/13"
                )
                return resp.status_code == 200 and resp.json().get("sigla") == "AM"
        except Exception as exc:
            logger.warning("IBGE connectivity test failed: %s", exc)
            return False

    def test_live_endpoints(self, fail_hard: bool = True) -> dict[str, bool]:
        """Execute mandatory 1-line real HTTP query against all 5 IBGE endpoints.

        If fail_hard is True, any HTTP failure, 429 rate-limit, 5xx error, or payload mismatch
        IMMEDIATELY raises RuntimeError to fail the test and allow human inspection.
        """
        results: dict[str, bool] = {}
        with self.http_client as client:
            for table_name, cfg in IBGE_LIVE_1LINE_ENDPOINTS.items():
                url = cfg["url"]
                expected_key = cfg["expected_key"]
                expected_val = cfg["expected_val"]
                try:
                    resp = client.get(url, cache_ttl=0)
                    if resp.status_code != 200:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{table_name}' returned status {resp.status_code} "
                            f"(URL: {url}). Payload: {resp.text[:200]}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[table_name] = False
                        continue

                    data = resp.json()
                    # Handle both direct dict and 1-element list
                    row = data[0] if isinstance(data, list) and data else data
                    actual_val = row.get(expected_key)
                    if actual_val != expected_val:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{table_name}' schema mismatch! "
                            f"Expected {expected_key}='{expected_val}', got '{actual_val}'. "
                            f"Full payload: {row}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[table_name] = False
                        continue

                    results[table_name] = True
                    logger.info("Live 1-line test passed for '%s': %s=%s", table_name, expected_key, actual_val)
                except Exception as exc:
                    if fail_hard and not isinstance(exc, RuntimeError):
                        raise RuntimeError(f"Fail-Hard: Live endpoint '{table_name}' query failed: {exc}") from exc
                    if isinstance(exc, RuntimeError) and fail_hard:
                        raise
                    logger.error("Live test failed for '%s': %s", table_name, exc)
                    results[table_name] = False

        return results

    def _load_offline_reference(self, table_name: str) -> list[dict[str, Any]]:
        """Load deterministic reference JSON seed for a given table."""
        ref_filename = f"{table_name}_brasil.json"
        ref_file = self.settings.repo_root / "data" / "reference" / ref_filename
        if not ref_file.exists():
            ref_file = self.settings.data_dir / "reference" / ref_filename
        if not ref_file.exists():
            ref_file = (
                self.settings.repo_root.parent
                / "public-data-gateway"
                / "data"
                / "reference"
                / ref_filename
            )

        if not ref_file.exists():
            raise FileNotFoundError(f"Reference seed '{ref_filename}' not found for table '{table_name}'. Checked {ref_file}")

        with open(ref_file, encoding="utf-8") as f:
            ref_data = json.load(f)

        self._persist_bronze_snapshot(ref_data, table_name=table_name)
        return ref_data

    def fetch_raw_bronze(
        self,
        table_name: str = "municipios",
        offline_fallback: bool = True,
        force_offline: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch raw JSON payload for any of the 5 tables and store in data/ibge_localidades/bronze/."""
        if force_offline or os.environ.get("BDG_OFFLINE_MODE") == "1":
            return self._load_offline_reference(table_name)

        endpoint_url = IBGE_ENDPOINTS.get(table_name)
        if not endpoint_url:
            raise ValueError(f"Unknown table '{table_name}'. Must be one of {list(IBGE_ENDPOINTS.keys())}")

        try:
            with self.http_client as client:
                resp = client.get(endpoint_url, cache_ttl=self.settings.cache_ttl_seconds)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) > 0:
                        self._persist_bronze_snapshot(data, table_name=table_name)
                        return data
        except Exception as exc:
            logger.warning("Remote IBGE fetch failed for '%s' (%s). Triggering offline fallback.", table_name, exc)

        if not offline_fallback:
            raise RuntimeError(f"Failed to fetch IBGE table '{table_name}' and fallback is disabled.")

        return self._load_offline_reference(table_name)

    def _persist_bronze_snapshot(self, data: list[dict[str, Any]], table_name: str = "municipios") -> Path:
        """Save raw JSON payload in data/ibge_localidades/bronze/raw_<table_name>.json and legacy bronze."""
        bronze_dir = self.settings.get_source_layer_dir(self.source_id, "bronze")
        raw_file = bronze_dir / f"raw_{table_name}.json"
        with open(raw_file, "w", encoding="utf-8", newline="\n") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Maintain backward compatibility for primary table in data/bronze/ibge_localidades/
        if table_name == "municipios":
            legacy_dir = self.settings.data_dir / "bronze" / self.source_id
            legacy_dir.mkdir(parents=True, exist_ok=True)
            legacy_file = legacy_dir / "raw_municipios.json"
            try:
                import shutil
                shutil.copy2(raw_file, legacy_file)
            except Exception as exc:
                logger.debug("Failed copying legacy bronze file: %s", exc)

        return raw_file

    # -------------------------------------------------------------------------
    # Transformations to Canonical Silver
    # -------------------------------------------------------------------------

    def transform_municipios(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize raw items into strict canonical Silver structure for municípios."""
        extracted_at = datetime.now(UTC).isoformat()
        reference_date = "2024-01-01"
        silver_rows: list[dict[str, Any]] = []

        for item in raw_items:
            raw_id = str(item.get("id") or item.get("id_municipio") or "").strip()
            if not raw_id:
                continue

            id_7 = raw_id.zfill(7)
            id_6 = item.get("id_municipio_6") or id_7[:6]
            nome = str(item.get("nome") or item.get("nome_municipio") or "").strip()

            sigla_uf = ""
            id_uf = ""
            nome_uf = ""
            regiao = ""
            sigla_regiao = ""
            meso_nome = None
            micro_nome = None
            reg_inter_nome = None
            reg_imed_nome = None

            micro = item.get("microrregiao")
            if isinstance(micro, dict):
                micro_nome = micro.get("nome")
                meso = micro.get("mesorregiao")
                if isinstance(meso, dict):
                    meso_nome = meso.get("nome")
                    uf_dict = meso.get("UF")
                    if isinstance(uf_dict, dict):
                        sigla_uf = uf_dict.get("sigla", "")
                        id_uf = str(uf_dict.get("id", "")).zfill(2)
                        nome_uf = uf_dict.get("nome", "")
                        reg_dict = uf_dict.get("regiao")
                        if isinstance(reg_dict, dict):
                            regiao = reg_dict.get("nome", "")
                            sigla_regiao = reg_dict.get("sigla", "")

            imed = item.get("regiao-imediata")
            if isinstance(imed, dict):
                reg_imed_nome = imed.get("nome")
                inter = imed.get("regiao-intermediaria")
                if isinstance(inter, dict):
                    reg_inter_nome = inter.get("nome")
                    if not sigla_uf:
                        uf_dict = inter.get("UF")
                        if isinstance(uf_dict, dict):
                            sigla_uf = uf_dict.get("sigla", "")
                            id_uf = str(uf_dict.get("id", "")).zfill(2)
                            nome_uf = uf_dict.get("nome", "")
                            reg_dict = uf_dict.get("regiao")
                            if isinstance(reg_dict, dict):
                                regiao = reg_dict.get("nome", "")
                                sigla_regiao = reg_dict.get("sigla", "")

            if not sigla_uf:
                sigla_uf = str(item.get("sigla_uf") or "").strip().upper()
            if not id_uf:
                id_uf = str(item.get("id_uf") or "").strip().zfill(2)
            if not nome_uf:
                nome_uf = str(item.get("nome_uf") or "").strip()
            if not regiao:
                regiao = str(item.get("regiao") or "").strip()
            if not meso_nome:
                meso_nome = item.get("mesorregiao")
            if not micro_nome:
                micro_nome = item.get("microrregiao")
            if not reg_imed_nome:
                reg_imed_nome = item.get("regiao_imediata")
            if not reg_inter_nome:
                reg_inter_nome = item.get("regiao_intermediaria")

            if not sigla_uf or not id_uf:
                uf_code = id_7[:2]
                id_uf = uf_code
                uf_meta = CanonicalNormalizer.UF_MAP.get(uf_code)
                if uf_meta:
                    sigla_uf = uf_meta["sigla"]
                    nome_uf = uf_meta["nome"]
                    regiao = uf_meta["regiao"]

            if not sigla_regiao and regiao:
                sigla_regiao = REGIAO_SIGLA_MAP.get(regiao, "")

            is_capital = id_7 in CAPITAIS_IBGE_7

            silver_rows.append(
                {
                    "id_municipio": id_7,
                    "id_municipio_6": id_6,
                    "nome_municipio": nome,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "nome_uf": nome_uf,
                    "regiao": regiao,
                    "sigla_regiao": sigla_regiao,
                    "mesorregiao": meso_nome,
                    "microrregiao": micro_nome,
                    "regiao_intermediaria": reg_inter_nome,
                    "regiao_imediata": reg_imed_nome,
                    "is_capital": is_capital,
                    "reference_date": reference_date,
                    "extracted_at": extracted_at,
                }
            )

        return apply_sentinel_cleaning(silver_rows, self.contract)

    transform_silver_rows = transform_municipios

    def transform_distritos(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize raw items into strict canonical Silver structure for distritos."""
        extracted_at = datetime.now(UTC).isoformat()
        reference_date = "2024-01-01"
        rows: list[dict[str, Any]] = []

        for item in raw_items:
            # Supports both nivelado keys ("distrito-id") and standard nested keys
            raw_id = str(item.get("distrito-id") or item.get("id") or "").strip()
            if not raw_id:
                continue

            id_distrito = raw_id.zfill(9)
            nome_distrito = str(item.get("distrito-nome") or item.get("nome") or "").strip()

            id_mun = str(item.get("municipio-id") or "").strip()
            nome_mun = str(item.get("municipio-nome") or "").strip()
            sigla_uf = str(item.get("UF-sigla") or "").strip().upper()
            id_uf = str(item.get("UF-id") or "").strip().zfill(2)
            nome_uf = str(item.get("UF-nome") or "").strip()
            regiao = str(item.get("regiao-nome") or "").strip()
            sigla_regiao = str(item.get("regiao-sigla") or "").strip().upper()

            # Handle nested structure if not nivelado
            if not id_mun:
                mun_obj = item.get("municipio")
                if isinstance(mun_obj, dict):
                    id_mun = str(mun_obj.get("id") or "").strip()
                    nome_mun = str(mun_obj.get("nome") or "").strip()
                    micro = mun_obj.get("microrregiao")
                    if isinstance(micro, dict):
                        meso = micro.get("mesorregiao")
                        if isinstance(meso, dict):
                            uf_obj = meso.get("UF")
                            if isinstance(uf_obj, dict):
                                sigla_uf = str(uf_obj.get("sigla") or "").upper()
                                id_uf = str(uf_obj.get("id") or "").zfill(2)
                                nome_uf = str(uf_obj.get("nome") or "")
                                reg_obj = uf_obj.get("regiao")
                                if isinstance(reg_obj, dict):
                                    regiao = str(reg_obj.get("nome") or "")
                                    sigla_regiao = str(reg_obj.get("sigla") or "").upper()

            if not id_mun and len(id_distrito) >= 7:
                id_mun = id_distrito[:7]

            id_mun_7 = id_mun.zfill(7)
            if not sigla_uf and id_mun_7:
                uf_code = id_mun_7[:2]
                id_uf = uf_code
                uf_meta = CanonicalNormalizer.UF_MAP.get(uf_code)
                if uf_meta:
                    sigla_uf = uf_meta["sigla"]
                    nome_uf = uf_meta["nome"]
                    regiao = uf_meta["regiao"]
                    sigla_regiao = REGIAO_SIGLA_MAP.get(regiao, "")

            rows.append(
                {
                    "id_distrito": id_distrito,
                    "nome_distrito": nome_distrito,
                    "id_municipio": id_mun_7,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "nome_uf": nome_uf,
                    "regiao": regiao,
                    "sigla_regiao": sigla_regiao,
                    "reference_date": reference_date,
                    "extracted_at": extracted_at,
                }
            )

        return rows

    def transform_subdistritos(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize raw items into strict canonical Silver structure for subdistritos."""
        extracted_at = datetime.now(UTC).isoformat()
        reference_date = "2024-01-01"
        rows: list[dict[str, Any]] = []

        for item in raw_items:
            raw_id = str(item.get("subdistrito-id") or item.get("id") or "").strip()
            if not raw_id:
                continue

            id_subdistrito = raw_id
            nome_subdistrito = str(item.get("subdistrito-nome") or item.get("nome") or "").strip()

            id_distrito = str(item.get("distrito-id") or "").strip()
            nome_distrito = str(item.get("distrito-nome") or "").strip()
            id_mun = str(item.get("municipio-id") or "").strip()
            nome_mun = str(item.get("municipio-nome") or "").strip()
            sigla_uf = str(item.get("UF-sigla") or "").strip().upper()
            id_uf = str(item.get("UF-id") or "").strip().zfill(2)
            nome_uf = str(item.get("UF-nome") or "").strip()

            # Handle nested structure if not nivelado
            if not id_distrito:
                dist_obj = item.get("distrito")
                if isinstance(dist_obj, dict):
                    id_distrito = str(dist_obj.get("id") or "").strip()
                    nome_distrito = str(dist_obj.get("nome") or "").strip()
                    mun_obj = dist_obj.get("municipio")
                    if isinstance(mun_obj, dict):
                        id_mun = str(mun_obj.get("id") or "").strip()
                        nome_mun = str(mun_obj.get("nome") or "").strip()
                        micro = mun_obj.get("microrregiao")
                        if isinstance(micro, dict):
                            meso = micro.get("mesorregiao")
                            if isinstance(meso, dict):
                                uf_obj = meso.get("UF")
                                if isinstance(uf_obj, dict):
                                    sigla_uf = str(uf_obj.get("sigla") or "").upper()
                                    id_uf = str(uf_obj.get("id") or "").zfill(2)
                                    nome_uf = str(uf_obj.get("nome") or "")

            if not id_distrito and len(id_subdistrito) >= 9:
                id_distrito = id_subdistrito[:9]
            if not id_mun and len(id_subdistrito) >= 7:
                id_mun = id_subdistrito[:7]

            id_mun_7 = id_mun.zfill(7)
            if not sigla_uf and id_mun_7:
                uf_code = id_mun_7[:2]
                id_uf = uf_code
                uf_meta = CanonicalNormalizer.UF_MAP.get(uf_code)
                if uf_meta:
                    sigla_uf = uf_meta["sigla"]
                    nome_uf = uf_meta["nome"]

            rows.append(
                {
                    "id_subdistrito": id_subdistrito,
                    "nome_subdistrito": nome_subdistrito,
                    "id_distrito": id_distrito.zfill(9) if id_distrito else "",
                    "nome_distrito": nome_distrito,
                    "id_municipio": id_mun_7,
                    "nome_municipio": nome_mun,
                    "sigla_uf": sigla_uf,
                    "id_uf": id_uf,
                    "nome_uf": nome_uf,
                    "reference_date": reference_date,
                    "extracted_at": extracted_at,
                }
            )

        return rows

    def transform_estados(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize raw items into strict canonical Silver structure for estados/UFs."""
        extracted_at = datetime.now(UTC).isoformat()
        reference_date = "2024-01-01"
        rows: list[dict[str, Any]] = []

        for item in raw_items:
            raw_id = str(item.get("UF-id") or item.get("id") or "").strip()
            if not raw_id:
                continue

            id_uf = raw_id.zfill(2)
            sigla_uf = str(item.get("UF-sigla") or item.get("sigla") or "").strip().upper()
            nome_uf = str(item.get("UF-nome") or item.get("nome") or "").strip()
            regiao = str(item.get("regiao-nome") or "").strip()
            sigla_regiao = str(item.get("regiao-sigla") or "").strip().upper()
            id_regiao = str(item.get("regiao-id") or "").strip()

            if not regiao:
                reg_obj = item.get("regiao")
                if isinstance(reg_obj, dict):
                    regiao = str(reg_obj.get("nome") or "").strip()
                    sigla_regiao = str(reg_obj.get("sigla") or "").strip().upper()
                    id_regiao = str(reg_obj.get("id") or "").strip()

            if not regiao and id_uf in CanonicalNormalizer.UF_MAP:
                meta = CanonicalNormalizer.UF_MAP[id_uf]
                regiao = meta["regiao"]
                sigla_regiao = REGIAO_SIGLA_MAP.get(regiao, "")

            if not id_regiao and regiao in REGIOES_CODIGO:
                id_regiao = str(REGIOES_CODIGO[regiao])

            rows.append(
                {
                    "id_uf": id_uf,
                    "sigla_uf": sigla_uf,
                    "nome_uf": nome_uf,
                    "regiao": regiao,
                    "sigla_regiao": sigla_regiao,
                    "id_regiao": id_regiao,
                    "reference_date": reference_date,
                    "extracted_at": extracted_at,
                }
            )

        return rows

    def transform_regioes(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize raw items into strict canonical Silver structure for regiões."""
        extracted_at = datetime.now(UTC).isoformat()
        reference_date = "2024-01-01"
        rows: list[dict[str, Any]] = []

        for item in raw_items:
            raw_id = str(item.get("id") or "").strip()
            if not raw_id:
                continue

            id_regiao = raw_id
            sigla_regiao = str(item.get("sigla") or "").strip().upper()
            nome_regiao = str(item.get("nome") or "").strip()

            rows.append(
                {
                    "id_regiao": id_regiao,
                    "sigla_regiao": sigla_regiao,
                    "nome_regiao": nome_regiao,
                    "reference_date": reference_date,
                    "extracted_at": extracted_at,
                }
            )

        return rows

    # -------------------------------------------------------------------------
    # End-to-End Extraction & Pipeline
    # -------------------------------------------------------------------------

    def extract_table(self, table_name: str) -> pa.Table:
        """Extract and normalize a single table, writing to data/ibge_localidades/silver/<table_name>.parquet."""
        raw_items = self.fetch_raw_bronze(table_name=table_name)

        if table_name == "municipios":
            rows = self.transform_municipios(raw_items)
            fields = [
                pa.field("id_municipio", pa.string(), nullable=False),
                pa.field("id_municipio_6", pa.string(), nullable=False),
                pa.field("nome_municipio", pa.string(), nullable=False),
                pa.field("sigla_uf", pa.string(), nullable=False),
                pa.field("id_uf", pa.string(), nullable=False),
                pa.field("nome_uf", pa.string(), nullable=False),
                pa.field("regiao", pa.string(), nullable=False),
                pa.field("sigla_regiao", pa.string(), nullable=False),
                pa.field("mesorregiao", pa.string(), nullable=True),
                pa.field("microrregiao", pa.string(), nullable=True),
                pa.field("regiao_intermediaria", pa.string(), nullable=True),
                pa.field("regiao_imediata", pa.string(), nullable=True),
                pa.field("is_capital", pa.bool_(), nullable=False),
                pa.field("reference_date", pa.string(), nullable=False),
                pa.field("extracted_at", pa.string(), nullable=False),
            ]
            contract_to_use = self.contract
        elif table_name == "distritos":
            rows = self.transform_distritos(raw_items)
            fields = [
                pa.field("id_distrito", pa.string(), nullable=False),
                pa.field("nome_distrito", pa.string(), nullable=False),
                pa.field("id_municipio", pa.string(), nullable=False),
                pa.field("nome_municipio", pa.string(), nullable=False),
                pa.field("sigla_uf", pa.string(), nullable=False),
                pa.field("id_uf", pa.string(), nullable=False),
                pa.field("nome_uf", pa.string(), nullable=False),
                pa.field("regiao", pa.string(), nullable=False),
                pa.field("sigla_regiao", pa.string(), nullable=False),
                pa.field("reference_date", pa.string(), nullable=False),
                pa.field("extracted_at", pa.string(), nullable=False),
            ]
            contract_to_use = None
        elif table_name == "subdistritos":
            rows = self.transform_subdistritos(raw_items)
            fields = [
                pa.field("id_subdistrito", pa.string(), nullable=False),
                pa.field("nome_subdistrito", pa.string(), nullable=False),
                pa.field("id_distrito", pa.string(), nullable=False),
                pa.field("nome_distrito", pa.string(), nullable=False),
                pa.field("id_municipio", pa.string(), nullable=False),
                pa.field("nome_municipio", pa.string(), nullable=False),
                pa.field("sigla_uf", pa.string(), nullable=False),
                pa.field("id_uf", pa.string(), nullable=False),
                pa.field("nome_uf", pa.string(), nullable=False),
                pa.field("reference_date", pa.string(), nullable=False),
                pa.field("extracted_at", pa.string(), nullable=False),
            ]
            contract_to_use = None
        elif table_name == "estados":
            rows = self.transform_estados(raw_items)
            fields = [
                pa.field("id_uf", pa.string(), nullable=False),
                pa.field("sigla_uf", pa.string(), nullable=False),
                pa.field("nome_uf", pa.string(), nullable=False),
                pa.field("regiao", pa.string(), nullable=False),
                pa.field("sigla_regiao", pa.string(), nullable=False),
                pa.field("id_regiao", pa.string(), nullable=False),
                pa.field("reference_date", pa.string(), nullable=False),
                pa.field("extracted_at", pa.string(), nullable=False),
            ]
            contract_to_use = None
        elif table_name == "regioes":
            rows = self.transform_regioes(raw_items)
            fields = [
                pa.field("id_regiao", pa.string(), nullable=False),
                pa.field("sigla_regiao", pa.string(), nullable=False),
                pa.field("nome_regiao", pa.string(), nullable=False),
                pa.field("reference_date", pa.string(), nullable=False),
                pa.field("extracted_at", pa.string(), nullable=False),
            ]
            contract_to_use = None
        else:
            raise ValueError(f"Unsupported table: {table_name}")

        schema = pa.schema(fields)
        pydict: dict[str, list[Any]] = {f.name: [] for f in fields}
        for r in rows:
            for f in fields:
                pydict[f.name].append(r.get(f.name))

        table = pa.Table.from_pydict(pydict, schema=schema)

        # Write to isolated data/ibge_localidades/silver/<table_name>.parquet
        _, _, obs_report = self.storage.write_medallion_table(
            table,
            source_id=self.source_id,
            layer="silver",
            table_name=table_name,
            contract=contract_to_use,
            use_source_subfolder=True,
        )

        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"IBGE Localidades Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )

        return table

    def extract_all(self) -> dict[str, pa.Table]:
        """Full execution across all 5 tables from Bronze to Silver and Gold."""
        tables: dict[str, pa.Table] = {}
        for tbl_name in ["municipios", "distritos", "subdistritos", "estados", "regioes"]:
            tables[tbl_name] = self.extract_table(tbl_name)

        # Build and materialize Gold ML Feature Views based on municipalities
        self._build_and_write_gold_features(tables["municipios"])
        return tables

    def extract(self) -> pa.Table:
        """Backwards-compatible primary entrypoint extracting all tables and returning municípios."""
        all_tables = self.extract_all()
        return all_tables["municipios"]

    def _build_and_write_gold_features(self, silver_table: pa.Table) -> pa.Table:
        """Construct ML feature view for territorial entities in data/ibge_localidades/gold/."""
        uf_counts: dict[str, int] = {}
        imediata_counts: dict[str, int] = {}

        siglas = silver_table.column("sigla_uf").to_pylist()
        imediatas = silver_table.column("regiao_imediata").to_pylist()

        for s in siglas:
            uf_counts[s] = uf_counts.get(s, 0) + 1
        for im in imediatas:
            if im:
                imediata_counts[im] = imediata_counts.get(im, 0) + 1

        gold_rows: list[dict[str, Any]] = []
        rows = silver_table.to_pylist()

        for r in rows:
            regiao_nome = r["regiao"]
            cod_regiao = REGIOES_CODIGO.get(regiao_nome, 0)
            uf_sigla = r["sigla_uf"]
            imediata = r.get("regiao_imediata")

            gold_rows.append(
                {
                    "id_municipio": r["id_municipio"],
                    "sigla_uf": uf_sigla,
                    "regiao": regiao_nome,
                    "codigo_macro_regiao": cod_regiao,
                    "is_capital": 1 if r["is_capital"] else 0,
                    "qtd_municipios_mesma_uf": uf_counts.get(uf_sigla, 0),
                    "qtd_municipios_mesma_imediata": (
                        imediata_counts.get(imediata, 0) if imediata else 0
                    ),
                    "reference_date": r["reference_date"],
                }
            )

        gold_fields = [
            pa.field("id_municipio", pa.string(), nullable=False),
            pa.field("sigla_uf", pa.string(), nullable=False),
            pa.field("regiao", pa.string(), nullable=False),
            pa.field("codigo_macro_regiao", pa.int64(), nullable=False),
            pa.field("is_capital", pa.int64(), nullable=False),
            pa.field("qtd_municipios_mesma_uf", pa.int64(), nullable=False),
            pa.field("qtd_municipios_mesma_imediata", pa.int64(), nullable=False),
            pa.field("reference_date", pa.string(), nullable=False),
        ]
        gold_schema = pa.schema(gold_fields)

        gold_pydict: dict[str, list[Any]] = {f.name: [] for f in gold_fields}
        for gr in gold_rows:
            for f in gold_fields:
                gold_pydict[f.name].append(gr.get(f.name))

        gold_table = pa.Table.from_pydict(gold_pydict, schema=gold_schema)

        gold_path, _, _ = self.storage.write_medallion_table(
            gold_table,
            source_id=self.source_id,
            layer="gold",
            table_name="features_municipio_ml",
            contract=None,
            use_source_subfolder=True,
        )

        # Maintain legacy flat gold alias
        legacy_gold = self.settings.gold_dir / f"{self.source_id}.parquet"
        try:
            import shutil
            shutil.copy2(gold_path, legacy_gold)
        except Exception as exc:
            logger.debug("Failed copying legacy flat gold file: %s", exc)

        return gold_table

    # -------------------------------------------------------------------------
    # Public Query Methods for SDK, API, CLI, and MCP
    # -------------------------------------------------------------------------

    def get_municipio(self, id_or_name: str, uf: str | None = None) -> dict[str, Any] | None:
        """Resolve a single municipality by IBGE code (7D/6D) or name with optional UF."""
        id_7 = self.normalizer.to_ibge_7(id_or_name, uf=uf)
        if not id_7:
            return None

        # Try reading from isolated silver Parquet first
        try:
            silver_path = self.settings.data_dir / self.source_id / "silver" / "municipios.parquet"
            if not silver_path.exists():
                silver_path = self.settings.silver_dir / f"{self.source_id}.parquet"

            if silver_path.exists():
                sql = (
                    f"SELECT * FROM read_parquet('{silver_path}') WHERE id_municipio = '{id_7}'"
                )
                res = self.storage.query_sql(sql)
                if res.num_rows > 0:
                    return res.to_pylist()[0]
        except Exception as exc:
            logger.debug("Silver parquet query failed: %s", exc)

        # Fallback to in-memory normalizer
        m = self.normalizer.get_municipio(id_7)
        if m:
            return {
                "id_municipio": m["id_municipio"],
                "id_municipio_6": m["id_municipio_6"],
                "nome_municipio": m["nome"],
                "sigla_uf": m["sigla_uf"],
                "id_uf": m["id_uf"],
                "nome_uf": m["nome_uf"],
                "regiao": m["regiao"],
                "sigla_regiao": REGIAO_SIGLA_MAP.get(m["regiao"], ""),
                "is_capital": m["id_municipio"] in CAPITAIS_IBGE_7,
                "reference_date": "2024-01-01",
            }
        return None

    def list_municipios(
        self,
        uf: str | None = None,
        regiao: str | None = None,
        capitais_only: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List municipalities filtered by UF, region, or capitals."""
        silver_path = self.settings.data_dir / self.source_id / "silver" / "municipios.parquet"
        if not silver_path.exists():
            silver_path = self.settings.silver_dir / f"{self.source_id}.parquet"

        if silver_path.exists():
            clauses = []
            if uf:
                clauses.append(f"sigla_uf = '{uf.upper()}'")
            if regiao:
                clauses.append(f"LOWER(regiao) = LOWER('{regiao}')")
            if capitais_only:
                clauses.append("is_capital = true")

            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            offset_clause = f"OFFSET {offset}" if offset else ""
            sql = f"SELECT * FROM read_parquet('{silver_path}') {where} ORDER BY id_municipio {limit_clause} {offset_clause}"
            return self.storage.query_sql(sql).to_pylist()

        # Fallback to normalizer
        results = []
        for m in self.normalizer.municipios:
            if uf and m["sigla_uf"] != uf.upper():
                continue
            if regiao and m["regiao"].lower() != regiao.lower():
                continue
            if capitais_only and m["id_municipio"] not in CAPITAIS_IBGE_7:
                continue
            results.append(
                {
                    "id_municipio": m["id_municipio"],
                    "id_municipio_6": m["id_municipio_6"],
                    "nome_municipio": m["nome"],
                    "sigla_uf": m["sigla_uf"],
                    "id_uf": m["id_uf"],
                    "nome_uf": m["nome_uf"],
                    "regiao": m["regiao"],
                    "is_capital": m["id_municipio"] in CAPITAIS_IBGE_7,
                }
            )
        if offset:
            results = results[offset:]
        if limit:
            results = results[:limit]
        return results

    def list_distritos(
        self,
        id_municipio: str | None = None,
        uf: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List distritos filtered by municipality ID or UF."""
        silver_path = self.settings.data_dir / self.source_id / "silver" / "distritos.parquet"
        if silver_path.exists():
            clauses = []
            if id_municipio:
                clauses.append(f"id_municipio = '{id_municipio.zfill(7)}'")
            if uf:
                clauses.append(f"sigla_uf = '{uf.upper()}'")
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            sql = f"SELECT * FROM read_parquet('{silver_path}') {where} ORDER BY id_distrito {limit_clause}"
            return self.storage.query_sql(sql).to_pylist()
        return []

    def list_subdistritos(
        self,
        id_municipio: str | None = None,
        id_distrito: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List subdistritos filtered by municipality ID or distrito ID."""
        silver_path = self.settings.data_dir / self.source_id / "silver" / "subdistritos.parquet"
        if silver_path.exists():
            clauses = []
            if id_municipio:
                clauses.append(f"id_municipio = '{id_municipio.zfill(7)}'")
            if id_distrito:
                clauses.append(f"id_distrito = '{id_distrito.zfill(9)}'")
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            sql = f"SELECT * FROM read_parquet('{silver_path}') {where} ORDER BY id_subdistrito {limit_clause}"
            return self.storage.query_sql(sql).to_pylist()
        return []

    def list_ufs(self) -> list[dict[str, Any]]:
        """List the 27 Brazilian Federative Units."""
        silver_path = self.settings.data_dir / self.source_id / "silver" / "estados.parquet"
        if silver_path.exists():
            sql = f"SELECT * FROM read_parquet('{silver_path}') ORDER BY sigla_uf"
            return self.storage.query_sql(sql).to_pylist()

        ufs = []
        for code, meta in sorted(CanonicalNormalizer.UF_MAP.items(), key=lambda x: x[1]["sigla"]):
            ufs.append(
                {
                    "id_uf": code,
                    "sigla_uf": meta["sigla"],
                    "nome_uf": meta["nome"],
                    "regiao": meta["regiao"],
                    "sigla_regiao": REGIAO_SIGLA_MAP.get(meta["regiao"], ""),
                }
            )
        return ufs

    def list_regioes(self) -> list[dict[str, Any]]:
        """List the 5 Brazilian macro-regions."""
        silver_path = self.settings.data_dir / self.source_id / "silver" / "regioes.parquet"
        if silver_path.exists():
            sql = f"SELECT * FROM read_parquet('{silver_path}') ORDER BY id_regiao"
            return self.storage.query_sql(sql).to_pylist()

        return [
            {"id_regiao": str(cod), "sigla_regiao": REGIAO_SIGLA_MAP[nome], "nome_regiao": nome}
            for nome, cod in sorted(REGIOES_CODIGO.items(), key=lambda x: x[1])
        ]


# Register connector globally
register_connector("ibge_localidades", IBGELocalidadesConnector)
