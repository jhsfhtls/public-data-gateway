"""BCB SGS Connector — Banco Central do Brasil Sistema Gerenciador de Séries Temporais.

Part of the public-data-gateway Medallion platform (C.O.R.E. Ecosystem).
Implements the 11-Gate Protocol:
1. Multi-table ingestion (Catalogo, IPCA, IGP-M, Selic, CDI, IBC-Br)
2. Isolated storage in data/bcb_sgs/{bronze, silver, gold, quarantine}/
3. dbt encapsulation & 100% column descriptions
4. Intra-domain macroeconomic data mart (mart_cenario_macroeconomico_brasil)
5. Live 1-line fail-hard tests against all official BACEN endpoints.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector
from public_data_gateway.core.contracts import load_contract
from public_data_gateway.core.http_client import ResilientHttpClient
from public_data_gateway.core.normalizer import CanonicalNormalizer
from public_data_gateway.core.storage import HybridStorageEngine

logger = logging.getLogger(__name__)

BCB_SGS_BASE_API = "https://api.bcb.gov.br/dados/serie"

BCB_SGS_LIVE_1LINE_ENDPOINTS: dict[str, dict[str, Any]] = {
    "ipca_mensal": {
        "serie_id": "433",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.433/dados/ultimos/1?formato=json",
        "name": "IPCA Variação Mensal",
    },
    "ipca_12m": {
        "serie_id": "13522",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.13522/dados/ultimos/1?formato=json",
        "name": "IPCA Acumulado 12 Meses",
    },
    "igpm_mensal": {
        "serie_id": "189",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.189/dados/ultimos/1?formato=json",
        "name": "IGP-M Variação Mensal",
    },
    "selic_diaria": {
        "serie_id": "11",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.11/dados/ultimos/1?formato=json",
        "name": "Taxa Selic Diária Anualizada",
    },
    "selic_meta": {
        "serie_id": "432",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.432/dados/ultimos/1?formato=json",
        "name": "Taxa Selic Meta COPOM",
    },
    "cdi_diario": {
        "serie_id": "12",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.12/dados/ultimos/1?formato=json",
        "name": "Taxa CDI Diária Anualizada",
    },
    "ibcbr": {
        "serie_id": "24363",
        "url": f"{BCB_SGS_BASE_API}/bcdata.sgs.24363/dados/ultimos/1?formato=json",
        "name": "Índice de Atividade Econômica BACEN (IBC-Br)",
    },
}

CATALOGO_SERIES_DEFINICOES: list[dict[str, str]] = [
    {
        "id_serie": "433",
        "nome_serie": "IPCA - Variação mensal (%)",
        "periodicidade": "MENSAL",
        "unidade_medida": "% a.m.",
        "orgao_gestor": "BACEN / IBGE",
        "categoria": "INFLACAO",
        "data_inicio_historico": "1980-01-01",
    },
    {
        "id_serie": "13522",
        "nome_serie": "IPCA - Acumulado 12 meses (%)",
        "periodicidade": "MENSAL",
        "unidade_medida": "% a.a.",
        "orgao_gestor": "BACEN / IBGE",
        "categoria": "INFLACAO",
        "data_inicio_historico": "1980-12-01",
    },
    {
        "id_serie": "189",
        "nome_serie": "IGP-M - Variação mensal (%)",
        "periodicidade": "MENSAL",
        "unidade_medida": "% a.m.",
        "orgao_gestor": "BACEN / FGV",
        "categoria": "INFLACAO",
        "data_inicio_historico": "1989-06-01",
    },
    {
        "id_serie": "11",
        "nome_serie": "Taxa Selic diária anualizada base 252 (%)",
        "periodicidade": "DIARIA",
        "unidade_medida": "% a.a.",
        "orgao_gestor": "BACEN / COPOM",
        "categoria": "JUROS",
        "data_inicio_historico": "1986-06-04",
    },
    {
        "id_serie": "432",
        "nome_serie": "Taxa Selic - Meta definida pelo Copom (% a.a.)",
        "periodicidade": "REUNIAO_COPOM",
        "unidade_medida": "% a.a.",
        "orgao_gestor": "BACEN / COPOM",
        "categoria": "JUROS",
        "data_inicio_historico": "1999-03-05",
    },
    {
        "id_serie": "12",
        "nome_serie": "Taxa CDI diária anualizada base 252 (%)",
        "periodicidade": "DIARIA",
        "unidade_medida": "% a.a.",
        "orgao_gestor": "BACEN / B3",
        "categoria": "JUROS",
        "data_inicio_historico": "1986-03-06",
    },
    {
        "id_serie": "24363",
        "nome_serie": "Índice de Atividade Econômica do Banco Central (IBC-Br dessazonalizado)",
        "periodicidade": "MENSAL",
        "unidade_medida": "Número Índice",
        "orgao_gestor": "BACEN",
        "categoria": "ATIVIDADE_ECONOMICA",
        "data_inicio_historico": "2003-01-01",
    },
]


class BCBSGSConnector(BaseConnector):
    """Production connector for BACEN SGS Macroeconomic Series."""

    def __init__(
        self,
        source_id: str = "bcb_sgs",
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
        """Run connectivity check against official BACEN SGS API."""
        try:
            with self.http_client as client:
                resp = client.get(
                    f"{BCB_SGS_BASE_API}/bcdata.sgs.433/dados/ultimos/1?formato=json"
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return isinstance(data, list) and len(data) > 0 and "valor" in data[0]
                return False
        except Exception as exc:
            logger.warning("BCB SGS connectivity test failed: %s", exc)
            return False

    def test_live_endpoints(self, fail_hard: bool = True) -> dict[str, bool]:
        """Execute mandatory 1-line real HTTP query against all official BCB SGS endpoints.

        If fail_hard is True, any HTTP failure, 429 rate-limit, 5xx error, or payload mismatch
        IMMEDIATELY raises RuntimeError to fail the test and allow human inspection.
        """
        results: dict[str, bool] = {}
        with self.http_client as client:
            for test_name, cfg in BCB_SGS_LIVE_1LINE_ENDPOINTS.items():
                url = cfg["url"]
                serie_name = cfg["name"]
                try:
                    resp = client.get(url, cache_ttl=0)
                    if resp.status_code == 304 and not resp.text.strip() and "/ultimos/1" in url:
                        # Upstream CDN cache artifact: query ultimos/2 and take latest record
                        alt_url = url.replace("/ultimos/1", "/ultimos/2")
                        resp = client.get(alt_url, cache_ttl=0)

                    if resp.status_code != 200:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{test_name}' ({serie_name}) returned status {resp.status_code} "
                            f"(URL: {url}). Payload: {resp.text[:200]}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[test_name] = False
                        continue

                    data = resp.json()
                    if not isinstance(data, list) or len(data) == 0:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{test_name}' returned empty or non-list response: {data}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[test_name] = False
                        continue

                    item = data[0]
                    if "data" not in item or "valor" not in item:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{test_name}' missing expected keys ('data', 'valor'). Got: {item}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[test_name] = False
                        continue

                    # Validate value is parsable as float
                    try:
                        float(item["valor"].replace(",", "."))
                    except (ValueError, AttributeError) as conv_err:
                        err_msg = f"Fail-Hard: Live endpoint '{test_name}' valor '{item.get('valor')}' is not numeric: {conv_err}"
                        if fail_hard:
                            raise RuntimeError(err_msg) from conv_err
                        results[test_name] = False
                        continue

                    results[test_name] = True
                    logger.info("Live 1-line test passed for '%s' (%s): data=%s, valor=%s", test_name, serie_name, item["data"], item["valor"])
                except Exception as exc:
                    if fail_hard and not isinstance(exc, RuntimeError):
                        raise RuntimeError(f"Fail-Hard: Live endpoint '{test_name}' query failed: {exc}") from exc
                    if isinstance(exc, RuntimeError) and fail_hard:
                        raise
                    logger.error("Live test failed for '%s': %s", test_name, exc)
                    results[test_name] = False

        return results

    def _load_offline_reference(self, table_name: str) -> Any:
        """Load deterministic reference JSON seed for a given table."""
        ref_filename = f"{table_name}_brasil.json"
        candidates = [
            self.settings.repo_root / "data" / "reference" / ref_filename,
            self.settings.data_dir / "reference" / ref_filename,
            self.settings.repo_root.parent / "public-data-gateway" / "data" / "reference" / ref_filename,
        ]
        for c in candidates:
            if c.exists():
                with open(c, encoding="utf-8") as f:
                    return json.load(f)
        raise FileNotFoundError(f"Offline reference file '{ref_filename}' not found in candidates: {candidates}")

    def _convert_date_to_iso(self, date_str: str) -> str:
        """Convert 'DD/MM/YYYY' date string to ISO 'YYYY-MM-DD'."""
        if not date_str or not isinstance(date_str, str):
            return "1900-01-01"
        try:
            parts = date_str.strip().split("/")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except Exception:
            pass
        return date_str

    def _convert_date_to_anomes(self, date_str: str) -> str:
        """Extract 'YYYY-MM' from 'DD/MM/YYYY' or 'YYYY-MM-DD'."""
        iso = self._convert_date_to_iso(date_str)
        return iso[:7] if len(iso) >= 7 else "1900-01"

    def _parse_float_safe(self, val: Any) -> float | None:
        """Parse string value to float, respecting sentinel and null checks."""
        if val is None:
            return None
        val_str = str(val).strip()
        if val_str in {"-", "-1", "99", "999", "SEM INFORMACAO", "IGNORADO", "N/A", ""}:
            return None
        try:
            return float(val_str.replace(",", "."))
        except (ValueError, TypeError):
            return None

    def _extract_sgs_catalogo(self) -> list[dict[str, Any]]:
        """Extract catalog of monitored SGS macroeconomic series."""
        try:
            data = self._load_offline_reference("sgs_catalogo")
        except Exception:
            data = CATALOGO_SERIES_DEFINICOES

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []
        for row in data:
            records.append({
                "id_serie": str(row.get("id_serie", "")),
                "nome_serie": str(row.get("nome_serie", "")),
                "periodicidade": str(row.get("periodicidade", "")),
                "unidade_medida": str(row.get("unidade_medida", "")),
                "orgao_gestor": str(row.get("orgao_gestor", "")),
                "categoria": str(row.get("categoria", "")),
                "data_inicio_historico": str(row.get("data_inicio_historico", "")),
                "reference_date": "2026-01-01",
                "extracted_at": now_iso,
            })
        return records

    def _extract_sgs_ipca(self) -> list[dict[str, Any]]:
        """Extract IPCA observations (mensal 433 and acumulado 12m 13522)."""
        data = self._load_offline_reference("sgs_ipca")
        mensal = data.get("mensal", []) if isinstance(data, dict) else data
        acum_12m = data.get("acumulado_12m", []) if isinstance(data, dict) else []

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in mensal:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "433",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "IPCA - Variação mensal (%)",
                    "unidade_medida": "% a.m.",
                    "periodicidade": "MENSAL",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        for row in acum_12m:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "13522",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "IPCA - Acumulado 12 meses (%)",
                    "unidade_medida": "% a.a.",
                    "periodicidade": "MENSAL",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        return records

    def _extract_sgs_igpm(self) -> list[dict[str, Any]]:
        """Extract IGP-M observations (189)."""
        raw = self._load_offline_reference("sgs_igpm")
        rows = raw if isinstance(raw, list) else []
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in rows:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "189",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "IGP-M - Variação mensal (%)",
                    "unidade_medida": "% a.m.",
                    "periodicidade": "MENSAL",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        return records

    def _extract_sgs_selic(self) -> list[dict[str, Any]]:
        """Extract Selic observations (diária 11 and meta COPOM 432)."""
        data = self._load_offline_reference("sgs_selic")
        diaria = data.get("diaria", []) if isinstance(data, dict) else data
        meta = data.get("meta_copom", []) if isinstance(data, dict) else []

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in diaria:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "11",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "Taxa Selic diária anualizada base 252 (%)",
                    "unidade_medida": "% a.a.",
                    "periodicidade": "DIARIA",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        for row in meta:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "432",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "Taxa Selic - Meta definida pelo Copom (% a.a.)",
                    "unidade_medida": "% a.a.",
                    "periodicidade": "REUNIAO_COPOM",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        return records

    def _extract_sgs_cdi(self) -> list[dict[str, Any]]:
        """Extract CDI observations (12)."""
        raw = self._load_offline_reference("sgs_cdi")
        rows = raw if isinstance(raw, list) else []
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in rows:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "12",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "Taxa CDI diária anualizada base 252 (%)",
                    "unidade_medida": "% a.a.",
                    "periodicidade": "DIARIA",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        return records

    def _extract_sgs_ibcbr(self) -> list[dict[str, Any]]:
        """Extract IBC-Br activity observations (24363)."""
        raw = self._load_offline_reference("sgs_ibcbr")
        rows = raw if isinstance(raw, list) else []
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in rows:
            dt_iso = self._convert_date_to_iso(row.get("data", ""))
            v = self._parse_float_safe(row.get("valor"))
            if v is not None:
                records.append({
                    "id_serie": "24363",
                    "data_referencia": dt_iso,
                    "ano_mes": dt_iso[:7],
                    "valor": v,
                    "nome_serie": "Índice de Atividade Econômica do Banco Central (IBC-Br dessazonalizado)",
                    "unidade_medida": "Número Índice",
                    "periodicidade": "MENSAL",
                    "reference_date": dt_iso,
                    "extracted_at": now_iso,
                })

        return records

    def _persist_bronze_snapshot(self, table_name: str, raw_data: Any) -> Path:
        """Persist raw JSON into data/bcb_sgs/bronze/."""
        source_dir = self.settings.data_dir / "bcb_sgs" / "bronze"
        source_dir.mkdir(parents=True, exist_ok=True)
        path = source_dir / f"raw_{table_name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False)
        return path

    def _persist_silver_table(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> Path:
        """Persist normalized records to data/bcb_sgs/silver/{table_name}.parquet."""
        table = pa.Table.from_pylist(records)

        # Write to isolated data/bcb_sgs/silver/<table_name>.parquet via write_medallion_table
        contract_to_use = self.contract if table_name != "sgs_catalogo" else None
        path, _, obs_report = self.storage.write_medallion_table(
            table,
            source_id="bcb_sgs",
            layer="silver",
            table_name=table_name,
            contract=contract_to_use,
            use_source_subfolder=True,
        )

        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"BCB SGS Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )

        return path

    def _build_and_write_gold_features(self) -> Path:
        """Compute and persist gold features view data/bcb_sgs/gold/features_macro_brasil.parquet."""
        # Read the silver tables to build the monthly consolidated feature store
        ipca_path = self.settings.data_dir / "bcb_sgs" / "silver" / "sgs_ipca.parquet"
        igpm_path = self.settings.data_dir / "bcb_sgs" / "silver" / "sgs_igpm.parquet"
        selic_path = self.settings.data_dir / "bcb_sgs" / "silver" / "sgs_selic.parquet"
        cdi_path = self.settings.data_dir / "bcb_sgs" / "silver" / "sgs_cdi.parquet"
        ibcbr_path = self.settings.data_dir / "bcb_sgs" / "silver" / "sgs_ibcbr.parquet"

        # Month aggregates mapping
        months_data: dict[str, dict[str, Any]] = {}

        def get_or_create(m: str) -> dict[str, Any]:
            if m not in months_data:
                months_data[m] = {
                    "competencia_mes": m,
                    "ipca_mensal_pct": None,
                    "ipca_acumulado_12m_pct": None,
                    "igpm_mensal_pct": None,
                    "selic_meta_fim_mes_pct": None,
                    "selic_diaria_media_mensal_pct": None,
                    "cdi_diario_medio_mensal_pct": None,
                    "ibcbr_indice": None,
                    "juros_real_ex_post_pct": None,
                    "spread_cdi_selic_pct": None,
                    "reference_date": f"{m}-01",
                    "extracted_at": datetime.now(UTC).isoformat(),
                }
            return months_data[m]

        if ipca_path.exists():
            t = pq.read_table(ipca_path).to_pylist()
            for r in t:
                m = r.get("ano_mes")
                if not m:
                    continue
                entry = get_or_create(m)
                if r.get("id_serie") == "433":
                    entry["ipca_mensal_pct"] = r.get("valor")
                elif r.get("id_serie") == "13522":
                    entry["ipca_acumulado_12m_pct"] = r.get("valor")

        if igpm_path.exists():
            t = pq.read_table(igpm_path).to_pylist()
            for r in t:
                m = r.get("ano_mes")
                if not m:
                    continue
                entry = get_or_create(m)
                entry["igpm_mensal_pct"] = r.get("valor")

        if selic_path.exists():
            t = pq.read_table(selic_path).to_pylist()
            selic_daily_by_month: dict[str, list[float]] = {}
            for r in t:
                m = r.get("ano_mes")
                if not m:
                    continue
                entry = get_or_create(m)
                if r.get("id_serie") == "432":
                    entry["selic_meta_fim_mes_pct"] = r.get("valor")
                elif r.get("id_serie") == "11" and r.get("valor") is not None:
                    selic_daily_by_month.setdefault(m, []).append(r["valor"])

            for m, vals in selic_daily_by_month.items():
                if vals and m in months_data:
                    months_data[m]["selic_diaria_media_mensal_pct"] = round(sum(vals) / len(vals), 4)

        if cdi_path.exists():
            t = pq.read_table(cdi_path).to_pylist()
            cdi_daily_by_month: dict[str, list[float]] = {}
            for r in t:
                m = r.get("ano_mes")
                if not m and r.get("valor") is not None:
                    continue
                cdi_daily_by_month.setdefault(m, []).append(r["valor"])

            for m, vals in cdi_daily_by_month.items():
                if vals and m in months_data:
                    months_data[m]["cdi_diario_medio_mensal_pct"] = round(sum(vals) / len(vals), 4)

        if ibcbr_path.exists():
            t = pq.read_table(ibcbr_path).to_pylist()
            for r in t:
                m = r.get("ano_mes")
                if not m:
                    continue
                entry = get_or_create(m)
                entry["ibcbr_indice"] = r.get("valor")

        # Compute ex-post real interest rate (Selic Meta - IPCA 12m) and spreads
        for entry in months_data.values():
            selic = entry.get("selic_meta_fim_mes_pct")
            ipca12 = entry.get("ipca_acumulado_12m_pct")
            if selic is not None and ipca12 is not None:
                entry["juros_real_ex_post_pct"] = round(selic - ipca12, 2)

            cdi_avg = entry.get("cdi_diario_medio_mensal_pct")
            selic_avg = entry.get("selic_diaria_media_mensal_pct")
            if cdi_avg is not None and selic_avg is not None:
                entry["spread_cdi_selic_pct"] = round(cdi_avg - selic_avg, 4)

        # Sort chronologically
        gold_rows = [months_data[k] for k in sorted(months_data.keys())]
        gold_table = pa.Table.from_pylist(gold_rows)

        gold_dir = self.settings.data_dir / "bcb_sgs" / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / "features_macro_brasil.parquet"
        pq.write_table(gold_table, gold_path, compression="zstd")

        # Flat copy for backwards compatibility
        flat_gold = self.settings.data_dir / "gold"
        flat_gold.mkdir(parents=True, exist_ok=True)
        pq.write_table(gold_table, flat_gold / "bcb_sgs.parquet", compression="zstd")

        return gold_path

    def extract_table(self, table_name: str) -> list[dict[str, Any]]:
        """Extract and normalize a specific table."""
        extractors = {
            "sgs_catalogo": self._extract_sgs_catalogo,
            "sgs_ipca": self._extract_sgs_ipca,
            "sgs_igpm": self._extract_sgs_igpm,
            "sgs_selic": self._extract_sgs_selic,
            "sgs_cdi": self._extract_sgs_cdi,
            "sgs_ibcbr": self._extract_sgs_ibcbr,
        }
        if table_name not in extractors:
            raise ValueError(f"Unknown table '{table_name}'. Valid: {list(extractors.keys())}")

        records = extractors[table_name]()
        self._persist_bronze_snapshot(table_name, records)
        self._persist_silver_table(table_name, records)
        return records

    def extract_all(self) -> dict[str, int]:
        """Ingest, normalize, validate, and materialize all 6 BCB SGS tables plus gold feature view."""
        table_names = [
            "sgs_catalogo",
            "sgs_ipca",
            "sgs_igpm",
            "sgs_selic",
            "sgs_cdi",
            "sgs_ibcbr",
        ]
        results: dict[str, int] = {}

        for tname in table_names:
            records = self.extract_table(tname)
            results[tname] = len(records)
            logger.info("Materialized table '%s' with %d records in data/bcb_sgs/silver/", tname, len(records))

        # Build Gold feature view
        gold_path = self._build_and_write_gold_features()
        results["features_macro_brasil"] = pq.read_metadata(gold_path).num_rows
        logger.info("Materialized Gold ML Features view in %s with %d rows", gold_path, results["features_macro_brasil"])

        return results

    def extract(self) -> pa.Table:
        """Execute default extraction (returns IPCA table as primary series)."""
        records = self.extract_table("sgs_ipca")
        return pa.Table.from_pylist(records)

    def run(self) -> dict[str, Any]:
        """Execute full connector workflow."""
        return self.extract_all()


register_connector("bcb_sgs", BCBSGSConnector)

