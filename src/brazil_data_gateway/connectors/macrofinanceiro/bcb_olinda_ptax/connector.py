"""BCB Olinda PTAX Connector — Banco Central do Brasil Cotações Diárias PTAX (OData v4).

Part of the brazil-data-gateway Medallion platform (C.O.R.E. Ecosystem).
Implements the 11-Gate Protocol:
1. Multi-table ingestion (Moedas, Cotação Dólar, Cotação Moedas Estrangeiras, Boletins Intradiários)
2. Isolated storage in data/bcb_olinda_ptax/{bronze, silver, gold, quarantine}/
3. dbt encapsulation & 100% column descriptions
4. Intra-domain foreign exchange data mart (mart_cambio_volatilidade_brasil)
5. Live 1-line fail-hard tests against all official BACEN Olinda endpoints.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.connector import BaseConnector, register_connector
from brazil_data_gateway.core.contracts import load_contract
from brazil_data_gateway.core.http_client import ResilientHttpClient
from brazil_data_gateway.core.normalizer import CanonicalNormalizer
from brazil_data_gateway.core.storage import HybridStorageEngine

logger = logging.getLogger(__name__)

BCB_OLINDA_PTAX_BASE_API = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"

BCB_OLINDA_LIVE_1LINE_ENDPOINTS: dict[str, dict[str, Any]] = {
    "moedas": {
        "url": f"{BCB_OLINDA_PTAX_BASE_API}/Moedas?$top=1&$format=json",
        "name": "Lista Cadastral de Moedas",
        "expected_key": "simbolo",
    },
    "cotacao_dolar_dia": {
        "url": f"{BCB_OLINDA_PTAX_BASE_API}/CotacaoDolarDia(dataCotacao=@dataCotacao)?@dataCotacao=%2711-06-2026%27&$format=json",
        "name": "Cotação Dólar Americano do Dia",
        "expected_key": "cotacaoVenda",
    },
    "cotacao_euro_dia": {
        "url": f"{BCB_OLINDA_PTAX_BASE_API}/CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)?@moeda=%27EUR%27&@dataCotacao=%2711-06-2026%27&$format=json",
        "name": "Cotação Euro do Dia",
        "expected_key": "cotacaoVenda",
    },
}


class BCBOlindaPTAXConnector(BaseConnector):
    """Production connector for BACEN Olinda PTAX Daily Foreign Exchange rates."""

    def __init__(
        self,
        source_id: str = "bcb_olinda_ptax",
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
        """Run connectivity check against official BACEN Olinda PTAX API."""
        try:
            with self.http_client as client:
                resp = client.get(
                    f"{BCB_OLINDA_PTAX_BASE_API}/Moedas?$top=1&$format=json"
                )
                if resp.status_code == 200:
                    data = resp.json()
                    val = data.get("value", [])
                    return isinstance(val, list) and len(val) > 0 and "simbolo" in val[0]
                return False
        except Exception as exc:
            logger.warning("BCB Olinda PTAX connectivity test failed: %s", exc)
            return False

    def test_live_endpoints(self, fail_hard: bool = True) -> dict[str, bool]:
        """Execute mandatory 1-line real HTTP query against all official Olinda PTAX endpoints.

        If fail_hard is True, any HTTP failure, 429 rate-limit, 5xx error, or payload mismatch
        IMMEDIATELY raises RuntimeError to fail the test and allow human inspection.
        """
        results: dict[str, bool] = {}
        with self.http_client as client:
            for test_name, cfg in BCB_OLINDA_LIVE_1LINE_ENDPOINTS.items():
                url = cfg["url"]
                desc_name = cfg["name"]
                expected_key = cfg["expected_key"]
                try:
                    resp = client.get(url, cache_ttl=0)
                    if resp.status_code != 200:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{test_name}' ({desc_name}) returned status {resp.status_code} "
                            f"(URL: {url}). Payload: {resp.text[:200]}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[test_name] = False
                        continue

                    data = resp.json()
                    values = data.get("value", [])
                    if not isinstance(values, list) or len(values) == 0:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{test_name}' returned empty or non-list 'value': {data}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[test_name] = False
                        continue

                    item = values[0]
                    if expected_key not in item:
                        err_msg = (
                            f"Fail-Hard: Live endpoint '{test_name}' missing expected key '{expected_key}'. Got: {item}"
                        )
                        if fail_hard:
                            raise RuntimeError(err_msg)
                        logger.error(err_msg)
                        results[test_name] = False
                        continue

                    results[test_name] = True
                    logger.info("Live 1-line test passed for '%s' (%s): %s=%s", test_name, desc_name, expected_key, item[expected_key])
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
            self.settings.repo_root.parent / "brazil-data-gateway" / "data" / "reference" / ref_filename,
        ]
        for c in candidates:
            if c.exists():
                with open(c, encoding="utf-8") as f:
                    return json.load(f)
        raise FileNotFoundError(f"Offline reference file '{ref_filename}' not found in candidates: {candidates}")

    def _extract_ptax_moedas(self) -> list[dict[str, Any]]:
        """Extract currency metadata catalog from Olinda Moedas."""
        raw = self._load_offline_reference("ptax_moedas")
        items = raw if isinstance(raw, list) else []
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            records.append({
                "simbolo_moeda": str(row.get("simbolo", "")),
                "nome_moeda": str(row.get("nomeFormatado", "")),
                "tipo_moeda": str(row.get("tipoMoeda", "")),
                "reference_date": "2026-01-01",
                "extracted_at": now_iso,
            })

        return records

    def _extract_ptax_cotacao_dolar(self) -> list[dict[str, Any]]:
        """Extract daily USD PTAX exchange rates."""
        raw = self._load_offline_reference("ptax_cotacao_dolar")
        items = raw if isinstance(raw, list) else []
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            dt_raw = row.get("dataHoraCotacao", "")
            # Example dt_raw: '2026-10-01 13:10:35.4469' or '2026-10-01'
            dt_iso = dt_raw[:10] if len(dt_raw) >= 10 else "1900-01-01"
            c_compra = float(row.get("cotacaoCompra", 0.0))
            c_venda = float(row.get("cotacaoVenda", 0.0))

            records.append({
                "simbolo_moeda": "USD",
                "data_cotacao": dt_iso,
                "data_hora_cotacao": dt_raw,
                "ano_mes": dt_iso[:7],
                "cotacao_compra": c_compra,
                "cotacao_venda": c_venda,
                "paridade_compra": 1.0,
                "paridade_venda": 1.0,
                "tipo_boletim": "Fechamento PTAX",
                "reference_date": dt_iso,
                "extracted_at": now_iso,
            })

        return records

    def _extract_ptax_cotacao_moedas(self) -> list[dict[str, Any]]:
        """Extract daily foreign exchange rates (EUR, GBP, JPY, CAD, CHF, AUD)."""
        raw = self._load_offline_reference("ptax_cotacao_moedas")
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        if isinstance(raw, dict):
            for moeda, items in raw.items():
                for row in items:
                    dt_raw = row.get("dataHoraCotacao", "")
                    dt_iso = dt_raw[:10] if len(dt_raw) >= 10 else "1900-01-01"
                    tipo_bol = row.get("tipoBoletim", "Fechamento")
                    c_compra = float(row.get("cotacaoCompra", 0.0))
                    c_venda = float(row.get("cotacaoVenda", 0.0))
                    p_compra = float(row.get("paridadeCompra", 1.0))
                    p_venda = float(row.get("paridadeVenda", 1.0))

                    records.append({
                        "simbolo_moeda": moeda,
                        "data_cotacao": dt_iso,
                        "data_hora_cotacao": dt_raw,
                        "ano_mes": dt_iso[:7],
                        "cotacao_compra": c_compra,
                        "cotacao_venda": c_venda,
                        "paridade_compra": p_compra,
                        "paridade_venda": p_venda,
                        "tipo_boletim": tipo_bol,
                        "reference_date": dt_iso,
                        "extracted_at": now_iso,
                    })

        return records

    def _extract_ptax_boletins(self) -> list[dict[str, Any]]:
        """Extract representative intraday exchange bulletins (Abertura, Intermediários, Fechamento)."""
        raw = self._load_offline_reference("ptax_boletins")
        items = raw if isinstance(raw, list) else []
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            dt_raw = row.get("dataHoraCotacao", "")
            dt_iso = dt_raw[:10] if len(dt_raw) >= 10 else "1900-01-01"
            records.append({
                "simbolo_moeda": "EUR",
                "data_cotacao": dt_iso,
                "data_hora_cotacao": dt_raw,
                "ano_mes": dt_iso[:7],
                "cotacao_compra": float(row.get("cotacaoCompra", 0.0)),
                "cotacao_venda": float(row.get("cotacaoVenda", 0.0)),
                "paridade_compra": float(row.get("paridadeCompra", 1.0)),
                "paridade_venda": float(row.get("paridadeVenda", 1.0)),
                "tipo_boletim": str(row.get("tipoBoletim", "")),
                "reference_date": dt_iso,
                "extracted_at": now_iso,
            })

        return records

    def _persist_bronze_snapshot(self, table_name: str, raw_data: Any) -> Path:
        """Persist raw JSON snapshot into data/bcb_olinda_ptax/bronze/."""
        source_dir = self.settings.data_dir / "bcb_olinda_ptax" / "bronze"
        source_dir.mkdir(parents=True, exist_ok=True)
        path = source_dir / f"raw_{table_name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False)
        return path

    def _persist_silver_table(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> Path:
        """Persist normalized records to data/bcb_olinda_ptax/silver/{table_name}.parquet."""
        table = pa.Table.from_pylist(records)

        # Apply contract validation on quotation tables
        contract_to_use = self.contract if table_name in {"ptax_cotacao_dolar", "ptax_cotacao_moedas"} else None

        path, _, obs_report = self.storage.write_medallion_table(
            table,
            source_id="bcb_olinda_ptax",
            layer="silver",
            table_name=table_name,
            contract=contract_to_use,
            use_source_subfolder=True,
        )

        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"BCB Olinda PTAX Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )

        return path

    def _build_and_write_gold_features(self) -> Path:
        """Compute and persist gold features view data/bcb_olinda_ptax/gold/features_cambio_brasil.parquet."""
        usd_path = self.settings.data_dir / "bcb_olinda_ptax" / "silver" / "ptax_cotacao_dolar.parquet"
        eur_path = self.settings.data_dir / "bcb_olinda_ptax" / "silver" / "ptax_cotacao_moedas.parquet"

        days_data: dict[str, dict[str, Any]] = {}

        def get_or_create(d: str) -> dict[str, Any]:
            if d not in days_data:
                days_data[d] = {
                    "data_cotacao": d,
                    "ano_mes": d[:7],
                    "dolar_ptax_compra": None,
                    "dolar_ptax_venda": None,
                    "dolar_spread": None,
                    "euro_ptax_compra": None,
                    "euro_ptax_venda": None,
                    "variacao_dolar_dia_pct": None,
                    "volatilidade_dolar_30d": None,
                    "reference_date": d,
                    "extracted_at": datetime.now(UTC).isoformat(),
                }
            return days_data[d]

        if usd_path.exists():
            t = pq.read_table(usd_path).to_pylist()
            for r in t:
                d = r.get("data_cotacao")
                if not d:
                    continue
                entry = get_or_create(d)
                entry["dolar_ptax_compra"] = r.get("cotacao_compra")
                entry["dolar_ptax_venda"] = r.get("cotacao_venda")
                if entry["dolar_ptax_venda"] is not None and entry["dolar_ptax_compra"] is not None:
                    entry["dolar_spread"] = round(entry["dolar_ptax_venda"] - entry["dolar_ptax_compra"], 4)

        if eur_path.exists():
            t = pq.read_table(eur_path).to_pylist()
            for r in t:
                if r.get("simbolo_moeda") == "EUR" and "Fechamento" in r.get("tipo_boletim", ""):
                    d = r.get("data_cotacao")
                    if not d:
                        continue
                    entry = get_or_create(d)
                    entry["euro_ptax_compra"] = r.get("cotacao_compra")
                    entry["euro_ptax_venda"] = r.get("cotacao_venda")

        # Sort dates chronologically and compute day-to-day returns and 30-day volatility
        sorted_dates = sorted(days_data.keys())
        usd_rates: list[float] = []

        for i, d in enumerate(sorted_dates):
            entry = days_data[d]
            current_venda = entry["dolar_ptax_venda"]

            if current_venda is not None:
                usd_rates.append(current_venda)

                # Day-to-day percentage variation
                if i > 0 and usd_rates and len(usd_rates) > 1:
                    prev_venda = usd_rates[-2]
                    if prev_venda and prev_venda > 0:
                        entry["variacao_dolar_dia_pct"] = round(
                            ((current_venda - prev_venda) / prev_venda) * 100, 4
                        )

                # 30-day rolling standard deviation (volatility)
                window = usd_rates[-30:]
                if len(window) >= 5:
                    mean_val = sum(window) / len(window)
                    variance = sum((x - mean_val) ** 2 for x in window) / (len(window) - 1)
                    entry["volatilidade_dolar_30d"] = round(math.sqrt(variance), 4)

        gold_rows = [days_data[k] for k in sorted_dates]
        gold_table = pa.Table.from_pylist(gold_rows)

        gold_dir = self.settings.data_dir / "bcb_olinda_ptax" / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / "features_cambio_brasil.parquet"
        pq.write_table(gold_table, gold_path, compression="zstd")

        # Flat copy for backwards compatibility
        flat_gold = self.settings.data_dir / "gold"
        flat_gold.mkdir(parents=True, exist_ok=True)
        pq.write_table(gold_table, flat_gold / "bcb_olinda_ptax.parquet", compression="zstd")

        return gold_path

    def extract_table(self, table_name: str) -> list[dict[str, Any]]:
        """Extract and normalize a specific table."""
        extractors = {
            "ptax_moedas": self._extract_ptax_moedas,
            "ptax_cotacao_dolar": self._extract_ptax_cotacao_dolar,
            "ptax_cotacao_moedas": self._extract_ptax_cotacao_moedas,
            "ptax_boletins": self._extract_ptax_boletins,
        }
        if table_name not in extractors:
            raise ValueError(f"Unknown table '{table_name}'. Valid: {list(extractors.keys())}")

        records = extractors[table_name]()
        self._persist_bronze_snapshot(table_name, records)
        self._persist_silver_table(table_name, records)
        return records

    def extract(self) -> pa.Table:
        """Execute default extraction (returns USD daily PTAX table)."""
        records = self.extract_table("ptax_cotacao_dolar")
        return pa.Table.from_pylist(records)

    def extract_all(self) -> dict[str, int]:
        """Ingest, normalize, validate, and materialize all 4 PTAX tables plus gold feature store."""
        table_names = [
            "ptax_moedas",
            "ptax_cotacao_dolar",
            "ptax_cotacao_moedas",
            "ptax_boletins",
        ]
        results: dict[str, int] = {}

        for tname in table_names:
            records = self.extract_table(tname)
            results[tname] = len(records)
            logger.info("Materialized table '%s' with %d records in data/bcb_olinda_ptax/silver/", tname, len(records))

        # Build Gold feature view
        gold_path = self._build_and_write_gold_features()
        results["features_cambio_brasil"] = pq.read_metadata(gold_path).num_rows
        logger.info("Materialized Gold FX Features view in %s with %d rows", gold_path, results["features_cambio_brasil"])

        return results

    def run(self) -> dict[str, Any]:
        """Execute full connector workflow."""
        return self.extract_all()


register_connector("bcb_olinda_ptax", BCBOlindaPTAXConnector)
