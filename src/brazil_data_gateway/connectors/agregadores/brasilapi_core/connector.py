"""BrasilAPI Core Connector — Borda Síncrona, Bancos, PIX, CVM, Feriados, DDDs & CEPs.

Part of the public-data-gateway Medallion platform (compatibility alias: brazil-data-gateway).
Implements the 11-Gate Protocol:
1. Multi-table ingestion (Bancos, PIX Participantes, Corretoras CVM, Feriados Nacionais, DDDs e CEPs)
2. Isolated storage in data/brasilapi_core/{bronze, silver, gold, quarantine}/
3. dbt encapsulation & 100% column descriptions
4. Intra-domain financial & cadastral infrastructure mart (mart_ecossistema_financeiro_brasil)
5. Live 1-line fail-hard tests against all 6 official BrasilAPI endpoints.
"""

from __future__ import annotations

import json
import logging
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

BRASILAPI_BASE_URL = "https://brasilapi.com.br/api"

BRASILAPI_LIVE_1LINE_ENDPOINTS: dict[str, dict[str, Any]] = {
    "bancos": {
        "url": f"{BRASILAPI_BASE_URL}/banks/v1",
        "name": "Catálogo do Sistema Financeiro Nacional (Bancos)",
        "expected_field": "ispb",
    },
    "cep": {
        "url": f"{BRASILAPI_BASE_URL}/cep/v2/01310100",
        "name": "Consulta Geográfica de CEP de Borda",
        "expected_field": "state",
    },
    "ddd": {
        "url": f"{BRASILAPI_BASE_URL}/ddd/v1/11",
        "name": "Mapeamento Telefônico DDD x Municípios",
        "expected_field": "state",
    },
    "feriados": {
        "url": f"{BRASILAPI_BASE_URL}/feriados/v1/2026",
        "name": "Calendário de Feriados Nacionais Bancários",
        "expected_field": "date",
    },
    "corretoras": {
        "url": f"{BRASILAPI_BASE_URL}/cvm/corretoras/v1",
        "name": "Corretoras e Distribuidoras Registradas na CVM",
        "expected_field": "cnpj",
    },
    "pix_participantes": {
        "url": f"{BRASILAPI_BASE_URL}/pix/v1/participants",
        "name": "Participantes Diretos e Indiretos do Arranjo PIX",
        "expected_field": "ispb",
    },
}


class BrasilAPICoreConnector(BaseConnector):
    """Production connector for BrasilAPI Core Edge Services & Financial Metadata."""

    def __init__(
        self,
        source_id: str = "brasilapi_core",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        self.normalizer = CanonicalNormalizer()
        self.storage = HybridStorageEngine(self.settings)
        contract_path = Path(__file__).parent / "contract.yaml"
        self.contract = load_contract(contract_path) if contract_path.exists() else None

    def test_connection(self) -> bool:
        """Verify connectivity to BrasilAPI root service."""
        try:
            with ResilientHttpClient(self.settings) as client:
                resp = client.get(f"{BRASILAPI_BASE_URL}/banks/v1")
                return resp.status_code == 200
        except Exception as exc:
            logger.warning("Failed connection check to BrasilAPI: %s", exc)
            return False

    def test_live_endpoints(self, fail_hard: bool = True) -> dict[str, bool]:
        """Execute 1-line real API test queries against all 6 official BrasilAPI endpoints.

        Enforces Fail-Hard Policy: raises RuntimeError on any HTTP error, 429, or schema drift.
        """
        results: dict[str, bool] = {}
        with ResilientHttpClient(self.settings) as client:
            for key, spec in BRASILAPI_LIVE_1LINE_ENDPOINTS.items():
                url = spec["url"]
                field = spec["expected_field"]
                try:
                    resp = client.get(url)
                    if resp.status_code != 200:
                        msg = (
                            f"[FAIL-HARD] BrasilAPI endpoint '{key}' returned HTTP {resp.status_code} "
                            f"(expected 200). Response: {resp.text[:200]}"
                        )
                        logger.error(msg)
                        if fail_hard:
                            raise RuntimeError(msg)
                        results[key] = False
                        continue

                    data = resp.json()
                    sample = data[0] if isinstance(data, list) and data else data
                    if not isinstance(sample, dict) or field not in sample:
                        msg = (
                            f"[FAIL-HARD] BrasilAPI endpoint '{key}' schema drift: expected field '{field}' "
                            f"missing from response. Payload keys: {list(sample.keys()) if isinstance(sample, dict) else type(sample)}"
                        )
                        logger.error(msg)
                        if fail_hard:
                            raise RuntimeError(msg)
                        results[key] = False
                        continue

                    results[key] = True
                    logger.info("Live 1-line check passed for '%s' (%s)", key, spec["name"])
                except RuntimeError:
                    raise
                except Exception as exc:
                    msg = f"[FAIL-HARD] BrasilAPI endpoint '{key}' connection failed: {exc}"
                    logger.error(msg)
                    if fail_hard:
                        raise RuntimeError(msg) from exc
                    results[key] = False

        return results

    def extract(self) -> pa.Table:
        """Extract primary table (brasilapi_bancos) in Arrow format."""
        all_tables = self.extract_all()
        return all_tables["brasilapi_bancos"]

    def extract_all(self) -> dict[str, pa.Table]:
        """Extract, normalize and persist all 6 BrasilAPI tables plus Gold Feature View."""
        logger.info("Starting extraction of all 6 BrasilAPI Core tables...")

        # 1. Bancos
        bancos_recs = self._extract_bancos()
        p_bancos = self._persist_silver_table("brasilapi_bancos", bancos_recs)

        # 2. PIX Participantes
        pix_recs = self._extract_pix_participantes()
        p_pix = self._persist_silver_table("brasilapi_pix_participantes", pix_recs)

        # 3. Corretoras CVM
        corretoras_recs = self._extract_corretoras()
        p_corretoras = self._persist_silver_table("brasilapi_corretoras", corretoras_recs)

        # 4. Feriados Nacionais
        feriados_recs = self._extract_feriados()
        p_feriados = self._persist_silver_table("brasilapi_feriados", feriados_recs)

        # 5. DDDs
        ddds_recs = self._extract_ddds()
        p_ddds = self._persist_silver_table("brasilapi_ddds", ddds_recs)

        # 6. CEPs
        ceps_recs = self._extract_ceps()
        p_ceps = self._persist_silver_table("brasilapi_ceps", ceps_recs)

        # 7. Gold Feature View
        self._build_and_persist_gold_features(bancos_recs, corretoras_recs, ddds_recs)

        return {
            "brasilapi_bancos": pq.read_table(p_bancos),
            "brasilapi_pix_participantes": pq.read_table(p_pix),
            "brasilapi_corretoras": pq.read_table(p_corretoras),
            "brasilapi_feriados": pq.read_table(p_feriados),
            "brasilapi_ddds": pq.read_table(p_ddds),
            "brasilapi_ceps": pq.read_table(p_ceps),
        }

    def _load_offline_reference(self, name: str) -> Any:
        """Load offline reference fixtures from data/reference/."""
        ref_path = self.settings.data_dir / "reference" / f"{name}_brasil.json"
        if not ref_path.exists():
            ref_path = Path("data/reference") / f"{name}_brasil.json"

        if ref_path.exists():
            with open(ref_path, encoding="utf-8") as f:
                return json.load(f)
        logger.warning("Reference fixture '%s' not found. Using empty data.", name)
        return []

    def _extract_bancos(self) -> list[dict[str, Any]]:
        """Extract and normalize all Brazilian banking institutions."""
        raw = self._load_offline_reference("brasilapi_bancos")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("brasilapi_bancos", items)

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            ispb_raw = str(row.get("ispb") or "0").strip()
            ispb = ispb_raw.zfill(8) if ispb_raw.isdigit() else ispb_raw

            code_raw = row.get("code")
            codigo_comp = str(code_raw).zfill(3) if code_raw is not None and str(code_raw).isdigit() else None

            hq = row.get("headquarters_address") or {}
            cidade_sede = hq.get("city") or row.get("localizacao_sede")
            uf_sede = hq.get("state")
            if uf_sede and len(str(uf_sede)) == 2:
                uf_sede = str(uf_sede).upper()
            else:
                uf_sede = None

            records.append({
                "ispb": ispb,
                "codigo_compensacao": codigo_comp,
                "nome_reduzido": str(row.get("name") or "").strip(),
                "nome_completo": str(row.get("fullName") or "").strip() or str(row.get("name") or "").strip(),
                "cidade_sede": str(cidade_sede).strip() if cidade_sede else None,
                "uf_sede": uf_sede,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _extract_pix_participantes(self) -> list[dict[str, Any]]:
        """Extract and normalize all PIX participants."""
        raw = self._load_offline_reference("brasilapi_pix_participantes")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("brasilapi_pix_participantes", items)

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            ispb_raw = str(row.get("ispb") or "0").strip()
            ispb = ispb_raw.zfill(8) if ispb_raw.isdigit() else ispb_raw

            inicio = row.get("inicio_operacao")
            inicio_str = str(inicio).strip() if inicio else None

            records.append({
                "ispb": ispb,
                "nome": str(row.get("nome") or "").strip(),
                "nome_reduzido": str(row.get("nome_reduzido") or "").strip(),
                "modalidade_participacao": str(row.get("modalidade_participacao") or "").strip(),
                "tipo_participacao": str(row.get("tipo_participacao") or "").strip(),
                "inicio_operacao": inicio_str,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _extract_corretoras(self) -> list[dict[str, Any]]:
        """Extract and normalize CVM brokers and securities distributors."""
        raw = self._load_offline_reference("brasilapi_corretoras")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("brasilapi_corretoras", items)

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            cnpj_raw = str(row.get("cnpj") or "").replace(".", "").replace("/", "").replace("-", "").strip()
            cnpj_14 = cnpj_raw.zfill(14) if cnpj_raw.isdigit() else cnpj_raw

            cep_raw = str(row.get("cep") or "").replace("-", "").strip()
            cep_8 = cep_raw.zfill(8) if cep_raw.isdigit() else None

            uf = str(row.get("uf") or "").upper().strip()
            sigla_uf = uf if len(uf) == 2 else None

            records.append({
                "cnpj_14": cnpj_14,
                "codigo_cvm": str(row.get("codigo_cvm") or "").strip(),
                "nome_social": str(row.get("nome_social") or "").strip(),
                "nome_comercial": str(row.get("nome_comercial") or "").strip(),
                "status": str(row.get("status") or "").strip(),
                "municipio": str(row.get("municipio") or "").strip(),
                "sigla_uf": sigla_uf,
                "cep_8": cep_8,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _extract_feriados(self) -> list[dict[str, Any]]:
        """Extract and normalize Brazilian national bank holidays."""
        raw = self._load_offline_reference("brasilapi_feriados")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("brasilapi_feriados", items)

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            dt = str(row.get("date") or "1900-01-01").strip()
            ano = int(row.get("ano") or dt[:4])

            records.append({
                "data_feriado": dt,
                "nome_feriado": str(row.get("name") or "").strip(),
                "tipo_feriado": str(row.get("type") or "national").strip(),
                "dia_semana": str(row.get("weekday") or "").strip(),
                "ano": ano,
                "reference_date": dt,
                "extracted_at": now_iso,
            })

        return records

    def _extract_ddds(self) -> list[dict[str, Any]]:
        """Extract and normalize area codes (DDDs) to state and municipalities."""
        raw = self._load_offline_reference("brasilapi_ddds")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("brasilapi_ddds", items)

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            ddd = str(row.get("ddd") or "").strip()
            uf = str(row.get("sigla_uf") or "").upper().strip()
            city = str(row.get("nome_municipio") or "").strip()

            records.append({
                "ddd": ddd,
                "sigla_uf": uf,
                "nome_municipio": city,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _extract_ceps(self) -> list[dict[str, Any]]:
        """Extract and normalize sample reference CEPs with IBGE geocoding."""
        raw = self._load_offline_reference("brasilapi_ceps")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("brasilapi_ceps", items)

        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            cep_raw = str(row.get("cep") or "").replace("-", "").strip()
            cep_8 = cep_raw.zfill(8) if cep_raw.isdigit() else cep_raw

            uf = str(row.get("state") or "").upper().strip()
            sigla_uf = uf if len(uf) == 2 else None

            ibge_meta = row.get("ibge") or {}
            id_mun = str(ibge_meta.get("city") or "")
            id_municipio = id_mun.zfill(7) if id_mun.isdigit() else None

            loc = row.get("location") or {}
            coords = loc.get("coordinates") or {}
            lat = float(coords.get("latitude", 0.0)) if coords.get("latitude") else None
            lng = float(coords.get("longitude", 0.0)) if coords.get("longitude") else None

            records.append({
                "cep_8": cep_8,
                "sigla_uf": sigla_uf,
                "nome_municipio": str(row.get("city") or "").strip(),
                "bairro": str(row.get("neighborhood") or "").strip(),
                "logradouro": str(row.get("street") or "").strip(),
                "id_municipio": id_municipio,
                "latitude": lat,
                "longitude": lng,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _persist_bronze_snapshot(self, table_name: str, raw_data: Any) -> Path:
        """Persist raw JSON snapshot into data/brasilapi_core/bronze/."""
        source_dir = self.settings.data_dir / "brasilapi_core" / "bronze"
        source_dir.mkdir(parents=True, exist_ok=True)
        path = source_dir / f"raw_{table_name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False)
        return path

    def _persist_silver_table(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> Path:
        """Persist normalized records to data/brasilapi_core/silver/{table_name}.parquet."""
        table = pa.Table.from_pylist(records)

        # Apply contract validation on primary banking table
        contract_to_use = self.contract if table_name == "brasilapi_bancos" else None

        path, _, obs_report = self.storage.write_medallion_table(
            table,
            source_id="brasilapi_core",
            layer="silver",
            table_name=table_name,
            contract=contract_to_use,
            use_source_subfolder=True,
        )
        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"BrasilAPI Core Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )
        logger.info(
            "Persisted silver table '%s': %d rows at %s",
            table_name,
            table.num_rows,
            path,
        )
        return path

    def _build_and_persist_gold_features(
        self,
        bancos: list[dict[str, Any]],
        corretoras: list[dict[str, Any]],
        ddds: list[dict[str, Any]],
    ) -> Path:
        """Aggregate financial infrastructure and connectivity features by UF."""
        ufs = [
            "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
            "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
            "RO", "RR", "RS", "SC", "SE", "SP", "TO"
        ]

        # Pre-aggregate
        bancos_por_uf: dict[str, int] = {uf: 0 for uf in ufs}
        for b in bancos:
            uf = b.get("uf_sede")
            if uf in bancos_por_uf:
                bancos_por_uf[uf] += 1

        corretoras_por_uf: dict[str, int] = {uf: 0 for uf in ufs}
        corretoras_ativas_por_uf: dict[str, int] = {uf: 0 for uf in ufs}
        for c in corretoras:
            uf = c.get("sigla_uf")
            if uf in corretoras_por_uf:
                corretoras_por_uf[uf] += 1
                if c.get("status") == "EM FUNCIONAMENTO NORMAL":
                    corretoras_ativas_por_uf[uf] += 1

        ddds_por_uf: dict[str, set[str]] = {uf: set() for uf in ufs}
        municipios_por_uf: dict[str, int] = {uf: 0 for uf in ufs}
        for d in ddds:
            uf = d.get("sigla_uf")
            if uf in ddds_por_uf:
                ddds_por_uf[uf].add(str(d.get("ddd")))
                municipios_por_uf[uf] += 1

        now_iso = datetime.now(UTC).isoformat()
        gold_rows: list[dict[str, Any]] = []

        for uf in ufs:
            total_b = bancos_por_uf[uf]
            total_c = corretoras_por_uf[uf]
            total_ca = corretoras_ativas_por_uf[uf]
            total_ddd = len(ddds_por_uf[uf])
            total_mun = municipios_por_uf[uf]

            # Classification
            if total_b >= 10 or total_ca >= 10:
                hub_tier = "POLO_FINANCEIRO_PRINCIPAL"
            elif total_b >= 2 or total_ca >= 2:
                hub_tier = "POLO_FINANCEIRO_REGIONAL"
            else:
                hub_tier = "COBERTURA_PADRAO"

            gold_rows.append({
                "sigla_uf": uf,
                "total_bancos_sede": total_b,
                "total_corretoras": total_c,
                "total_corretoras_ativas": total_ca,
                "total_ddds_cobertura": total_ddd,
                "total_municipios_cobertos_ddd": total_mun,
                "hub_financeiro_tier": hub_tier,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        gold_table = pa.Table.from_pylist(gold_rows)
        gold_dir = self.settings.data_dir / "brasilapi_core" / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / "features_infraestrutura_brasil.parquet"
        pq.write_table(gold_table, gold_path, compression="zstd")
        logger.info("Persisted gold feature view at %s (%d UFs)", gold_path, gold_table.num_rows)
        return gold_path


register_connector("brasilapi_core", BrasilAPICoreConnector)
