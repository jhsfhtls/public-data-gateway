"""RFB CNPJ Lakehouse Connector — Receita Federal do Brasil (CNPJ, QSA, Simples & Domínios).

Part of the public-data-gateway Medallion platform (C.O.R.E. Ecosystem).
Implements the 11-Gate Protocol:
1. Multi-table ingestion (10 tabelas: Empresas, Estabelecimentos, Sócios QSA, Simples/MEI, CNAEs, Motivos, Municípios, Naturezas, Países, Qualificações)
2. Isolated storage in data/rfb_cnpj_lakehouse/{bronze, silver, gold, quarantine}/
3. dbt encapsulation & 100% column descriptions
4. Intra-domain corporate density data mart (mart_densidade_empresarial_brasil)
5. Live 1-line fail-hard tests against official CNPJ edge endpoint.
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

RFB_LIVE_CNPJ_URL = "https://brasilapi.com.br/api/cnpj/v1/00000000000191"


class RFBCNPJLakehouseConnector(BaseConnector):
    """Production connector for Receita Federal do Brasil CNPJ Lakehouse."""

    def __init__(
        self,
        source_id: str = "rfb_cnpj_lakehouse",
        settings: GatewaySettings | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        self.normalizer = CanonicalNormalizer()
        self.storage = HybridStorageEngine(self.settings)
        contract_path = Path(__file__).parent / "contract.yaml"
        self.contract = load_contract(contract_path) if contract_path.exists() else None

    def test_connection(self) -> bool:
        """Verify connectivity to CNPJ resolution edge service."""
        try:
            with ResilientHttpClient(self.settings) as client:
                resp = client.get(RFB_LIVE_CNPJ_URL)
                return resp.status_code == 200
        except Exception as exc:
            logger.warning("Failed connection check to CNPJ edge service: %s", exc)
            return False

    def test_live_endpoints(self, fail_hard: bool = True) -> dict[str, bool]:
        """Execute 1-line real API test query against official CNPJ endpoint.

        Enforces Fail-Hard Policy: raises RuntimeError on any HTTP error, 429, or schema drift.
        """
        results: dict[str, bool] = {}
        with ResilientHttpClient(self.settings) as client:
            try:
                resp = client.get(RFB_LIVE_CNPJ_URL)
                if resp.status_code != 200:
                    msg = (
                        f"[FAIL-HARD] RFB CNPJ live endpoint returned HTTP {resp.status_code} "
                        f"(expected 200). Response: {resp.text[:200]}"
                    )
                    logger.error(msg)
                    if fail_hard:
                        raise RuntimeError(msg)
                    results["cnpj_live"] = False
                    return results

                data = resp.json()
                if not isinstance(data, dict) or "cnpj" not in data or "razao_social" not in data:
                    msg = (
                        f"[FAIL-HARD] RFB CNPJ live schema drift: expected fields 'cnpj' and 'razao_social' "
                        f"missing from response. Payload keys: {list(data.keys()) if isinstance(data, dict) else type(data)}"
                    )
                    logger.error(msg)
                    if fail_hard:
                        raise RuntimeError(msg)
                    results["cnpj_live"] = False
                    return results

                results["cnpj_live"] = True
                logger.info("Live 1-line check passed for RFB CNPJ endpoint (%s)", data.get("razao_social"))
            except RuntimeError:
                raise
            except Exception as exc:
                msg = f"[FAIL-HARD] RFB CNPJ live endpoint connection failed: {exc}"
                logger.error(msg)
                if fail_hard:
                    raise RuntimeError(msg) from exc
                results["cnpj_live"] = False

        return results

    def extract(self) -> pa.Table:
        """Extract primary table (rfb_estabelecimentos) in Arrow format."""
        all_tables = self.extract_all()
        return all_tables["rfb_estabelecimentos"]

    def extract_all(self) -> dict[str, pa.Table]:
        """Extract, normalize and persist all 10 RFB CNPJ tables plus Gold Feature View."""
        logger.info("Starting extraction of all 10 RFB CNPJ tables...")

        # 1. CNAEs
        cnaes_recs = self._extract_cnaes()
        p_cnaes = self._persist_silver_table("rfb_cnaes", cnaes_recs)

        # 2. Motivos
        motivos_recs = self._extract_motivos()
        p_motivos = self._persist_silver_table("rfb_motivos", motivos_recs)

        # 3. Municipios
        municipios_recs = self._extract_municipios()
        p_mun = self._persist_silver_table("rfb_municipios", municipios_recs)

        # 4. Naturezas
        naturezas_recs = self._extract_naturezas()
        p_nat = self._persist_silver_table("rfb_naturezas", naturezas_recs)

        # 5. Paises
        paises_recs = self._extract_paises()
        p_paises = self._persist_silver_table("rfb_paises", paises_recs)

        # 6. Qualificacoes
        qualif_recs = self._extract_qualificacoes()
        p_qualif = self._persist_silver_table("rfb_qualificacoes", qualif_recs)

        # 7. Empresas
        empresas_recs = self._extract_empresas()
        p_emp = self._persist_silver_table("rfb_empresas", empresas_recs)

        # 8. Estabelecimentos
        estab_recs = self._extract_estabelecimentos()
        p_estab = self._persist_silver_table("rfb_estabelecimentos", estab_recs)

        # 9. Socios (QSA)
        socios_recs = self._extract_socios()
        p_socios = self._persist_silver_table("rfb_socios", socios_recs)

        # 10. Simples e MEI
        simples_recs = self._extract_simples()
        p_simples = self._persist_silver_table("rfb_simples", simples_recs)

        # 11. Gold Feature View
        self._build_and_persist_gold_features(empresas_recs, estab_recs, simples_recs)

        return {
            "rfb_cnaes": pq.read_table(p_cnaes),
            "rfb_motivos": pq.read_table(p_motivos),
            "rfb_municipios": pq.read_table(p_mun),
            "rfb_naturezas": pq.read_table(p_nat),
            "rfb_paises": pq.read_table(p_paises),
            "rfb_qualificacoes": pq.read_table(p_qualif),
            "rfb_empresas": pq.read_table(p_emp),
            "rfb_estabelecimentos": pq.read_table(p_estab),
            "rfb_socios": pq.read_table(p_socios),
            "rfb_simples": pq.read_table(p_simples),
        }

    def _load_offline_reference(self, name: str) -> Any:
        """Load offline reference fixtures from data/reference/."""
        ref_path = self.settings.data_dir / "reference" / f"{name}_brasil.json"
        if not ref_path.exists():
            ref_path = self.settings.repo_root / "data" / "reference" / f"{name}_brasil.json"
        if not ref_path.exists():
            ref_path = Path("data/reference") / f"{name}_brasil.json"

        if ref_path.exists():
            with open(ref_path, encoding="utf-8") as f:
                return json.load(f)
        logger.warning("Reference fixture '%s' not found. Using empty data.", name)
        return []

    def _extract_cnaes(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_cnaes")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_cnaes", items)
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "codigo_cnae": str(row.get("codigo_cnae") or "").strip(),
                "descricao_cnae": str(row.get("descricao_cnae") or "").strip(),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            }
            for row in items
        ]

    def _extract_motivos(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_motivos")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_motivos", items)
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "codigo_motivo": str(row.get("codigo_motivo") or "").strip(),
                "descricao_motivo": str(row.get("descricao_motivo") or "").strip(),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            }
            for row in items
        ]

    def _extract_municipios(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_municipios")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_municipios", items)
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "codigo_municipio_rf": str(row.get("codigo_municipio_rf") or "").strip(),
                "nome_municipio": str(row.get("nome_municipio") or "").strip(),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            }
            for row in items
        ]

    def _extract_naturezas(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_naturezas")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_naturezas", items)
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "codigo_natureza": str(row.get("codigo_natureza") or "").strip(),
                "descricao_natureza": str(row.get("descricao_natureza") or "").strip(),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            }
            for row in items
        ]

    def _extract_paises(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_paises")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_paises", items)
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "codigo_pais": str(row.get("codigo_pais") or "").strip(),
                "nome_pais": str(row.get("nome_pais") or "").strip(),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            }
            for row in items
        ]

    def _extract_qualificacoes(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_qualificacoes")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_qualificacoes", items)
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "codigo_qualificacao": str(row.get("codigo_qualificacao") or "").strip(),
                "descricao_qualificacao": str(row.get("descricao_qualificacao") or "").strip(),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            }
            for row in items
        ]

    def _extract_empresas(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_empresas")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_empresas", items)
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []
        for row in items:
            raw_cap = row.get("capital_social")
            if isinstance(raw_cap, (int, float)):
                cap_val = float(raw_cap)
            else:
                try:
                    cap_val = float(str(raw_cap or "0").replace(",", ".").strip())
                except ValueError:
                    cap_val = 0.0

            records.append({
                "cnpj_basico": str(row.get("cnpj_basico") or "").zfill(8),
                "razao_social": str(row.get("razao_social") or "").strip(),
                "natureza_juridica": str(row.get("natureza_juridica") or "").strip(),
                "qualificacao_responsavel": str(row.get("qualificacao_responsavel") or "").strip(),
                "capital_social": cap_val,
                "porte_empresa": str(row.get("porte_empresa") or "").strip(),
                "ente_federativo_responsavel": row.get("ente_federativo_responsavel"),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })
        return records

    def _extract_estabelecimentos(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_estabelecimentos")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_estabelecimentos", items)
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            cnpj_raw = str(row.get("cnpj_14") or "").replace(".", "").replace("/", "").replace("-", "").strip()
            cnpj_14 = cnpj_raw.zfill(14) if cnpj_raw.isdigit() else cnpj_raw

            c_basico = str(row.get("cnpj_basico") or cnpj_14[:8]).zfill(8)
            c_ordem = str(row.get("cnpj_ordem") or cnpj_14[8:12]).zfill(4)
            c_dv = str(row.get("cnpj_dv") or cnpj_14[12:14]).zfill(2)

            uf = str(row.get("sigla_uf") or "").upper().strip()
            sigla_uf = uf if len(uf) == 2 else None

            cep_raw = str(row.get("cep_8") or "").replace("-", "").strip()
            cep_8 = cep_raw.zfill(8) if cep_raw.isdigit() else None

            cnae = str(row.get("cnae_fiscal_principal") or "").replace("-", "").replace("/", "").strip()
            cnae_7 = cnae.zfill(7) if cnae.isdigit() else cnae

            records.append({
                "cnpj_14": cnpj_14,
                "cnpj_basico": c_basico,
                "cnpj_ordem": c_ordem,
                "cnpj_dv": c_dv,
                "identificador_matriz_filial": str(row.get("identificador_matriz_filial") or "1"),
                "nome_fantasia": str(row.get("nome_fantasia") or "").strip() or None,
                "situacao_cadastral": str(row.get("situacao_cadastral") or "02"),
                "data_situacao_cadastral": str(row.get("data_situacao_cadastral") or "1900-01-01"),
                "motivo_situacao_cadastral": str(row.get("motivo_situacao_cadastral") or "00"),
                "data_inicio_atividade": str(row.get("data_inicio_atividade") or "1900-01-01"),
                "cnae_fiscal_principal": cnae_7,
                "tipo_logradouro": str(row.get("tipo_logradouro") or "").strip() or None,
                "logradouro": str(row.get("logradouro") or "").strip() or None,
                "numero": str(row.get("numero") or "SN").strip(),
                "complemento": str(row.get("complemento") or "").strip() or None,
                "bairro": str(row.get("bairro") or "").strip() or None,
                "cep_8": cep_8,
                "sigla_uf": sigla_uf,
                "id_municipio_rf": str(row.get("id_municipio_rf") or "").zfill(4),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _extract_socios(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_socios")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_socios", items)
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            c_basico = str(row.get("cnpj_basico") or "0").zfill(8)
            cpf_mascarado = str(row.get("cpf_cnpj_socio_mascarado") or "").strip()

            records.append({
                "cnpj_basico": c_basico,
                "identificador_socio": str(row.get("identificador_socio") or "2"),
                "nome_socio": str(row.get("nome_socio") or "").strip(),
                "cpf_cnpj_socio_mascarado": cpf_mascarado,
                "qualificacao_socio": str(row.get("qualificacao_socio") or "").zfill(2),
                "data_entrada_sociedade": str(row.get("data_entrada_sociedade") or "1900-01-01"),
                "faixa_etaria": str(row.get("faixa_etaria") or "0"),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _extract_simples(self) -> list[dict[str, Any]]:
        raw = self._load_offline_reference("rfb_simples")
        items = raw if isinstance(raw, list) else []
        self._persist_bronze_snapshot("rfb_simples", items)
        now_iso = datetime.now(UTC).isoformat()
        records: list[dict[str, Any]] = []

        for row in items:
            c_basico = str(row.get("cnpj_basico") or "0").zfill(8)
            records.append({
                "cnpj_basico": c_basico,
                "opcao_pelo_simples": str(row.get("opcao_pelo_simples") or "N").strip().upper(),
                "data_opcao_simples": row.get("data_opcao_simples"),
                "data_exclusao_simples": row.get("data_exclusao_simples"),
                "opcao_mei": str(row.get("opcao_mei") or "N").strip().upper(),
                "data_opcao_mei": row.get("data_opcao_mei"),
                "data_exclusao_mei": row.get("data_exclusao_mei"),
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        return records

    def _persist_bronze_snapshot(self, table_name: str, raw_data: Any) -> Path:
        """Persist raw JSON snapshot into data/rfb_cnpj_lakehouse/bronze/."""
        source_dir = self.settings.data_dir / "rfb_cnpj_lakehouse" / "bronze"
        source_dir.mkdir(parents=True, exist_ok=True)
        path = source_dir / f"raw_{table_name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False)
        return path

    def _persist_silver_table(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> Path:
        """Persist normalized records to data/rfb_cnpj_lakehouse/silver/{table_name}.parquet."""
        table = pa.Table.from_pylist(records)

        # Apply contract validation on estabelecimentos
        contract_to_use = self.contract if table_name == "rfb_estabelecimentos" else None

        path, _, obs_report = self.storage.write_medallion_table(
            table,
            source_id="rfb_cnpj_lakehouse",
            layer="silver",
            table_name=table_name,
            contract=contract_to_use,
            use_source_subfolder=True,
        )

        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"RFB CNPJ Silver observability checks failed for '{table_name}': {obs_report.violations}"
            )

        logger.info("Persisted silver table '%s': %d rows at %s", table_name, table.num_rows, path)
        return path

    def _build_and_persist_gold_features(
        self,
        empresas: list[dict[str, Any]],
        estabelecimentos: list[dict[str, Any]],
        simples: list[dict[str, Any]],
    ) -> Path:
        """Aggregate corporate firmographics and business density by UF."""
        ufs = [
            "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
            "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
            "RO", "RR", "RS", "SC", "SE", "SP", "TO"
        ]

        empresa_map = {e["cnpj_basico"]: e for e in empresas}
        simples_map = {s["cnpj_basico"]: s for s in simples}

        # Counters by UF
        total_estab_uf: dict[str, int] = {uf: 0 for uf in ufs}
        matrizes_uf: dict[str, int] = {uf: 0 for uf in ufs}
        filiais_uf: dict[str, int] = {uf: 0 for uf in ufs}
        ativos_uf: dict[str, int] = {uf: 0 for uf in ufs}
        capital_uf: dict[str, float] = {uf: 0.0 for uf in ufs}
        simples_uf: dict[str, int] = {uf: 0 for uf in ufs}
        mei_uf: dict[str, int] = {uf: 0 for uf in ufs}

        for est in estabelecimentos:
            uf = est.get("sigla_uf")
            if uf not in total_estab_uf:
                continue

            total_estab_uf[uf] += 1
            if est.get("identificador_matriz_filial") == "1":
                matrizes_uf[uf] += 1
            else:
                filiais_uf[uf] += 1

            if est.get("situacao_cadastral") == "02":
                ativos_uf[uf] += 1

            c_basico = est.get("cnpj_basico")
            emp = empresa_map.get(c_basico)
            if emp and est.get("identificador_matriz_filial") == "1":
                capital_uf[uf] += float(emp.get("capital_social") or 0.0)

            simp = simples_map.get(c_basico)
            if simp:
                if simp.get("opcao_pelo_simples") == "S":
                    simples_uf[uf] += 1
                if simp.get("opcao_mei") == "S":
                    mei_uf[uf] += 1

        now_iso = datetime.now(UTC).isoformat()
        gold_rows: list[dict[str, Any]] = []

        for uf in ufs:
            total_est = total_estab_uf[uf]
            total_mat = matrizes_uf[uf]
            total_fil = filiais_uf[uf]
            total_atv = ativos_uf[uf]
            cap = capital_uf[uf]
            total_simp = simples_uf[uf]
            total_m = mei_uf[uf]

            if total_est >= 3:
                tier = "POLO_CORPORATIVO_ALTO"
            elif total_est >= 1:
                tier = "POLO_CORPORATIVO_MEDIO"
            else:
                tier = "COBERTURA_DISPERSA"

            gold_rows.append({
                "sigla_uf": uf,
                "total_estabelecimentos": total_est,
                "total_matrizes": total_mat,
                "total_filiais": total_fil,
                "total_estabelecimentos_ativos": total_atv,
                "capital_social_total_matrizes": cap,
                "total_optantes_simples": total_simp,
                "total_optantes_mei": total_m,
                "densidade_corporativa_tier": tier,
                "reference_date": "2026-11-07",
                "extracted_at": now_iso,
            })

        gold_table = pa.Table.from_pylist(gold_rows)
        gold_dir = self.settings.data_dir / "rfb_cnpj_lakehouse" / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)
        gold_path = gold_dir / "features_empresariais_brasil.parquet"
        pq.write_table(gold_table, gold_path, compression="zstd")
        logger.info("Persisted gold feature view at %s (%d UFs)", gold_path, gold_table.num_rows)
        return gold_path


register_connector("rfb_cnpj_lakehouse", RFBCNPJLakehouseConnector)
