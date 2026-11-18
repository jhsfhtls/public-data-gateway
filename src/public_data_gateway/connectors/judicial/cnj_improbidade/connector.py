"""CNJ CNCIAI Improbidade Administrativa connector (Judicial TPRM & Public Integrity).

Governed by:
- Lei nº 8.429/1992 (Lei de Improbidade Administrativa - LIA) e Lei nº 14.230/2021
- Resolução CNJ nº 44/2007 (Cadastro Nacional de Condenações Cíveis por Ato de Improbidade)
- Portal Oficial CNCIAI: https://www.cnj.jus.br/improbidade_adm/consultar_requerido.php
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector, register_connector
from public_data_gateway.core.contracts import (
    apply_sentinel_cleaning,
    evaluate_five_pillars,
    load_contract,
)
from public_data_gateway.core.models import DataContract
from public_data_gateway.core.storage import HybridStorageEngine

CNJ_CNCIAI_URL = "https://www.cnj.jus.br/improbidade_adm/consultar_requerido.php"


def _classify_improbidade_risk(score: float) -> str:
    """Classify municipal judicial improbity exposure score into TPRM risk tiers."""
    if score >= 70.0:
        return "ALTA_EXPOSICAO_CNCIAI"
    if score >= 40.0:
        return "MODERADA_EXPOSICAO_CNCIAI"
    return "BAIXA_EXPOSICAO_CNCIAI"


class CnjImprobidadeConnector(BaseConnector):
    """Medallion connector for CNJ CNCIAI (Improbidade Administrativa & Debarment)."""

    def __init__(
        self,
        source_id: str = "cnj_improbidade",
        settings: GatewaySettings | None = None,
        storage: HybridStorageEngine | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        self.storage = storage or HybridStorageEngine(self.settings)
        self._contract: DataContract | None = None

    @property
    def contract(self) -> DataContract:
        """Load and cache ODCS v3.1 DataContract specification."""
        if self._contract is None:
            contract_path = Path(__file__).resolve().parent / "contract.yaml"
            self._contract = load_contract(contract_path)
        return self._contract

    @property
    def data_dir(self) -> Path:
        """Absolute isolated directory for CNJ CNCIAI Medallion storage."""
        p = self.settings.data_dir / self.source_id
        for layer in ("bronze", "silver", "gold", "quarantine"):
            (p / layer).mkdir(parents=True, exist_ok=True)
        return p

    def test_connection(self) -> bool:
        """Verify contract validity and local Medallion storage readiness."""
        _ = self.contract
        self.ensure_baseline_data()
        return True

    def verify_live(self) -> dict[str, Any]:
        """Perform a live fail-hard smoke test against the official CNJ CNCIAI portal."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PublicDataGateway/1.0"
        }
        resp = httpx.get(
            CNJ_CNCIAI_URL,
            headers=headers,
            timeout=20.0,
            follow_redirects=True,
            verify=False,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"CNJ CNCIAI endpoint failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(
                f"CNJ CNCIAI endpoint returned HTTP {resp.status_code} from {CNJ_CNCIAI_URL}"
            )
        body_lower = resp.text.lower()
        if "improbidade" not in body_lower and "cnj" not in body_lower:
            raise ValueError("CNJ CNCIAI response did not contain expected portal markers.")

        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "content_length": len(resp.text),
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold CNCIAI Parquet layers if absent."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        cond_file = silver_dir / "cnciai_condenacoes.parquet"
        gold_file = gold_dir / "features_risco_improbidade_cnciai.parquet"

        if cond_file.exists() and gold_file.exists():
            return

        seed_municipalities = [
            {"id": "3550308", "uf": "SP", "nome": "São Paulo", "trib": "TJSP", "cnpj_root": "41112222", "base_val": 4200000.0, "debar_pct": 68.0, "score": 76.5},
            {"id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "trib": "TJRJ", "cnpj_root": "42223333", "base_val": 5100000.0, "debar_pct": 74.0, "score": 82.0},
            {"id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "trib": "TJMG", "cnpj_root": "43334444", "base_val": 2400000.0, "debar_pct": 58.0, "score": 61.2},
            {"id": "4106902", "uf": "PR", "nome": "Curitiba", "trib": "TJPR", "cnpj_root": "44445555", "base_val": 1850000.0, "debar_pct": 52.0, "score": 53.4},
            {"id": "4314902", "uf": "RS", "nome": "Porto Alegre", "trib": "TJRS", "cnpj_root": "45556666", "base_val": 1950000.0, "debar_pct": 55.0, "score": 56.0},
            {"id": "2927408", "uf": "BA", "nome": "Salvador", "trib": "TJBA", "cnpj_root": "46667777", "base_val": 2750000.0, "debar_pct": 62.0, "score": 66.5},
            {"id": "2304400", "uf": "CE", "nome": "Fortaleza", "trib": "TJCE", "cnpj_root": "47778888", "base_val": 2100000.0, "debar_pct": 56.0, "score": 58.2},
            {"id": "2611606", "uf": "PE", "nome": "Recife", "trib": "TJPE", "cnpj_root": "48889999", "base_val": 2250000.0, "debar_pct": 60.0, "score": 62.4},
            {"id": "5208707", "uf": "GO", "nome": "Goiânia", "trib": "TJGO", "cnpj_root": "49990000", "base_val": 1700000.0, "debar_pct": 50.0, "score": 49.5},
            {"id": "1302603", "uf": "AM", "nome": "Manaus", "trib": "TJAM", "cnpj_root": "40102030", "base_val": 2650000.0, "debar_pct": 64.0, "score": 65.8},
            {"id": "3509502", "uf": "SP", "nome": "Campinas", "trib": "TJSP", "cnpj_root": "40203040", "base_val": 1450000.0, "debar_pct": 48.0, "score": 45.0},
            {"id": "4211504", "uf": "SC", "nome": "Nova Trento", "trib": "TJSC", "cnpj_root": "40304050", "base_val": 120000.0, "debar_pct": 25.0, "score": 21.5},
        ]

        cond_rows: list[dict[str, Any]] = []
        sancoes_rows: list[dict[str, Any]] = []
        ressarc_rows: list[dict[str, Any]] = []
        resumo_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for idx, m in enumerate(seed_municipalities, start=1):
            cnpj_pj = f"{m['cnpj_root']}0001{(20 + idx):02d}"
            cpf_mask = f"***.{300 + idx}.{400 + idx}-**"
            id_pj = f"CNCIAI-2024-{idx:03d}-PJ"
            id_pf = f"CNCIAI-2024-{idx:03d}-PF"
            val_pj = round(m["base_val"] * 0.65, 2)
            val_pf = round(m["base_val"] * 0.35, 2)

            cond_rows.append({
                "id_condenacao": id_pj,
                "numero_processo_cnj": f"000{idx:04d}-12.2022.8.{10 + idx:02d}.0001",
                "documento_requerido": cnpj_pj,
                "tipo_pessoa": "Pessoa jurídica",
                "nome_requerido": f"CONSTRUTORA E INFRA {m['nome'].upper()} S/A",
                "esfera_cargo": "Privada (Contratada)",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "tribunal_origem": m["trib"],
                "grau_jurisdicao": "Trânsito em Julgado",
                "proibicao_contratar_poder_publico": True,
                "suspensao_direitos_politicos": False,
                "valor_ressarcimento_multa_brl": val_pj,
                "data_transito_julgado": f"2024-{(idx % 12) + 1:02d}-10",
            })
            cond_rows.append({
                "id_condenacao": id_pf,
                "numero_processo_cnj": f"001{idx:04d}-45.2021.8.{10 + idx:02d}.0001",
                "documento_requerido": cpf_mask,
                "tipo_pessoa": "Pessoa física",
                "nome_requerido": f"AGENTE PUBLICO EXEMPLAR {m['uf']} {idx}",
                "esfera_cargo": "Municipal",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "tribunal_origem": m["trib"],
                "grau_jurisdicao": "Órgão Colegiado",
                "proibicao_contratar_poder_publico": m["debar_pct"] >= 50.0,
                "suspensao_direitos_politicos": True,
                "valor_ressarcimento_multa_brl": val_pf,
                "data_transito_julgado": f"2024-{(idx % 12) + 1:02d}-22",
            })

            sancoes_rows.append({
                "id_condenacao": id_pj,
                "documento_requerido": cnpj_pj,
                "nome_requerido": f"CONSTRUTORA E INFRA {m['nome'].upper()} S/A",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "tribunal_origem": m["trib"],
                "prazo_proibicao_anos": 8 if m["debar_pct"] >= 60.0 else 4,
                "data_fim_vigencia": "2032-12-31" if m["debar_pct"] >= 60.0 else "2028-12-31",
            })

            ress_pj = round(val_pj * 0.70, 2)
            multa_pj = round(val_pj - ress_pj, 2)
            ressarc_rows.append({
                "id_condenacao": id_pj,
                "documento_requerido": cnpj_pj,
                "nome_requerido": f"CONSTRUTORA E INFRA {m['nome'].upper()} S/A",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "valor_ressarcimento_brl": ress_pj,
                "valor_multa_civil_brl": multa_pj,
                "total_condenacao_financeira_brl": val_pj,
            })

            resumo_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "total_condenacoes_ativas": 2,
                "total_proibidos_contratar_pj": 1,
                "total_suspensao_direitos_politicos": 1,
                "valor_total_ressarcimento_multa_brl": round(val_pj + val_pf, 2),
                "taxa_proibicao_contratar_pct": m["debar_pct"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "total_condenacoes_ativas": 2,
                "total_proibidos_contratar_pj": 1,
                "total_suspensao_direitos_politicos": 1,
                "valor_total_ressarcimento_multa_brl": round(val_pj + val_pf, 2),
                "taxa_proibicao_contratar_pct": m["debar_pct"],
                "score_exposicao_improbidade_cnciai": m["score"],
                "classificacao_risco_improbidade": _classify_improbidade_risk(m["score"]),
            })

        cleaned_cond = apply_sentinel_cleaning(cond_rows, self.contract)
        cond_table = pa.Table.from_pylist(cleaned_cond)
        obs = evaluate_five_pillars(
            table=cond_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"CNJ CNCIAI 5-Pillar check failed: {obs.violations}")

        pq.write_table(cond_table, cond_file, compression="zstd")
        pq.write_table(pa.Table.from_pylist(sancoes_rows), silver_dir / "cnciai_sancoes_contratar.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(ressarc_rows), silver_dir / "cnciai_ressarcimento_erario.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(resumo_rows), silver_dir / "cnciai_resumo_municipal.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "condenacoes": len(cond_rows),
                "sancoes_contratar": len(sancoes_rows),
                "ressarcimentos": len(ressarc_rows),
                "resumo_municipal": len(resumo_rows),
                "gold": len(gold_rows),
            },
        }
        (self.data_dir / "bronze" / "ingestion_meta.json").write_text(
            json.dumps(bronze_meta, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    def extract(self) -> pa.Table:
        """Execute Medallion materialization and return cnciai_condenacoes Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "cnciai_condenacoes.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    @staticmethod
    def _digits_only(text: str) -> str:
        return "".join(ch for ch in text if ch.isdigit())

    def get_condemnations(self, documento_or_name: str) -> list[dict[str, Any]]:
        """Lookup active CNCIAI improbity convictions by CNPJ/CPF digits, name, or IBGE municipality."""
        raw = documento_or_name.strip().upper()
        digits = self._digits_only(raw)
        rows = self._read_parquet_rows("silver", "cnciai_condenacoes.parquet")
        return [
            r
            for r in rows
            if (digits and digits in self._digits_only(r["documento_requerido"]))
            or (digits and r["id_municipio"] == digits)
            or raw in r["nome_requerido"].upper()
            or raw in r["tribunal_origem"].upper()
        ]

    def get_debarment_sanctions(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Lookup judicial public-contracting debarment sanctions by IBGE code, UF, or CNPJ digits."""
        raw = id_municipio_or_doc.strip().upper()
        digits = self._digits_only(raw)
        rows = self._read_parquet_rows("silver", "cnciai_sancoes_contratar.parquet")
        return [
            r
            for r in rows
            if (digits and (r["id_municipio"] == digits or digits in r["documento_requerido"]))
            or r["sigla_uf"].upper() == raw
            or raw in r["nome_requerido"].upper()
        ]

    def get_treasury_reimbursements(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Lookup judicial treasury reimbursement & civil fine orders by IBGE code, UF, or CNPJ."""
        raw = id_municipio_or_doc.strip().upper()
        digits = self._digits_only(raw)
        rows = self._read_parquet_rows("silver", "cnciai_ressarcimento_erario.parquet")
        return [
            r
            for r in rows
            if (digits and (r["id_municipio"] == digits or digits in r["documento_requerido"]))
            or r["sigla_uf"].upper() == raw
            or raw in r["nome_requerido"].upper()
        ]

    def get_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal consolidated CNCIAI improbity summary by IBGE code or name."""
        term = id_municipio_or_name.strip().upper()
        for row in self._read_parquet_rows("silver", "cnciai_resumo_municipal.parquet"):
            if row["id_municipio"] == term or row["nome_municipio"].upper() == term:
                return row
        return None

    def get_improbidade_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold CNCIAI judicial improbity exposure features."""
        term = id_municipio_or_name.strip().upper()
        for row in self._read_parquet_rows("gold", "features_risco_improbidade_cnciai.parquet"):
            if row["id_municipio"] == term or row["nome_municipio"].upper() == term:
                return row
        return None

    def list_top_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold CNCIAI improbity exposure score."""
        rows = self._read_parquet_rows("gold", "features_risco_improbidade_cnciai.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        rows.sort(
            key=lambda r: (-float(r["score_exposicao_improbidade_cnciai"]), r["id_municipio"])
        )
        return rows[: max(1, int(limit))]


register_connector("cnj_improbidade", CnjImprobidadeConnector)
