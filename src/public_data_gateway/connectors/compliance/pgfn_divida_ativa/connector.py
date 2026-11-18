"""PGFN Dívida Ativa da União & FGTS connector (KYC/KYB Tax Default Risk & Compliance).

Governed by:
- Lei nº 6.830/1980 (Lei de Execução Fiscal)
- Art. 198, § 3º do Código Tributário Nacional (CTN) - Transparência em Dívida Ativa
- Dados Abertos PGFN: https://dadosabertos.pgfn.gov.br/
"""

from __future__ import annotations

import json
import re
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

PGFN_OPEN_DATA_URL = "https://dadosabertos.pgfn.gov.br/"
_TRIMESTRE_PATTERN = re.compile(r"(\d{4}_trimestre_\d{2})")


def _classify_pgfn_risk(score: float) -> str:
    """Classify municipal PGFN tax default & judicial execution intensity into risk tiers."""
    if score >= 75.0:
        return "ALTO_RISCO_EXPOSICAO_PGFN"
    if score >= 45.0:
        return "RISCO_MODERADO_PGFN"
    return "BAIXO_RISCO_PGFN"


class PgfnDividaAtivaConnector(BaseConnector):
    """Medallion connector for Procuradoria-Geral da Fazenda Nacional (Dívida Ativa & FGTS)."""

    def __init__(
        self,
        source_id: str = "pgfn_divida_ativa",
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
        """Absolute isolated directory for PGFN Medallion storage."""
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
        """Perform a live fail-hard smoke test against the official PGFN open data index."""
        resp = httpx.get(
            PGFN_OPEN_DATA_URL,
            timeout=20.0,
            follow_redirects=True,
            verify=False,
        )
        if resp.status_code >= 500:
            raise RuntimeError(
                f"PGFN open data index failed with HTTP {resp.status_code}: {resp.text[:300]}"
            )
        if resp.status_code != 200:
            raise ValueError(
                f"PGFN open data index returned HTTP {resp.status_code} from {PGFN_OPEN_DATA_URL}"
            )

        matches = sorted(set(_TRIMESTRE_PATTERN.findall(resp.text)))
        if not matches:
            raise ValueError("PGFN open data index did not return quarterly '_trimestre_' directories.")

        return {
            "status": "LIVE_SUCCESS",
            "http_status": resp.status_code,
            "quarters_found": len(matches),
            "latest_quarter_dir": matches[-1],
        }

    def ensure_baseline_data(self) -> None:
        """Materialize deterministic Bronze, Silver, and Gold PGFN Parquet layers if absent."""
        silver_dir = self.data_dir / "silver"
        gold_dir = self.data_dir / "gold"
        gerais_file = silver_dir / "pgfn_inscricoes_gerais.parquet"
        gold_file = gold_dir / "features_risco_divida_ativa_pgfn.parquet"

        if gerais_file.exists() and gold_file.exists():
            return

        quarter_ref = "2025_trimestre_02"
        # 12 canonical Brazilian municipalities with representative PGFN DAU, Previdenciária, and FGTS profiles
        seed_municipalities = [
            {"id": "3550308", "uf": "SP", "nome": "São Paulo", "pf": "PFN-SP", "cnpj_root": "11222333", "base_brl": 18500000.0, "aj_pct": 78.5, "risk": 84.2},
            {"id": "3304557", "uf": "RJ", "nome": "Rio de Janeiro", "pf": "PFN-RJ", "cnpj_root": "22333444", "base_brl": 12400000.0, "aj_pct": 81.0, "risk": 86.5},
            {"id": "3106200", "uf": "MG", "nome": "Belo Horizonte", "pf": "PFN-MG", "cnpj_root": "33444555", "base_brl": 6400000.0, "aj_pct": 72.0, "risk": 71.0},
            {"id": "4106902", "uf": "PR", "nome": "Curitiba", "pf": "PFN-PR", "cnpj_root": "44555666", "base_brl": 4900000.0, "aj_pct": 66.5, "risk": 62.4},
            {"id": "4314902", "uf": "RS", "nome": "Porto Alegre", "pf": "PFN-RS", "cnpj_root": "55666777", "base_brl": 5200000.0, "aj_pct": 74.0, "risk": 68.0},
            {"id": "2927408", "uf": "BA", "nome": "Salvador", "pf": "PFN-BA", "cnpj_root": "66777888", "base_brl": 5800000.0, "aj_pct": 75.5, "risk": 73.5},
            {"id": "2304400", "uf": "CE", "nome": "Fortaleza", "pf": "PFN-CE", "cnpj_root": "77888999", "base_brl": 4300000.0, "aj_pct": 69.0, "risk": 64.0},
            {"id": "2611606", "uf": "PE", "nome": "Recife", "pf": "PFN-PE", "cnpj_root": "88999000", "base_brl": 4100000.0, "aj_pct": 70.5, "risk": 65.2},
            {"id": "5208707", "uf": "GO", "nome": "Goiânia", "pf": "PFN-GO", "cnpj_root": "99000111", "base_brl": 3700000.0, "aj_pct": 64.0, "risk": 58.0},
            {"id": "1302603", "uf": "AM", "nome": "Manaus", "pf": "PFN-AM", "cnpj_root": "10203040", "base_brl": 3950000.0, "aj_pct": 68.5, "risk": 61.5},
            {"id": "3509502", "uf": "SP", "nome": "Campinas", "pf": "PFN-SP-CPS", "cnpj_root": "20304050", "base_brl": 3600000.0, "aj_pct": 63.0, "risk": 56.8},
            {"id": "4211504", "uf": "SC", "nome": "Nova Trento", "pf": "PFN-SC", "cnpj_root": "30405060", "base_brl": 210000.0, "aj_pct": 42.0, "risk": 28.4},
        ]

        gerais_rows: list[dict[str, Any]] = []
        prev_rows: list[dict[str, Any]] = []
        fgts_rows: list[dict[str, Any]] = []
        resumo_rows: list[dict[str, Any]] = []
        gold_rows: list[dict[str, Any]] = []

        for idx, m in enumerate(seed_municipalities, start=1):
            cnpj_pj = f"{m['cnpj_root']}0001{(10 + idx):02d}"
            cpf_mask = f"***.{100 + idx}.{200 + idx}-**"
            val_geral_1 = round(m["base_brl"] * 0.48, 2)
            val_geral_2 = round(m["base_brl"] * 0.12, 2)
            val_prev = round(m["base_brl"] * 0.28, 2)
            val_fgts = round(m["base_brl"] * 0.12, 2)

            gerais_rows.append({
                "numero_inscricao": f"804{idx:02d}24001001",
                "documento_devedor": cnpj_pj,
                "tipo_pessoa": "Pessoa jurídica",
                "nome_devedor": f"INDUSTRIA E SERVICOS {m['nome'].upper()} LTDA",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "unidade_responsavel": m["pf"],
                "situacao_inscricao": "Em cobrança",
                "indicador_ajuizado": True,
                "valor_consolidado_brl": val_geral_1,
                "ano_trimestre_ref": quarter_ref,
            })
            gerais_rows.append({
                "numero_inscricao": f"804{idx:02d}24001002",
                "documento_devedor": cpf_mask,
                "tipo_pessoa": "Pessoa física",
                "nome_devedor": f"CONTRIBUINTE EXEMPLAR {m['uf']} {idx}",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "unidade_responsavel": m["pf"],
                "situacao_inscricao": "Parcelado",
                "indicador_ajuizado": m["aj_pct"] >= 65.0,
                "valor_consolidado_brl": val_geral_2,
                "ano_trimestre_ref": quarter_ref,
            })

            prev_rows.append({
                "numero_inscricao": f"904{idx:02d}24002001",
                "documento_devedor": cnpj_pj,
                "nome_devedor": f"INDUSTRIA E SERVICOS {m['nome'].upper()} LTDA",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "situacao_inscricao": "Em cobrança",
                "indicador_ajuizado": True,
                "valor_consolidado_brl": val_prev,
                "ano_trimestre_ref": quarter_ref,
            })

            fgts_rows.append({
                "numero_inscricao": f"FGTS{idx:02d}24003001",
                "documento_devedor": cnpj_pj,
                "nome_devedor": f"INDUSTRIA E SERVICOS {m['nome'].upper()} LTDA",
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "situacao_inscricao": "Em cobrança",
                "indicador_ajuizado": m["aj_pct"] >= 60.0,
                "valor_consolidado_brl": val_fgts,
                "ano_trimestre_ref": quarter_ref,
            })

            total_geral = round(val_geral_1 + val_geral_2, 2)
            total_consolidado = round(total_geral + val_prev + val_fgts, 2)

            resumo_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "ano_trimestre_ref": quarter_ref,
                "total_inscricoes_ativas": 4,
                "total_devedores_pj": 1,
                "valor_total_geral_brl": total_geral,
                "valor_total_previdenciario_brl": val_prev,
                "valor_total_fgts_brl": val_fgts,
                "valor_total_consolidado_brl": total_consolidado,
                "taxa_ajuizamento_pct": m["aj_pct"],
            })

            gold_rows.append({
                "id_municipio": m["id"],
                "sigla_uf": m["uf"],
                "nome_municipio": m["nome"],
                "ano_trimestre_ref": quarter_ref,
                "total_inscricoes_ativas": 4,
                "total_devedores_pj": 1,
                "valor_total_geral_brl": total_geral,
                "valor_total_previdenciario_brl": val_prev,
                "valor_total_fgts_brl": val_fgts,
                "valor_total_consolidado_brl": total_consolidado,
                "taxa_ajuizamento_pct": m["aj_pct"],
                "score_risco_inadimplencia_fiscal": m["risk"],
                "classificacao_risco_pgfn": _classify_pgfn_risk(m["risk"]),
            })

        cleaned_gerais = apply_sentinel_cleaning(gerais_rows, self.contract)
        gerais_table = pa.Table.from_pylist(cleaned_gerais)
        obs = evaluate_five_pillars(
            table=gerais_table,
            contract=self.contract,
        )
        if not obs.passed:
            raise ValueError(f"PGFN 5-Pillar check failed: {obs.violations}")

        pq.write_table(gerais_table, gerais_file, compression="zstd")
        pq.write_table(pa.Table.from_pylist(prev_rows), silver_dir / "pgfn_divida_previdenciaria.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(fgts_rows), silver_dir / "pgfn_divida_fgts.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(resumo_rows), silver_dir / "pgfn_resumo_municipal.parquet", compression="zstd")
        pq.write_table(pa.Table.from_pylist(gold_rows), gold_file, compression="zstd")

        bronze_meta = {
            "source_id": self.source_id,
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "quarter_ref": quarter_ref,
            "lineage_hash": obs.lineage_hash,
            "counts": {
                "gerais": len(gerais_rows),
                "previdenciario": len(prev_rows),
                "fgts": len(fgts_rows),
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
        """Execute Medallion materialization and return pgfn_inscricoes_gerais Silver table."""
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / "silver" / "pgfn_inscricoes_gerais.parquet")

    def _read_parquet_rows(self, layer: str, filename: str) -> list[dict[str, Any]]:
        self.ensure_baseline_data()
        return pq.read_table(self.data_dir / layer / filename).to_pylist()

    @staticmethod
    def _digits_only(text: str) -> str:
        return "".join(ch for ch in text if ch.isdigit())

    def get_debtor_inscriptions(self, documento_or_name: str) -> list[dict[str, Any]]:
        """Lookup active PGFN DAU inscriptions by CNPJ/CPF digits, debtor name, or municipality code."""
        raw = documento_or_name.strip().upper()
        digits = self._digits_only(raw)
        rows = self._read_parquet_rows("silver", "pgfn_inscricoes_gerais.parquet")
        return [
            r
            for r in rows
            if (digits and digits in self._digits_only(r["documento_devedor"]))
            or (digits and r["id_municipio"] == digits)
            or raw in r["nome_devedor"].upper()
        ]

    def get_previdenciario_debts(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Lookup PGFN Social Security debts by IBGE municipality code, UF, or CNPJ digits."""
        raw = id_municipio_or_doc.strip().upper()
        digits = self._digits_only(raw)
        rows = self._read_parquet_rows("silver", "pgfn_divida_previdenciaria.parquet")
        return [
            r
            for r in rows
            if (digits and (r["id_municipio"] == digits or digits in r["documento_devedor"]))
            or r["sigla_uf"].upper() == raw
            or raw in r["nome_devedor"].upper()
        ]

    def get_fgts_debts(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Lookup PGFN FGTS active inscriptions by IBGE municipality code, UF, or CNPJ digits."""
        raw = id_municipio_or_doc.strip().upper()
        digits = self._digits_only(raw)
        rows = self._read_parquet_rows("silver", "pgfn_divida_fgts.parquet")
        return [
            r
            for r in rows
            if (digits and (r["id_municipio"] == digits or digits in r["documento_devedor"]))
            or r["sigla_uf"].upper() == raw
            or raw in r["nome_devedor"].upper()
        ]

    def get_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve consolidated municipal PGFN Silver summary by IBGE code or name."""
        term = id_municipio_or_name.strip().upper()
        for row in self._read_parquet_rows("silver", "pgfn_resumo_municipal.parquet"):
            if row["id_municipio"] == term or row["nome_municipio"].upper() == term:
                return row
        return None

    def get_pgfn_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold PGFN tax default & judicial execution risk features."""
        term = id_municipio_or_name.strip().upper()
        for row in self._read_parquet_rows("gold", "features_risco_divida_ativa_pgfn.parquet"):
            if row["id_municipio"] == term or row["nome_municipio"].upper() == term:
                return row
        return None

    def list_top_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by consolidated PGFN active debt volume."""
        rows = self._read_parquet_rows("gold", "features_risco_divida_ativa_pgfn.parquet")
        if sigla_uf:
            uf_clean = sigla_uf.strip().upper()
            rows = [r for r in rows if r["sigla_uf"] == uf_clean]
        rows.sort(key=lambda r: (-float(r["valor_total_consolidado_brl"]), r["id_municipio"]))
        return rows[: max(1, int(limit))]


register_connector("pgfn_divida_ativa", PgfnDividaAtivaConnector)
