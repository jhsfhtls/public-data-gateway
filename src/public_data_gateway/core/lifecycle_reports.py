"""Commercial Lifecycle Reports (S01..S09) & Sales Dossier Generator for Solution Packs (P01..P10)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.cross_domain import VALID_SOLUTION_PACKS

STAGE_TABLES: dict[str, str] = {
    "S01": "mart_lifecycle_s01_gtm_icp_ranking",
    "S02": "mart_lifecycle_s02_discovery_dores_roi",
    "S03": "mart_lifecycle_s03_pricing_benchmarks_tam",
    "S04": "mart_lifecycle_s04_legal_compliance_dd",
    "S05": "mart_lifecycle_s05_onboarding_quickwins",
    "S06": "mart_lifecycle_s06_delivery_feature_blueprint",
    "S07": "mart_lifecycle_s07_uat_quality_scorecard",
    "S08": "mart_lifecycle_s08_sre_freshness_slos",
    "S09": "mart_lifecycle_s09_cs_upsell_matrix",
}

UF_SCOPED_STAGES = frozenset({"S01", "S02", "S03", "S04", "S09"})


class LifecycleReportsService:
    """Queries materialized dbt Gold lifecycle marts (S01..S09) and builds executive sales dossiers."""

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()

    def _resolve_duckdb_path(self) -> Path:
        env_path = os.environ.get("PDG_DUCKDB_PATH") or os.environ.get("BDG_DUCKDB_PATH")
        if env_path:
            return Path(env_path).resolve()
        return (self.settings.data_dir / "brasil.duckdb").resolve()

    def _query_all(
        self, sql: str, params: list[Any] | tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        db_path = self._resolve_duckdb_path()
        if not db_path.is_file():
            raise FileNotFoundError(
                f"Gold warehouse not found at '{db_path}'. "
                "Run 'dbt build --project-dir dbt --profiles-dir dbt' first."
            )
        with duckdb.connect(str(db_path), read_only=True) as conn:
            cursor = conn.execute(sql, list(params))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    @staticmethod
    def _normalize_stage(stage_id: str) -> str:
        clean = str(stage_id).strip().upper()
        if clean not in STAGE_TABLES:
            raise ValueError(
                f"Invalid lifecycle stage '{stage_id}'. Expected one of {sorted(STAGE_TABLES)}."
            )
        return clean

    @staticmethod
    def _normalize_pack(pack_id: str) -> str:
        clean = str(pack_id).strip().upper()
        if clean not in VALID_SOLUTION_PACKS:
            raise ValueError(
                f"Invalid Solution Pack '{pack_id}'. Expected one of {sorted(VALID_SOLUTION_PACKS)}."
            )
        return clean

    @staticmethod
    def _normalize_uf(sigla_uf: str) -> str:
        clean = str(sigla_uf).strip().upper()
        if len(clean) != 2 or not clean.isalpha():
            raise ValueError(f"Invalid UF code '{sigla_uf}'. Expected 2 letters.")
        return clean

    def get_stage_report(
        self,
        stage_id: str,
        pack_id: str,
        sigla_uf: str | None = None,
        id_municipio: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Query the Gold mart for a specific lifecycle stage (S01..S09) and Solution Pack (P01..P10)."""
        stage = self._normalize_stage(stage_id)
        pack = self._normalize_pack(pack_id)
        table = STAGE_TABLES[stage]
        safe_limit = max(1, min(int(limit), 100))

        clauses = ["pack_id = ?"]
        params: list[Any] = [pack]

        if sigla_uf and stage in UF_SCOPED_STAGES:
            clauses.append("sigla_uf = ?")
            params.append(self._normalize_uf(sigla_uf))

        if id_municipio and stage == "S01":
            clean_mun = "".join(ch for ch in str(id_municipio) if ch.isdigit())
            if len(clean_mun) not in (6, 7):
                raise ValueError(f"Invalid IBGE municipality code '{id_municipio}'.")
            if len(clean_mun) == 7:
                clauses.append("id_municipio = ?")
                params.append(clean_mun)
            else:
                clauses.append("LEFT(id_municipio, 6) = ?")
                params.append(clean_mun)

        where_sql = " AND ".join(clauses)
        params.append(safe_limit)
        return self._query_all(
            f"SELECT * FROM {table} WHERE {where_sql} LIMIT ?",
            params,
        )

    def build_sales_dossier(
        self,
        pack_id: str,
        sigla_uf: str,
        id_municipio: str | None = None,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Build a complete 9-stage commercial & delivery dossier for a Solution Pack in a UF."""
        pack = self._normalize_pack(pack_id)
        uf = self._normalize_uf(sigla_uf)

        stages: dict[str, list[dict[str, Any]]] = {}
        for stage_id in sorted(STAGE_TABLES):
            stages[stage_id] = self.get_stage_report(
                stage_id=stage_id,
                pack_id=pack,
                sigla_uf=uf,
                id_municipio=id_municipio if stage_id == "S01" else None,
                limit=limit if stage_id == "S01" else 1,
            )

        return {
            "pack_id": pack,
            "sigla_uf": uf,
            "id_municipio": id_municipio,
            "stages": stages,
        }

    def format_dossier_markdown(self, dossier: dict[str, Any]) -> str:
        """Render a structured Markdown executive brief from a sales dossier dictionary."""
        pack = dossier["pack_id"]
        uf = dossier["sigla_uf"]
        stages = dossier["stages"]
        lines = [
            f"# Dossie Comercial & Entrega — {pack} ({uf})",
            "",
            "| Estagio | Resumo Executivo |",
            "| :--- | :--- |",
        ]
        for stage_id in sorted(stages):
            rows = stages[stage_id]
            if not rows:
                lines.append(f"| **{stage_id}** | Sem registros para os filtros informados |")
                continue
            r = rows[0]
            if stage_id == "S01":
                summary = f"Top ICP: {r.get('nome_municipio')} ({r.get('score_icp_pack_municipio')}) — {r.get('tier_icp_abm')}"
            elif stage_id == "S02":
                summary = f"Dor: {r.get('dor_latente_principal')} | Impacto est.: R$ {r.get('impacto_financeiro_estimado_brl', 0):,.2f}"
            elif stage_id == "S03":
                summary = f"TAM: {r.get('tam_empresas_ativas_uf')} empresas | ROI est.: {r.get('ancora_roi_estimado_pct')}% ({r.get('faixa_payback_meses')})"
            elif stage_id == "S04":
                summary = f"DD: {r.get('nivel_rigor_due_diligence')} | Sancoes UF: {r.get('total_sancoes_ativas_uf')}"
            elif stage_id == "S05":
                summary = f"Quick-Win 14d: {r.get('quick_win_14_dias')} ({r.get('tempo_estimado_ativacao_horas')}h)"
            elif stage_id == "S06":
                summary = f"Blueprint: {r.get('blueprint_arquitetura_entrega')} ({r.get('sprints_estimadas_delivery')} sprints)"
            elif stage_id == "S07":
                summary = f"UAT DoD: {r.get('criterio_aceite_uat_dod')}"
            elif stage_id == "S08":
                summary = f"SRE SLO: {r.get('slo_disponibilidade_pct')}% | Freshness <= {r.get('freshness_sla_horas')}h"
            else:
                summary = f"Expansao ARR: {r.get('next_recommended_pack_id')} — {r.get('tese_expansao_arr')}"
            lines.append(f"| **{stage_id}** | {summary} |")
        return "\n".join(lines)
