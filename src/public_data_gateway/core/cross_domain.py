"""Cross-Domain Gold Analytics, Consolidated ML Feature Store & Solution Packs Insights."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

from public_data_gateway.core.config import GatewaySettings

VALID_SOLUTION_PACKS = frozenset({f"P{i:02d}" for i in range(1, 11)})


class CrossDomainService:
    """Queries materialized dbt Gold cross-domain marts and the consolidated ML Feature Store."""

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
                f"Cross-domain Gold warehouse not found at '{db_path}'. "
                "Run 'dbt build --project-dir dbt --profiles-dir dbt' first."
            )
        with duckdb.connect(str(db_path), read_only=True) as conn:
            cursor = conn.execute(sql, list(params))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    @staticmethod
    def _normalize_municipio(id_municipio: str) -> str:
        clean = "".join(ch for ch in str(id_municipio) if ch.isdigit())
        if len(clean) not in (6, 7):
            raise ValueError(
                f"Invalid IBGE municipality code '{id_municipio}'. Expected 6 or 7 digits."
            )
        return clean

    @staticmethod
    def _normalize_uf(sigla_uf: str) -> str:
        clean = str(sigla_uf).strip().upper()
        if len(clean) != 2 or not clean.isalpha():
            raise ValueError(f"Invalid UF code '{sigla_uf}'. Expected 2 letters.")
        return clean

    @staticmethod
    def _normalize_pack_id(pack_id: str) -> str:
        clean = str(pack_id).strip().upper()
        if clean not in VALID_SOLUTION_PACKS:
            raise ValueError(
                f"Invalid Solution Pack ID '{pack_id}'. Expected one of {sorted(VALID_SOLUTION_PACKS)}."
            )
        return clean

    def get_cross_domain_features(self, id_municipio: str) -> dict[str, Any]:
        """Return consolidated ML Feature Store row for a Brazilian municipality."""
        mun = self._normalize_municipio(id_municipio)
        if len(mun) == 7:
            rows = self._query_all(
                "SELECT * FROM feature_store_cross_domain_municipio WHERE id_municipio = ? LIMIT 1",
                (mun,),
            )
        else:
            rows = self._query_all(
                "SELECT * FROM feature_store_cross_domain_municipio WHERE LEFT(id_municipio, 6) = ? LIMIT 1",
                (mun,),
            )
        if not rows:
            raise KeyError(f"Municipality '{id_municipio}' not found in cross-domain Feature Store.")
        return rows[0]

    def list_top_cross_domain_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by composite B2B propensity score."""
        safe_limit = max(1, min(int(limit), 100))
        if sigla_uf:
            uf = self._normalize_uf(sigla_uf)
            return self._query_all(
                "SELECT * FROM feature_store_cross_domain_municipio "
                "WHERE sigla_uf = ? "
                "ORDER BY feat_score_propensao_b2b_composto DESC, id_municipio ASC "
                "LIMIT ?",
                (uf, safe_limit),
            )
        return self._query_all(
            "SELECT * FROM feature_store_cross_domain_municipio "
            "ORDER BY feat_score_propensao_b2b_composto DESC, id_municipio ASC "
            "LIMIT ?",
            (safe_limit,),
        )

    def get_solution_pack_insights(
        self, pack_id: str, sigla_uf: str | None = None, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Return state-level commercial and operational insights for a Solution Pack (P01..P10)."""
        pack = self._normalize_pack_id(pack_id)
        safe_limit = max(1, min(int(limit), 27))
        if sigla_uf:
            uf = self._normalize_uf(sigla_uf)
            return self._query_all(
                "SELECT * FROM mart_solution_packs_insights_brasil "
                "WHERE pack_id = ? AND sigla_uf = ? "
                "ORDER BY score_oportunidade_pack_uf DESC "
                "LIMIT ?",
                (pack, uf, safe_limit),
            )
        return self._query_all(
            "SELECT * FROM mart_solution_packs_insights_brasil "
            "WHERE pack_id = ? "
            "ORDER BY score_oportunidade_pack_uf DESC, sigla_uf ASC "
            "LIMIT ?",
            (pack, safe_limit),
        )

    def get_macro_territory_b2b(self, sigla_uf: str) -> dict[str, Any]:
        """Return cross-domain Territory x B2B x Macro x Public Procurement mart for a state."""
        uf = self._normalize_uf(sigla_uf)
        rows = self._query_all(
            "SELECT * FROM mart_cross_domain_territorio_b2b_macro_brasil WHERE sigla_uf = ? LIMIT 1",
            (uf,),
        )
        if not rows:
            raise KeyError(f"UF '{uf}' not found in cross-domain B2B/macro mart.")
        return rows[0]

    def get_esg_agro_infra(self, id_municipio: str) -> dict[str, Any]:
        """Return cross-domain ESG x Agro x Water Security x 5G Telecom mart for a municipality."""
        mun = self._normalize_municipio(id_municipio)
        if len(mun) == 7:
            rows = self._query_all(
                "SELECT * FROM mart_cross_domain_esg_agro_infra_brasil WHERE id_municipio = ? LIMIT 1",
                (mun,),
            )
        else:
            rows = self._query_all(
                "SELECT * FROM mart_cross_domain_esg_agro_infra_brasil WHERE LEFT(id_municipio, 6) = ? LIMIT 1",
                (mun,),
            )
        if not rows:
            raise KeyError(f"Municipality '{id_municipio}' not found in ESG/Agro/Infra mart.")
        return rows[0]
