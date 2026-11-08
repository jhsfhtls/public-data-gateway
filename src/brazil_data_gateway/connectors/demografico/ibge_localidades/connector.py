"""IBGE Localidades & Malha Territorial Connector.

Ingests, normalizes, validates, and materializes Brazilian municipalities and UFs
across Bronze, Silver, and Gold layers per POP_T_177, POP_T_070, and POP_T_074.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.connector import BaseConnector, register_connector
from brazil_data_gateway.core.contracts import apply_sentinel_cleaning, load_contract
from brazil_data_gateway.core.http_client import ResilientHttpClient
from brazil_data_gateway.core.normalizer import CanonicalNormalizer
from brazil_data_gateway.core.storage import HybridStorageEngine

logger = logging.getLogger(__name__)

IBGE_MUNICIPIOS_API = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
IBGE_ESTADOS_API = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"

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
    """Production connector for IBGE Municipalities, UFs, and Territorial Hierarchies."""

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
                    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/13"
                )
                return resp.status_code == 200 and resp.json().get("sigla") == "AM"
        except Exception as exc:
            logger.warning("IBGE connectivity test failed: %s", exc)
            return False

    def _load_offline_reference(self) -> list[dict[str, Any]]:
        """Load deterministic reference municipalities seed."""
        ref_file = self.settings.repo_root / "data" / "reference" / "municipios_brasil.json"
        if not ref_file.exists():
            ref_file = (
                self.settings.repo_root.parent
                / "core"
                / "data"
                / "referencia"
                / "municipios_brasil.json"
            )

        if not ref_file.exists():
            raise FileNotFoundError(f"Reference municipalities seed not found at {ref_file}")

        with open(ref_file, encoding="utf-8") as f:
            ref_data = json.load(f)

        self._persist_bronze_snapshot(ref_data)
        return ref_data

    def fetch_raw_bronze(
        self, offline_fallback: bool = True, force_offline: bool = False
    ) -> list[dict[str, Any]]:
        """Fetch raw JSON list of all municipalities from IBGE API or reference fallback."""
        if force_offline or os.environ.get("BDG_OFFLINE_MODE") == "1":
            return self._load_offline_reference()

        try:
            with self.http_client as client:
                resp = client.get(IBGE_MUNICIPIOS_API, cache_ttl=self.settings.cache_ttl_seconds)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list) and len(data) >= 5000:
                        self._persist_bronze_snapshot(data)
                        return data
        except Exception as exc:
            logger.warning("Remote IBGE fetch failed (%s). Triggering offline fallback.", exc)

        if not offline_fallback:
            raise RuntimeError("Failed to fetch IBGE municipalities and fallback is disabled.")

        return self._load_offline_reference()

    def _persist_bronze_snapshot(self, data: list[dict[str, Any]]) -> Path:
        """Save raw JSON payload in data/bronze with manifest."""
        bronze_dir = self.settings.data_dir / "bronze" / self.source_id
        bronze_dir.mkdir(parents=True, exist_ok=True)
        raw_file = bronze_dir / "raw_municipios.json"
        with open(raw_file, "w", encoding="utf-8", newline="\n") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return raw_file

    def transform_silver_rows(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize raw items into strict canonical Silver structure."""
        extracted_at = datetime.now(UTC).isoformat()
        reference_date = "2024-01-01"
        silver_rows: list[dict[str, Any]] = []

        for item in raw_items:
            # Handle both live IBGE API format and sanitized reference JSON format
            raw_id = str(item.get("id") or item.get("id_municipio") or "").strip()
            if not raw_id:
                continue

            id_7 = raw_id.zfill(7)
            id_6 = item.get("id_municipio_6") or id_7[:6]
            nome = str(item.get("nome") or "").strip()

            # Resolve UF & Região from nested structures or reference fields
            sigla_uf = ""
            id_uf = ""
            nome_uf = ""
            regiao = ""
            sigla_regiao = ""
            meso_nome = None
            micro_nome = None
            reg_inter_nome = None
            reg_imed_nome = None

            # 1. Check nested microrregiao / mesorregiao (classic IBGE structure)
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

            # 2. Check regiao-imediata / regiao-intermediaria (recent emancipations e.g. Boa Esperança do Norte)
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

            # 3. Fallback to flat reference attributes
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

            # 4. Ultimate prefix-based fallback if UF is still missing
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

        cleaned_rows = apply_sentinel_cleaning(silver_rows, self.contract)
        return cleaned_rows

    def extract(self) -> pa.Table:
        """Full pipeline: Bronze fetch -> Silver normalizer/observability -> Gold features."""
        raw_items = self.fetch_raw_bronze()
        silver_rows = self.transform_silver_rows(raw_items)

        # Build PyArrow Table for Silver
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
        schema = pa.schema(fields)

        pydict: dict[str, list[Any]] = {f.name: [] for f in fields}
        for r in silver_rows:
            for f in fields:
                pydict[f.name].append(r.get(f.name))

        silver_table = pa.Table.from_pydict(pydict, schema=schema)

        # Materialize Silver with 5-Pillar evaluation (auto quarantine if invalid)
        _, _, obs_report = self.storage.write_medallion_table(
            silver_table,
            source_id=self.source_id,
            layer="silver",
            contract=self.contract,
        )

        if obs_report and not obs_report.passed:
            raise RuntimeError(
                f"IBGE Localidades Silver observability checks failed: {obs_report.violations}"
            )

        # Build and materialize Gold ML Feature Views
        self._build_and_write_gold_features(silver_table)
        return silver_table

    def _build_and_write_gold_features(self, silver_table: pa.Table) -> pa.Table:
        """Construct ML feature view for territorial entities."""
        # Regional counts
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

        self.storage.write_medallion_table(
            gold_table,
            source_id=self.source_id,
            layer="gold",
            contract=None,
        )
        return gold_table

    # Query methods for SDK, API, CLI, and MCP
    def get_municipio(self, id_or_name: str, uf: str | None = None) -> dict[str, Any] | None:
        """Resolve a single municipality by IBGE code (7D/6D) or name with optional UF."""
        id_7 = self.normalizer.to_ibge_7(id_or_name, uf=uf)
        if not id_7:
            return None

        # Try reading from silver Parquet first
        try:
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

    def list_ufs(self) -> list[dict[str, Any]]:
        """List the 27 Brazilian Federative Units."""
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


# Register connector globally
register_connector("ibge_localidades", IBGELocalidadesConnector)
