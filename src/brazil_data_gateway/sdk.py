"""High-level Python SDK facade for Brazil Data Gateway (C.O.R.E. Ecosystem)."""

from pathlib import Path
from typing import Any, Literal

import pyarrow as pa

from brazil_data_gateway.core.catalog import CatalogRegistry
from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.models import EnrichmentReport, SourceEntry
from brazil_data_gateway.core.normalizer import CanonicalNormalizer, CheckDigitValidator
from brazil_data_gateway.core.storage import HybridStorageEngine
from brazil_data_gateway.enrichment.engine import EnrichmentEngine


class BrazilData:
    """Unified developer SDK facade for querying, normalizing, and enriching Brazilian public datasets."""

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()
        self.catalog = CatalogRegistry(self.settings)
        self.storage = HybridStorageEngine(self.settings)
        self.normalizer = CanonicalNormalizer(self.settings)
        self.check_digits = CheckDigitValidator()
        self.enricher = EnrichmentEngine(
            settings=self.settings,
            storage_engine=self.storage,
            normalizer=self.normalizer,
        )

    def list_sources(
        self,
        wave: int | None = None,
        segment_id: str | None = None,
        status: str | None = None,
        product_id: str | None = None,
    ) -> list[SourceEntry]:
        """List catalog sources filtered by wave, segment, status, or product."""
        return self.catalog.list_sources(
            wave=wave, segment_id=segment_id, status=status, product_id=product_id
        )

    def get_source(self, source_id: str) -> SourceEntry | None:
        """Get source metadata by identifier."""
        return self.catalog.get_source(source_id)

    def read_table(self, source_id: str, layer: str = "silver") -> pa.Table:
        """Read a dataset from a specific Medallion layer."""
        return self.storage.read_table(source_id=source_id, layer=layer)

    def query(self, sql: str, params: list[Any] | None = None) -> pa.Table:
        """Execute read-only SQL via DuckDB engine."""
        return self.storage.query_sql(sql=sql, params=params)

    def resolve_municipality(
        self, code_or_name: str | int, uf: str | None = None
    ) -> dict[str, Any] | None:
        """Resolve a municipality code or name into official IBGE record."""
        return self.normalizer.get_municipio_record(code_or_name, uf=uf)

    def enrich(
        self,
        entity_type: Literal["municipio", "cnpj", "cep"],
        key_column: str,
        input_path: Path | str | None = None,
        input_table: pa.Table | None = None,
        uf_column: str | None = None,
        output_path: Path | str | None = None,
    ) -> tuple[pa.Table, EnrichmentReport]:
        """Enrich a dataset with official identifiers, enforcing POP_T_154 shield."""
        return self.enricher.enrich(
            entity_type=entity_type,
            key_column=key_column,
            input_path=input_path,
            input_table=input_table,
            uf_column=uf_column,
            output_path=output_path,
        )
