"""High-level Python SDK facade for Brazil Data Gateway (C.O.R.E. Ecosystem)."""

from pathlib import Path
from typing import Any, Literal

import pyarrow as pa

from public_data_gateway.core.catalog import CatalogRegistry
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.models import EnrichmentReport, SourceEntry
from public_data_gateway.core.normalizer import CanonicalNormalizer, CheckDigitValidator
from public_data_gateway.core.storage import HybridStorageEngine
from public_data_gateway.enrichment.engine import EnrichmentEngine


class GeoFacade:
    """Geographic & territorial query interface powered by IBGE Localidades."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._connector: Any = None

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from public_data_gateway.connectors.demografico.ibge_localidades import (
                IBGELocalidadesConnector,
            )

            self._connector = IBGELocalidadesConnector(
                settings=self._sdk.settings,
                storage=self._sdk.storage,
            )
        return self._connector

    def municipios(
        self,
        uf: str | None = None,
        regiao: str | None = None,
        capitais_only: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List Brazilian municipalities with optional filters."""
        return self.connector.list_municipios(
            uf=uf, regiao=regiao, capitais_only=capitais_only, limit=limit, offset=offset
        )

    def municipio(self, id_or_name: str | int, uf: str | None = None) -> dict[str, Any] | None:
        """Get detailed municipality information by 6D/7D IBGE code or name."""
        return self.connector.get_municipio(str(id_or_name), uf=uf)

    def ufs(self) -> list[dict[str, Any]]:
        """List all 27 Brazilian Federative Units (UFs) with regions."""
        return self.connector.list_ufs()

    def sync_localidades(self) -> pa.Table:
        """Trigger Bronze-to-Gold extraction and materialization pipeline."""
        return self.connector.extract()


class ProcurementFacade:
    """Public procurement query interface powered by PNCP (Lei nº 14.133/2021)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._connector: Any = None

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
                PNCPContratacoesConnector,
            )

            self._connector = PNCPContratacoesConnector(settings=self._sdk.settings)
        return self._connector

    def contratacoes(
        self,
        uf: str | None = None,
        modalidade_id: int | None = None,
        ano: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List procurement processes with optional filters."""
        return self.connector.list_contratacoes(
            uf=uf, modalidade_id=modalidade_id, ano=ano, limit=limit
        )

    def contratos(
        self,
        cnpj_orgao: str | None = None,
        ni_fornecedor: str | None = None,
        ano: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List administrative contracts with optional filters."""
        return self.connector.list_contratos(
            cnpj_orgao=cnpj_orgao, ni_fornecedor=ni_fornecedor, ano=ano, limit=limit
        )


class ComplianceFacade:
    """Compliance, administrative sanctions and anti-corruption query interface (CGU CEIS/CNEP/CEPIM)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._connector: Any = None

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from public_data_gateway.connectors.compliance.cgu_ceis_cnep.connector import (
                CGUCEISCNEPConnector,
            )

            self._connector = CGUCEISCNEPConnector(settings=self._sdk.settings)
        return self._connector

    def status(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Check whether a CNPJ or CPF has active sanctions in CEIS, CNEP, or CEPIM."""
        return self.connector.get_compliance_status(cnpj_or_cpf)

    def sancoes(
        self,
        cadastro: str | None = None,
        uf: str | None = None,
        ativas_only: bool = True,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """List administrative sanctions with optional filters."""
        if cadastro == "ceis":
            records = self.connector.extract_table("cgu_ceis")
        elif cadastro == "cnep":
            records = self.connector.extract_table("cgu_cnep")
        elif cadastro == "cepim":
            records = self.connector.extract_table("cgu_cepim")
        else:
            records = (
                self.connector.extract_table("cgu_ceis")
                + self.connector.extract_table("cgu_cnep")
                + self.connector.extract_table("cgu_cepim")
            )

        filtered = []
        for r in records:
            if ativas_only and not r.get("is_sancao_ativa", r.get("is_ativo", False)):
                continue
            if uf and r.get("sigla_uf_orgao") != uf.upper():
                continue
            filtered.append(r)
            if limit and len(filtered) >= limit:
                break
        return filtered

    def pep_status(self, cpf_or_name: str) -> dict[str, Any]:
        """Check whether a person (CPF or Name) is registered as Politically Exposed Person (PEP)."""
        from public_data_gateway.connectors.compliance.cgu_pep.connector import (
            CGUPEPConnector,
        )

        conn = CGUPEPConnector(settings=self._sdk.settings)
        return conn.get_pep_status(cpf_or_name)

    def list_pep(
        self,
        uf: str | None = None,
        sigla_funcao: str | None = None,
        ativos_only: bool = True,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List Politically Exposed Persons with optional filters."""
        from public_data_gateway.connectors.compliance.cgu_pep.connector import (
            CGUPEPConnector,
        )

        conn = CGUPEPConnector(settings=self._sdk.settings)
        return conn.list_pep(uf=uf, sigla_funcao=sigla_funcao, ativos_only=ativos_only, limit=limit)


class JudicialFacade:
    """Judicial proceedings, procedural metadata and litigation risk interface (CNJ DataJud)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._connector: Any = None

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from public_data_gateway.connectors.judicial.cnj_datajud.connector import (
                CNJDataJudConnector,
            )

            self._connector = CNJDataJudConnector(settings=self._sdk.settings)
        return self._connector

    def litigation_risk(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Assess corporate or individual litigation risk profile across all courts."""
        return self.connector.get_litigation_risk(cnpj_or_cpf)

    def search_processos(
        self,
        termo: str,
        tribunal: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Search judicial processes by process number (NUP), party name, or court."""
        return self.connector.search_processos(termo=termo, tribunal=tribunal, limit=limit)


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
        self.geo = GeoFacade(self)
        self.procurement = ProcurementFacade(self)
        self.compliance = ComplianceFacade(self)
        self.judicial = JudicialFacade(self)

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

    def get_simples_status(self, cnpj: str) -> dict[str, Any]:
        """Resolve Simples Nacional and MEI tax status for a CNPJ."""
        from public_data_gateway.connectors.empresarial.rfb_simples_mei import (
            RFBSimplesMEIConnector,
        )

        conn = RFBSimplesMEIConnector(settings=self.settings)
        return conn.get_simples_mei_status(cnpj)

    def get_contratacoes_pncp(
        self,
        uf: str | None = None,
        modalidade_id: int | None = None,
        ano: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query public procurements published in PNCP."""
        return self.procurement.contratacoes(
            uf=uf, modalidade_id=modalidade_id, ano=ano, limit=limit
        )

    def get_contratos_pncp(
        self,
        cnpj_orgao: str | None = None,
        ni_fornecedor: str | None = None,
        ano: int | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Query administrative contracts registered in PNCP."""
        return self.procurement.contratos(
            cnpj_orgao=cnpj_orgao, ni_fornecedor=ni_fornecedor, ano=ano, limit=limit
        )

    def get_compliance_status(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Check whether a CNPJ or CPF has active administrative sanctions in CEIS, CNEP, or CEPIM."""
        return self.compliance.status(cnpj_or_cpf)

    def get_pep_status(self, cpf_or_name: str) -> dict[str, Any]:
        """Check whether a person (CPF or Name) is registered as Politically Exposed Person (PEP)."""
        return self.compliance.pep_status(cpf_or_name)

    def get_litigation_risk(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Assess corporate or individual litigation risk profile across all courts (CNJ DataJud)."""
        return self.judicial.litigation_risk(cnpj_or_cpf)

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


PublicData = BrazilData
