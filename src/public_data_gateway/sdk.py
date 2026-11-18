"""High-level Python SDK facade for Public Data Gateway."""

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

    @property
    def cnefe_connector(self) -> Any:
        if not hasattr(self, "_cnefe_conn") or self._cnefe_conn is None:
            from public_data_gateway.connectors.geografico.ibge_cnefe import (
                IBGECnefeConnector,
            )

            self._cnefe_conn = IBGECnefeConnector(settings=self._sdk.settings)
        return self._cnefe_conn

    def cnefe_municipality_summary(self, id_municipio: str) -> dict[str, Any] | None:
        """Get CNEFE address and building density summary for a municipality."""
        return self.cnefe_connector.get_municipality_summary(id_municipio)

    def cnefe_cep_profile(self, cep: str) -> dict[str, Any] | None:
        """Get CNEFE address, centroid and building count by 8-digit CEP."""
        return self.cnefe_connector.get_cep_profile(cep)

    def cnefe_building_types(self, id_municipio: str) -> list[dict[str, Any]]:
        """List building species distribution in a municipality."""
        return self.cnefe_connector.list_building_types(id_municipio)

    def cnefe_top_address_density(self, limit: int = 10) -> list[dict[str, Any]]:
        """List municipalities ranked by address density per km2."""
        return self.cnefe_connector.list_top_address_density(limit=limit)

    @property
    def malhas_connector(self) -> Any:
        if not hasattr(self, "_malhas_conn") or self._malhas_conn is None:
            from public_data_gateway.connectors.geografico.ibge_malhas import (
                IBGEMalhasConnector,
            )

            self._malhas_conn = IBGEMalhasConnector(settings=self._sdk.settings)
        return self._malhas_conn

    def get_municipio_boundary(
        self, id_municipio: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Get official vector territorial boundary GeoJSON and metrics for a municipality."""
        return self.malhas_connector.get_municipio_boundary(id_municipio, qualidade=qualidade)

    def get_uf_boundary(
        self, sigla_uf: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Get official vector territorial boundary GeoJSON and metrics for a Federative Unit."""
        return self.malhas_connector.get_uf_boundary(sigla_uf, qualidade=qualidade)

    def get_territorial_features(self, id_municipio: str) -> dict[str, Any]:
        """Get spatial morphology features and compactness scores from the Gold Feature Store."""
        return self.malhas_connector.get_territorial_features(id_municipio)

    def list_top_compact_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by territorial compactness score."""
        return self.malhas_connector.list_top_compact_municipalities(limit=limit, sigla_uf=sigla_uf)

    @property
    def anatel_connector(self) -> Any:
        if not hasattr(self, "_anatel_conn") or self._anatel_conn is None:
            from public_data_gateway.connectors.geografico.anatel_mosaico import (
                AnatelMosaicoConnector,
            )

            self._anatel_conn = AnatelMosaicoConnector(settings=self._sdk.settings)
        return self._anatel_conn

    def get_telecom_stations(self, id_municipio: str) -> dict[str, Any]:
        """Get licensed SMP telecom towers and 4G/5G coverage summary (ANATEL Mosaico)."""
        return self.anatel_connector.get_station_coverage_summary(id_municipio)

    def get_broadband_profile(self, id_municipio: str) -> dict[str, Any]:
        """Get fixed broadband (SCM) and fiber optic (FTTH/backhaul) profile (ANATEL Mosaico)."""
        return self.anatel_connector.get_broadband_scm_profile(id_municipio)

    def list_spectrum_allocations(
        self, id_municipio: str, faixa_frequencia_mhz: str | None = None
    ) -> list[dict[str, Any]]:
        """List radiofrequency spectrum allocations by band for a municipality."""
        return self.anatel_connector.list_spectrum_allocations(
            id_municipio, faixa_frequencia_mhz=faixa_frequencia_mhz
        )

    def get_telecom_features(self, id_municipio: str) -> dict[str, Any]:
        """Get digital connectivity and 5G maturity features from the Gold Feature Store."""
        return self.anatel_connector.get_telecom_features(id_municipio)

    def list_top_connected_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by composite digital connectivity score."""
        return self.anatel_connector.list_top_connected_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def antt_connector(self) -> Any:
        if not hasattr(self, "_antt_conn") or self._antt_conn is None:
            from public_data_gateway.connectors.geografico.antt_malha_transporte import (
                AnttMalhaTransporteConnector,
            )

            self._antt_conn = AnttMalhaTransporteConnector(settings=self._sdk.settings)
        return self._antt_conn

    def get_highway_corridors(self, id_municipio: str) -> dict[str, Any]:
        """Get federal highway corridors, concessions, tolls and heavy VMD (ANTT)."""
        return self.antt_connector.get_highway_corridors(id_municipio)

    def get_rntrc_fleet_profile(self, id_municipio: str) -> dict[str, Any]:
        """Get RNTRC freight carrier registry (ETC/TAC/CTC) and fleet capacity (ANTT)."""
        return self.antt_connector.get_rntrc_fleet_profile(id_municipio)

    def get_rail_intermodal_terminals(self, id_municipio: str) -> dict[str, Any]:
        """Get rail connectivity, intermodal cargo terminals and rail volumes (ANTT)."""
        return self.antt_connector.get_rail_intermodal_terminals(id_municipio)

    def list_modal_flows(
        self, id_municipio: str, modal_transporte: str | None = None
    ) -> list[dict[str, Any]]:
        """List freight modal split (road/rail/waterway), TKU cost and reliability (ANTT)."""
        return self.antt_connector.list_modal_flows(
            id_municipio, modal_transporte=modal_transporte
        )

    def get_logistics_features(self, id_municipio: str) -> dict[str, Any]:
        """Get logistics infrastructure and intermodality features from the Gold Feature Store."""
        return self.antt_connector.get_logistics_features(id_municipio)

    def list_top_logistics_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by composite ANTT logistics infrastructure score."""
        return self.antt_connector.list_top_logistics_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def senatran_frota_connector(self) -> Any:
        if (
            not hasattr(self, "_senatran_frota_connector")
            or self._senatran_frota_connector is None
        ):
            from public_data_gateway.connectors.geografico.senatran_frota import (
                SenatranFrotaConnector,
            )

            self._senatran_frota_connector = SenatranFrotaConnector(
                settings=self._sdk.settings
            )
        return self._senatran_frota_connector

    def get_fleet_by_type(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SENATRAN RENAVAM active vehicle fleet by category, code, IBGE, UF, or city."""
        return self.senatran_frota_connector.get_fleet_by_type(termo)

    def get_fleet_propulsion_ev(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SENATRAN municipal fleet propulsion energy matrix and EV/HEV share."""
        return self.senatran_frota_connector.get_fleet_propulsion_ev(termo)

    def get_fleet_age_renewal(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal fleet average age, motorization per 100 hab, and new licenses."""
        return self.senatran_frota_connector.get_fleet_age_renewal(id_municipio_or_name)

    def get_fleet_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal RENAVAM total fleet, trucks, and EV count."""
        return self.senatran_frota_connector.get_fleet_municipal_summary(
            id_municipio_or_name
        )

    def get_fleet_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold SENATRAN vehicle fleet modernization and EV readiness features."""
        return self.senatran_frota_connector.get_fleet_features(id_municipio_or_name)

    def list_top_fleet_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_ev: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by fleet modernization score or EV/HEV volume."""
        return self.senatran_frota_connector.list_top_fleet_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_ev=sort_by_ev
        )

    @property
    def prf_acidentes_connector(self) -> Any:
        if (
            not hasattr(self, "_prf_acidentes_connector")
            or self._prf_acidentes_connector is None
        ):
            from public_data_gateway.connectors.geografico.prf_acidentes import (
                PrfAccidentsConnector,
            )

            self._prf_acidentes_connector = PrfAccidentsConnector(
                settings=self._sdk.settings
            )
        return self._prf_acidentes_connector

    def get_prf_accidents(self, termo: str) -> list[dict[str, Any]]:
        """Lookup PRF highway accident records by code, BR highway, cause, IBGE, UF, or city."""
        return self.prf_acidentes_connector.get_prf_accidents(termo)

    def get_prf_corridors(self, termo: str) -> list[dict[str, Any]]:
        """Lookup PRF logistics corridor accident & severity metrics by code, BR, IBGE, UF, or city."""
        return self.prf_acidentes_connector.get_prf_corridors(termo)

    def get_prf_causes_profile(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal PRF collision causes, heavy truck share, and DUI enforcement."""
        return self.prf_acidentes_connector.get_prf_causes_profile(
            id_municipio_or_name
        )

    def get_prf_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal PRF annual crashes, injuries, and lethality."""
        return self.prf_acidentes_connector.get_prf_municipal_summary(
            id_municipio_or_name
        )

    def get_prf_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold PRF road safety score and corridor risk tier."""
        return self.prf_acidentes_connector.get_prf_features(id_municipio_or_name)

    def list_top_safe_prf_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_fatalities: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by PRF road safety score or highest highway fatalities."""
        return self.prf_acidentes_connector.list_top_safe_prf_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_fatalities=sort_by_fatalities
        )


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

    @property
    def catmat_connector(self) -> Any:
        if not hasattr(self, "_catmat_conn") or self._catmat_conn is None:
            from public_data_gateway.connectors.compras_publicas.comprasgov_catmat import (
                ComprasGovCatmatConnector,
            )

            self._catmat_conn = ComprasGovCatmatConnector(settings=self._sdk.settings)
        return self._catmat_conn

    def search_materials(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Search CATMAT materials by description or PDM name."""
        return self.catmat_connector.search_materials(query=query, limit=limit)

    def search_services(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Search CATSER services by description or service name."""
        return self.catmat_connector.search_services(query=query, limit=limit)

    def get_material(self, codigo_item: str) -> dict[str, Any] | None:
        """Lookup CATMAT material details by its unique item code."""
        return self.catmat_connector.get_item_detail(codigo_item)

    def get_price_benchmark(self, codigo_pdm_or_item: str) -> dict[str, Any] | None:
        """Get official price benchmark, dispersion, and overpricing risk rating."""
        return self.catmat_connector.get_price_benchmark(codigo_pdm_or_item)

    def get_supplier_history(self, cnpj: str) -> dict[str, Any] | None:
        """Lookup government procurement history by supplier CNPJ."""
        return self.catmat_connector.get_supplier_procurement_history(cnpj)

    @property
    def querido_diario_connector(self) -> Any:
        if (
            not hasattr(self, "_querido_diario_connector")
            or self._querido_diario_connector is None
        ):
            from public_data_gateway.connectors.compras_publicas.querido_diario_okbr import (
                QueridoDiarioOkbrConnector,
            )

            self._querido_diario_connector = QueridoDiarioOkbrConnector(
                settings=self._sdk.settings
            )
        return self._querido_diario_connector

    def get_querido_diario_tenders(self, termo: str) -> list[dict[str, Any]]:
        """Lookup indexed municipal official gazette tenders/dispensations by code, theme, IBGE, UF, or city."""
        return self.querido_diario_connector.get_gazette_tenders(termo)

    def get_querido_diario_excerpts(self, termo: str) -> list[dict[str, Any]]:
        """Lookup 12-month indexed gazette editions, education/health & ESG mentions, and OCR legibility."""
        return self.querido_diario_connector.get_thematic_excerpts(termo)

    def get_querido_diario_transparency(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve historical gazette archive depth, open format share, and regularity."""
        return self.querido_diario_connector.get_active_transparency_metrics(
            id_municipio_or_name
        )

    def get_querido_diario_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal Querido Diário editions, tenders, and cited values."""
        return self.querido_diario_connector.get_gazette_summary(id_municipio_or_name)

    def get_querido_diario_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold Querido Diário OKBR official gazette transparency score and tier."""
        return self.querido_diario_connector.get_gazette_features(id_municipio_or_name)

    def list_top_querido_diario_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by gazette transparency score or cited contract value."""
        return self.querido_diario_connector.list_top_gazette_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
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

    @property
    def pgfn_connector(self) -> Any:
        if not hasattr(self, "_pgfn_conn") or self._pgfn_conn is None:
            from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
                PgfnDividaAtivaConnector,
            )

            self._pgfn_conn = PgfnDividaAtivaConnector(settings=self._sdk.settings)
        return self._pgfn_conn

    def get_pgfn_debtor(self, documento_or_name: str) -> list[dict[str, Any]]:
        """Lookup active PGFN DAU inscriptions by CNPJ/CPF digits, name, or IBGE municipality."""
        return self.pgfn_connector.get_debtor_inscriptions(documento_or_name)

    def get_pgfn_previdenciario(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Lookup PGFN Social Security debts by IBGE municipality, UF, or CNPJ digits."""
        return self.pgfn_connector.get_previdenciario_debts(id_municipio_or_doc)

    def get_pgfn_fgts(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Lookup PGFN FGTS active debts by IBGE municipality, UF, or CNPJ digits."""
        return self.pgfn_connector.get_fgts_debts(id_municipio_or_doc)

    def get_pgfn_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve municipal consolidated PGFN debt summary."""
        return self.pgfn_connector.get_municipal_summary(id_municipio_or_name)

    def get_pgfn_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold PGFN tax default & judicial execution risk features."""
        return self.pgfn_connector.get_pgfn_features(id_municipio_or_name)

    def list_top_pgfn_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by consolidated PGFN active debt volume."""
        return self.pgfn_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def sinesp_connector(self) -> Any:
        if (
            not hasattr(self, "_sinesp_connector")
            or self._sinesp_connector is None
        ):
            from public_data_gateway.connectors.compliance.sinesp_seguranca import (
                SinespSegurancaConnector,
            )

            self._sinesp_connector = SinespSegurancaConnector(
                settings=self._sdk.settings
            )
        return self._sinesp_connector

    def get_sinesp_crimes(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SINESP criminal offenses by code, crime type, IBGE 7D, UF, or city."""
        return self.sinesp_connector.get_sinesp_crimes(termo)

    def get_sinesp_mvi_victims(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SINESP/FBSP intentional violent deaths (MVI) by code, IBGE 7D, UF, or city."""
        return self.sinesp_connector.get_sinesp_mvi_victims(termo)

    def get_sinesp_security_capacity(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal police stations, guard, and security spending per capita."""
        return self.sinesp_connector.get_sinesp_security_capacity(
            id_municipio_or_name
        )

    def get_sinesp_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SINESP violent death and freight theft metrics."""
        return self.sinesp_connector.get_sinesp_municipal_summary(
            id_municipio_or_name
        )

    def get_sinesp_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold SINESP/FBSP public safety score and territorial security tier."""
        return self.sinesp_connector.get_sinesp_features(id_municipio_or_name)

    def list_top_safest_sinesp_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_cargo_theft: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by public safety score or cargo theft rate."""
        return self.sinesp_connector.list_top_safest_sinesp_municipalities(
            limit=limit,
            sigla_uf=sigla_uf,
            sort_by_cargo_theft=sort_by_cargo_theft,
        )

    @property
    def cgu_gastos_connector(self) -> Any:
        if (
            not hasattr(self, "_cgu_gastos_connector")
            or self._cgu_gastos_connector is None
        ):
            from public_data_gateway.connectors.compliance.cgu_transparencia_gastos import (
                CguTransparenciaGastosConnector,
            )

            self._cgu_gastos_connector = CguTransparenciaGastosConnector(
                settings=self._sdk.settings
            )
        return self._cgu_gastos_connector

    def get_cgu_direct_expenses(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CGU federal expenses by code, SIAFI agency, function, modality, IBGE, UF, or city."""
        return self.cgu_gastos_connector.get_cgu_direct_expenses(termo)

    def get_cgu_constitutional_transfers(self, termo: str) -> list[dict[str, Any]]:
        """Lookup federal FPM, FUNDEB, and SUS transfers by code, IBGE, UF, or city."""
        return self.cgu_gastos_connector.get_cgu_constitutional_transfers(termo)

    def get_cgu_amendments_covenants(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal active federal covenants, parliamentary amendments, accountability, and CPGF spend."""
        return self.cgu_gastos_connector.get_cgu_amendments_covenants(
            id_municipio_or_name
        )

    def get_cgu_expenses_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal federal spend, transfers, budget execution, and per capita BRL."""
        return self.cgu_gastos_connector.get_cgu_expenses_summary(
            id_municipio_or_name
        )

    def get_cgu_expenses_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold CGU spending execution integrity score and transparency tier."""
        return self.cgu_gastos_connector.get_cgu_expenses_features(
            id_municipio_or_name
        )

    def list_top_cgu_transparency_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_spending: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by CGU execution integrity score or total federal spend."""
        return self.cgu_gastos_connector.list_top_cgu_transparency_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_spending=sort_by_spending
        )

    @property
    def cgu_servidores_connector(self) -> Any:
        if (
            not hasattr(self, "_cgu_servidores_connector")
            or self._cgu_servidores_connector is None
        ):
            from public_data_gateway.connectors.compliance.cgu_servidores_federais import (
                CguServidoresFederaisConnector,
            )

            self._cgu_servidores_connector = CguServidoresFederaisConnector(
                settings=self._sdk.settings
            )
        return self._cgu_servidores_connector

    def get_siape_active_staff(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SIAPE federal civil servants by code, agency, legal regime, IBGE, UF, or city."""
        return self.cgu_servidores_connector.get_siape_active_staff(termo)

    def get_siape_payroll(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SIAPE monthly gross average salary, ceiling share, and DAS/FCE extras."""
        return self.cgu_servidores_connector.get_siape_payroll(termo)

    def get_cgu_staff_compliance(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal SIAPE exclusive dedication share, double-dipping alerts, and integrity."""
        return self.cgu_servidores_connector.get_cgu_staff_compliance(
            id_municipio_or_name
        )

    def get_siape_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SIAPE headcount, monthly gross payroll, and DAS share."""
        return self.cgu_servidores_connector.get_siape_municipal_summary(
            id_municipio_or_name
        )

    def get_siape_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold CGU/SIAPE civil service governance score and tier."""
        return self.cgu_servidores_connector.get_siape_features(id_municipio_or_name)

    def list_top_siape_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_payroll: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SIAPE governance score or monthly federal payroll."""
        return self.cgu_servidores_connector.list_top_siape_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_payroll=sort_by_payroll
        )

    @property
    def tse_eleicoes_connector(self) -> Any:
        if (
            not hasattr(self, "_tse_eleicoes_connector")
            or self._tse_eleicoes_connector is None
        ):
            from public_data_gateway.connectors.compliance.tse_eleicoes import (
                TseEleicoesConnector,
            )

            self._tse_eleicoes_connector = TseEleicoesConnector(
                settings=self._sdk.settings
            )
        return self._tse_eleicoes_connector

    def get_tse_candidacies(self, termo: str) -> list[dict[str, Any]]:
        """Lookup TSE municipal candidacies by election code, office, IBGE, UF, or city."""
        return self.tse_eleicoes_connector.get_candidacies(termo)

    def get_tse_turnout(self, termo: str) -> list[dict[str, Any]]:
        """Lookup TSE municipal electorate, turnout, abstention, and valid votes."""
        return self.tse_eleicoes_connector.get_turnout_stats(termo)

    def get_tse_campaign_finance(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve TSE campaign revenue, spending, approval rate, and distinct donors."""
        return self.tse_eleicoes_connector.get_campaign_finance(id_municipio_or_name)

    def get_tse_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal TSE electorate, candidacies, and campaign cost."""
        return self.tse_eleicoes_connector.get_electoral_summary(id_municipio_or_name)

    def get_tse_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold TSE electoral participation and integrity score and tier."""
        return self.tse_eleicoes_connector.get_electoral_features(id_municipio_or_name)

    def list_top_tse_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_electorate: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by TSE electoral integrity score or electorate size."""
        return self.tse_eleicoes_connector.list_top_electoral_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_electorate=sort_by_electorate
        )


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

    @property
    def tst_connector(self) -> Any:
        if not hasattr(self, "_tst_connector") or getattr(self, "_tst_connector", None) is None:
            from public_data_gateway.connectors.judicial.tst_cndt.connector import (
                TstCndtConnector,
            )
            self._tst_connector = TstCndtConnector(settings=self._sdk.settings)
        return self._tst_connector

    def cndt(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Query official CNDT labor compliance certificate from TST."""
        return self.tst_connector.get_cndt(cnpj_or_cpf)

    def labor_risk(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Assess labor compliance risk score and BNDT debtor exposure."""
        return self.tst_connector.get_labor_risk(cnpj_or_cpf)

    def search_debitos_trabalhistas(self, termo: str) -> list[dict[str, Any]]:
        """Search Banco Nacional de Devedores Trabalhistas (BNDT) records."""
        return self.tst_connector.search_debitos(termo)

    @property
    def justica_aberta_connector(self) -> Any:
        if (
            not hasattr(self, "_justica_aberta_connector")
            or getattr(self, "_justica_aberta_connector", None) is None
        ):
            from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
                CNJJusticaAbertaConnector,
            )

            self._justica_aberta_connector = CNJJusticaAbertaConnector(
                settings=self._sdk.settings
            )
        return self._justica_aberta_connector

    def get_justica_aberta_offices(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal extrajudicial notary offices registry and titular tenure (CNJ Justiça Aberta)."""
        return self.justica_aberta_connector.get_notary_offices_registry(id_municipio)

    def list_justica_aberta_productivity(
        self, id_municipio: str, atribuicao_cartorial: str | None = None
    ) -> list[dict[str, Any]]:
        """List notary acts, certificates, and e-Notariado/SREI share by attribution (CNJ Justiça Aberta)."""
        return self.justica_aberta_connector.list_productivity_by_attribution(
            id_municipio=id_municipio, atribuicao_cartorial=atribuicao_cartorial
        )

    def get_justica_aberta_emoluments(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal cartorial emoluments revenue, statutory transfers, and unit ticket (CNJ)."""
        return self.justica_aberta_connector.get_emoluments_revenue(id_municipio)

    def get_justica_aberta_digitalization(self, id_municipio: str) -> dict[str, Any]:
        """Get historical archive digitization, SREI/ONR/CRC integration, and transparency profile."""
        return self.justica_aberta_connector.get_digitalization_compliance(id_municipio)

    def get_justica_aberta_features(self, id_municipio: str) -> dict[str, Any]:
        """Get Gold CNJ Justiça Aberta cartorial efficiency and real-estate/notary dynamism features."""
        return self.justica_aberta_connector.get_justica_aberta_features(id_municipio)

    def list_top_justica_aberta_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite CNJ cartorial efficiency score."""
        return self.justica_aberta_connector.list_top_notary_efficiency_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def improbidade_connector(self) -> Any:
        if (
            not hasattr(self, "_improbidade_connector")
            or getattr(self, "_improbidade_connector", None) is None
        ):
            from public_data_gateway.connectors.judicial.cnj_improbidade import (
                CnjImprobidadeConnector,
            )

            self._improbidade_connector = CnjImprobidadeConnector(
                settings=self._sdk.settings
            )
        return self._improbidade_connector

    def get_improbidade_condenacoes(
        self, documento_or_name: str
    ) -> list[dict[str, Any]]:
        """Query CNJ CNCIAI administrative misconduct condemnations by document, name, or IBGE code."""
        return self.improbidade_connector.get_condemnations(documento_or_name)

    def get_improbidade_proibicoes_contratar(
        self, id_municipio_or_doc: str
    ) -> list[dict[str, Any]]:
        """Query active public contracting and subsidy debarment sanctions (CNJ CNCIAI)."""
        return self.improbidade_connector.get_debarment_sanctions(
            id_municipio_or_doc
        )

    def get_improbidade_ressarcimentos(
        self, id_municipio_or_doc: str
    ) -> list[dict[str, Any]]:
        """Query public treasury reimbursement determinations and execution status (CNJ CNCIAI)."""
        return self.improbidade_connector.get_treasury_reimbursements(
            id_municipio_or_doc
        )

    def get_improbidade_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal consolidated CNJ CNCIAI administrative misconduct summary."""
        return self.improbidade_connector.get_municipal_summary(
            id_municipio_or_name
        )

    def get_improbidade_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold CNJ CNCIAI administrative misconduct and procurement exclusion risk features."""
        return self.improbidade_connector.get_improbidade_features(
            id_municipio_or_name
        )

    def list_top_improbidade_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold CNJ CNCIAI misconduct risk exposure."""
        return self.improbidade_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def onr_connector(self) -> Any:
        if (
            not hasattr(self, "_onr_connector")
            or getattr(self, "_onr_connector", None) is None
        ):
            from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
                ONRRegistroImoveisConnector,
            )

            self._onr_connector = ONRRegistroImoveisConnector(settings=self._sdk.settings)
        return self._onr_connector


class CompanyFacade:
    """Corporate registries, tax regimes, and regional fiscal incentives (RFB & SUFRAMA)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._suframa_connector: Any = None

    @property
    def suframa_connector(self) -> Any:
        if self._suframa_connector is None:
            from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
                SuframaPimZfmConnector,
            )

            self._suframa_connector = SuframaPimZfmConnector(settings=self._sdk.settings)
        return self._suframa_connector

    def suframa(self, cnpj_or_inscricao: str) -> dict[str, Any]:
        """Query SUFRAMA registration, incentive approval, and operational status."""
        return self.suframa_connector.get_suframa_status(cnpj_or_inscricao)

    def tax_incentives(self, cnpj: str) -> dict[str, Any]:
        """Assess corporate fiscal incentive score and industrial capacity profile (PIM/ZFM/ALC)."""
        return self.suframa_connector.get_tax_incentives_profile(cnpj)

    def list_pim_projects(
        self,
        cnpj_or_inscricao: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """List industrial investment projects submitted to SUFRAMA CAS."""
        return self.suframa_connector.list_projetos(
            cnpj_or_inscricao=cnpj_or_inscricao, status=status
        )

    def simples_status(self, cnpj: str) -> dict[str, Any]:
        """Resolve Simples Nacional and MEI tax regime for a given CNPJ."""
        return self._sdk.get_simples_status(cnpj)

    @property
    def cno_connector(self) -> Any:
        if not hasattr(self, "_cno_conn") or self._cno_conn is None:
            from public_data_gateway.connectors.empresarial.rfb_cno import (
                RfbCnoConnector,
            )

            self._cno_conn = RfbCnoConnector(settings=self._sdk.settings)
        return self._cno_conn

    def get_cno_works(self, termo: str) -> list[dict[str, Any]]:
        """Query registered RFB CNO civil works by CNO code, CNPJ, IBGE 7D, CEP, CNAE, or name."""
        return self.cno_connector.get_works(termo)

    def get_cno_area_breakdown(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal built area breakdown (residential, commercial, infrastructure m²)."""
        return self.cno_connector.get_area_breakdown(id_municipio_or_name)

    def get_cno_responsible_links(self, termo: str) -> list[dict[str, Any]]:
        """Lookup construction firm and property owner links in RFB CNO."""
        return self.cno_connector.get_responsible_links(termo)

    def get_cno_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal consolidated RFB CNO work counts and built area."""
        return self.cno_connector.get_municipal_summary(id_municipio_or_name)

    def get_cno_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold RFB CNO civil construction dynamism and CAPEX pipeline features."""
        return self.cno_connector.get_cno_features(id_municipio_or_name)

    def list_top_cno_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold RFB CNO civil construction dynamism score."""
        return self.cno_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def inpi_pi_connector(self) -> Any:
        if (
            not hasattr(self, "_inpi_pi_connector")
            or self._inpi_pi_connector is None
        ):
            from public_data_gateway.connectors.empresarial.inpi_propriedade_intelectual import (
                InpiPropriedadeIntelectualConnector,
            )

            self._inpi_pi_connector = InpiPropriedadeIntelectualConnector(
                settings=self._sdk.settings
            )
        return self._inpi_pi_connector

    def get_inpi_patentes(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INPI patents and IP filings by process code, nature, IPC/Nice, IBGE, UF, or city."""
        return self.inpi_pi_connector.get_inpi_patentes(termo)

    def get_inpi_marcas(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INPI active trademark registrations and industrial designs."""
        return self.inpi_pi_connector.get_inpi_marcas(termo)

    def get_inpi_software_transferencia(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal INPI computer program registrations and technology transfers."""
        return self.inpi_pi_connector.get_inpi_software_transferencia(
            id_municipio_or_name
        )

    def get_inpi_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal INPI patents, trademarks, software, and per-capita."""
        return self.inpi_pi_connector.get_inpi_municipal_summary(
            id_municipio_or_name
        )

    def get_inpi_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold INPI intellectual-property innovation score and Deep Tech tier."""
        return self.inpi_pi_connector.get_inpi_features(id_municipio_or_name)

    def list_top_inpi_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_patentes: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by INPI intellectual-property innovation score or patents."""
        return self.inpi_pi_connector.list_top_inpi_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_patentes=sort_by_patentes
        )


class FinanceFacade:
    """Capital markets, standardized financial statements and solvency metrics (CVM DFP/ITR)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._cvm_connector: Any = None

    @property
    def cvm_connector(self) -> Any:
        if self._cvm_connector is None:
            from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
                CvmDfpItrConnector,
            )

            self._cvm_connector = CvmDfpItrConnector(settings=self._sdk.settings)
        return self._cvm_connector

    def cvm_company(self, cnpj_or_codigo: str) -> dict[str, Any]:
        """Query CVM company registration and latest financial snapshot by CNPJ or CVM code."""
        return self.cvm_connector.get_company(cnpj_or_codigo)

    def financial_ratios(self, cnpj_or_codigo: str) -> dict[str, Any]:
        """Query fundamental financial ratios (liquidez, margens, ROE, endividamento) and solvency score."""
        return self.cvm_connector.get_financial_ratios(cnpj_or_codigo)

    def list_cvm_companies(
        self,
        setor: str | None = None,
        situacao: str | None = None,
    ) -> list[dict[str, Any]]:
        """List publicly traded companies registered with CVM."""
        return self.cvm_connector.list_companies(setor=setor, situacao=situacao)

    @property
    def cvm_extra_connector(self) -> Any:
        if (
            not hasattr(self, "_cvm_extra_connector")
            or self._cvm_extra_connector is None
        ):
            from public_data_gateway.connectors.macrofinanceiro.cvm_mercado_extra import (
                CvmMercadoExtraConnector,
            )

            self._cvm_extra_connector = CvmMercadoExtraConnector(
                settings=self._sdk.settings
            )
        return self._cvm_extra_connector

    def get_rcvm160_offerings(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CVM RCVM 160 public offerings by code, security type, rite, IBGE, UF, or city."""
        return self.cvm_extra_connector.get_rcvm160_offerings(termo)

    def get_fii_administrators(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CVM portfolio managers, FII/FIP managers, AUM, and regulatory compliance."""
        return self.cvm_extra_connector.get_fii_administrators(termo)

    def get_investment_advisors(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal CVM active investment advisors (AAIs), firms, and PASM risk."""
        return self.cvm_extra_connector.get_investment_advisors(id_municipio_or_name)

    def get_cvm_extra_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal RCVM 160 capital raised, managers, and AAIs."""
        return self.cvm_extra_connector.get_cvm_extra_summary(id_municipio_or_name)

    def get_cvm_extra_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold CVM capital markets sophistication score and ecosystem tier."""
        return self.cvm_extra_connector.get_cvm_extra_features(id_municipio_or_name)

    def list_top_cvm_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by CVM capital markets sophistication score or RCVM 160 volume."""
        return self.cvm_extra_connector.list_top_cvm_extra_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )

    @property
    def susep_seguros_connector(self) -> Any:
        if (
            not hasattr(self, "_susep_seguros_connector")
            or self._susep_seguros_connector is None
        ):
            from public_data_gateway.connectors.macrofinanceiro.susep_seguros import (
                SusepSegurosConnector,
            )

            self._susep_seguros_connector = SusepSegurosConnector(
                settings=self._sdk.settings
            )
        return self._susep_seguros_connector

    def get_susep_premios_ramo(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SUSEP insurance premiums & loss ratios by code, branch, IBGE, UF, or city."""
        return self.susep_seguros_connector.get_susep_premios_ramo(termo)

    def get_susep_previdencia_vgbl(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SUSEP VGBL/PGBL pension contributions and technical provisions."""
        return self.susep_seguros_connector.get_susep_previdencia_vgbl(termo)

    def get_susep_corretoras_solvencia(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal SUSEP active insurance brokers and regulatory solvency index."""
        return self.susep_seguros_connector.get_susep_corretoras_solvencia(
            id_municipio_or_name
        )

    def get_susep_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SUSEP issued premiums, claims, and per-capita."""
        return self.susep_seguros_connector.get_susep_municipal_summary(
            id_municipio_or_name
        )

    def get_susep_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SUSEP insurance market maturity score and solvency tier."""
        return self.susep_seguros_connector.get_susep_features(id_municipio_or_name)

    def list_top_susep_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_premios: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SUSEP insurance maturity score or issued premiums."""
        return self.susep_seguros_connector.list_top_susep_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_premios=sort_by_premios
        )


class TradeFacade:
    """Foreign trade statistics, export/import flows and trade balance (MDIC Comex Stat)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._comex_connector: Any = None

    @property
    def comex_connector(self) -> Any:
        if self._comex_connector is None:
            from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
                MDICComexStatConnector,
            )

            self._comex_connector = MDICComexStatConnector(settings=self._sdk.settings)
        return self._comex_connector

    def municipality_trade(self, id_municipio: str) -> dict[str, Any]:
        """Obter fluxo de comércio exterior e saldo comercial por município (7 dígitos IBGE)."""
        return self.comex_connector.get_municipality_trade(id_municipio)

    def ncm_trade(self, codigo_ncm: str) -> dict[str, Any]:
        """Consultar histórico e fluxo estatístico de uma mercadoria NCM de 8 dígitos."""
        return self.comex_connector.get_ncm_trade(codigo_ncm)

    def trade_balance_profile(self) -> dict[str, Any]:
        """Obter o panorama macro da balança comercial brasileira."""
        return self.comex_connector.get_trade_balance_profile()


class FundsFacade:
    """Investment funds regulatory registry, daily reports, asset managers and liquidity (CVM ICVM 555 / Resolução 175)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._fundos_connector: Any = None

    @property
    def fundos_connector(self) -> Any:
        if self._fundos_connector is None:
            from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
                CvmFundosConnector,
            )

            self._fundos_connector = CvmFundosConnector(settings=self._sdk.settings)
        return self._fundos_connector

    def get_fund_by_cnpj(self, cnpj: str) -> dict[str, Any] | None:
        """Lookup an investment fund by 14-digit CNPJ."""
        return self.fundos_connector.get_fund_by_cnpj(cnpj)

    def get_daily_reports(
        self, cnpj: str, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve daily reports for an investment fund."""
        return self.fundos_connector.get_daily_reports(cnpj, start_date=start_date, end_date=end_date)

    def get_asset_managers(
        self, query: str | None = None, min_aum: float = 0.0
    ) -> list[dict[str, Any]]:
        """Retrieve Asset Management firms ranked by AuM with optional filter."""
        return self.fundos_connector.get_asset_managers(query=query, min_aum=min_aum)

    def get_fund_features(self, cnpj: str) -> dict[str, Any] | None:
        """Retrieve pre-computed Gold feature vector and resilience score for a fund."""
        return self.fundos_connector.get_fund_features(cnpj)


class BankingFacade:
    """Prudential banking, regulatory capital, balances, and solidity metrics (BACEN IF.data / Resolução CMN 4.966 / 4.553)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._banking_connector: Any = None

    @property
    def banking_connector(self) -> Any:
        if self._banking_connector is None:
            from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
                BcbIfDataConnector,
            )

            self._banking_connector = BcbIfDataConnector(settings=self._sdk.settings)
        return self._banking_connector

    def get_bank_by_cnpj(self, cnpj: str) -> dict[str, Any] | None:
        """Lookup prudential financial institution profile by 8 or 14-digit CNPJ."""
        return self.banking_connector.get_bank_by_cnpj(cnpj)

    def get_bank_balance_sheet(self, cnpj: str) -> dict[str, Any] | None:
        """Retrieve quarterly balance sheet and P&L (Ativo, Passivo, DRE) for a financial institution."""
        return self.banking_connector.get_bank_balance_sheet(cnpj)

    def get_banking_ranking(self, limit: int = 10) -> list[dict[str, Any]]:
        """Retrieve top financial institutions in Brazil ranked by Total Assets with Basiléia index."""
        return self.banking_connector.get_banking_ranking(limit=limit)

    def get_bank_solidity(self, cnpj: str) -> dict[str, Any] | None:
        """Retrieve pre-computed Gold feature vector and banking solidity score (0-100) for an institution."""
        return self.banking_connector.get_bank_solidity(cnpj)

    @property
    def estban_connector(self) -> Any:
        if not hasattr(self, "_estban_conn") or self._estban_conn is None:
            from public_data_gateway.connectors.macrofinanceiro.bcb_estban import (
                BcbEstbanConnector,
            )

            self._estban_conn = BcbEstbanConnector(settings=self._sdk.settings)
        return self._estban_conn

    def get_estban_municipal_balance(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve monthly BACEN ESTBAN banking balance sheet for a municipality."""
        return self.estban_connector.get_municipal_balance(id_municipio_or_name)

    def get_estban_credit_portfolio(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal sectoral credit portfolio (rural, real estate, PCLD) from ESTBAN."""
        return self.estban_connector.get_credit_portfolio(id_municipio_or_name)

    def get_estban_deposit_funding(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal deposit funding breakdown (demand, savings, term, public) from ESTBAN."""
        return self.estban_connector.get_deposit_funding(id_municipio_or_name)

    def get_estban_local_institutions(
        self, id_municipio_or_cnpj: str
    ) -> list[dict[str, Any]]:
        """Retrieve financial institutions operating locally via ESTBAN."""
        return self.estban_connector.get_local_institutions(id_municipio_or_cnpj)

    def get_estban_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold BACEN ESTBAN local banking depth and liquidity features."""
        return self.estban_connector.get_estban_features(id_municipio_or_name)

    def list_top_estban_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold BACEN ESTBAN banking depth score."""
        return self.estban_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def agencia_connector(self) -> Any:
        if not hasattr(self, "_agencia_conn") or self._agencia_conn is None:
            from public_data_gateway.connectors.macrofinanceiro.bcb_agencia import (
                BcbAgenciaConnector,
            )

            self._agencia_conn = BcbAgenciaConnector(settings=self._sdk.settings)
        return self._agencia_conn

    def get_bcb_agencias(
        self, id_municipio_or_cnpj: str
    ) -> list[dict[str, Any]]:
        """Lookup registered BACEN bank branches by IBGE 7D, CNPJ, CEP, UF, or name."""
        return self.agencia_connector.get_branches(id_municipio_or_cnpj)

    def get_bcb_postos_atendimento(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal banking service outposts (PA, PAE, administrative units)."""
        return self.agencia_connector.get_service_outposts(id_municipio_or_name)

    def get_bcb_cobertura_municipal(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal bank branch density per 100k inhabitants and public/private mix."""
        return self.agencia_connector.get_municipal_coverage(
            id_municipio_or_name
        )

    def get_bcb_inclusao_financeira(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve financial inclusion readiness and bank-correspondent dependency status."""
        return self.agencia_connector.get_financial_inclusion_status(
            id_municipio_or_name
        )

    def get_bcb_agencia_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold BACEN physical banking capillarity features."""
        return self.agencia_connector.get_agencia_features(id_municipio_or_name)

    def list_top_bcb_agencia_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold BACEN physical banking capillarity score."""
        return self.agencia_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def bndes_connector(self) -> Any:
        if (
            not hasattr(self, "_bndes_connector")
            or self._bndes_connector is None
        ):
            from public_data_gateway.connectors.macrofinanceiro.bndes_operacoes import (
                BndesOperacoesConnector,
            )

            self._bndes_connector = BndesOperacoesConnector(
                settings=self._sdk.settings
            )
        return self._bndes_connector

    def get_bndes_operations(self, termo: str) -> list[dict[str, Any]]:
        """Lookup BNDES direct/indirect operations by code, modality, sector, IBGE, UF, or city."""
        return self.bndes_connector.get_credit_operations(termo)

    def get_bndes_disbursements(self, termo: str) -> list[dict[str, Any]]:
        """Lookup annual BNDES disbursements, green/ESG share, and MSME participation."""
        return self.bndes_connector.get_disbursements(termo)

    def get_bndes_portfolio_risk(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve BNDES average TLP rate, default share, FGI coverage, and ESG projects."""
        return self.bndes_connector.get_portfolio_risk(id_municipio_or_name)

    def get_bndes_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal BNDES operations count and contracted volume."""
        return self.bndes_connector.get_bndes_summary(id_municipio_or_name)

    def get_bndes_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold BNDES development financing dynamism score and maturity tier."""
        return self.bndes_connector.get_bndes_features(id_municipio_or_name)

    def list_top_bndes_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by BNDES financing dynamism score or contracted volume."""
        return self.bndes_connector.list_top_bndes_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )


class TreasuryFacade:
    """Public finances, SADIPEM credit operations, CAPAG rating and FPM transfers (STN Tesouro Transparente)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._treasury_connector: Any = None

    @property
    def treasury_connector(self) -> Any:
        if self._treasury_connector is None:
            from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
                StnTesouroTransparenteConnector,
            )

            self._treasury_connector = StnTesouroTransparenteConnector(settings=self._sdk.settings)
        return self._treasury_connector

    def get_municipio_fiscal_profile(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Lookup municipality fiscal health, debt ratio and CAPAG grade."""
        return self.treasury_connector.get_municipio_fiscal_profile(id_municipio_or_name)

    def get_credit_operations_pvl(
        self, id_municipio_or_uf: str, status: str | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve SADIPEM credit operations for a municipality or UF."""
        return self.treasury_connector.get_credit_operations_pvl(id_municipio_or_uf, status=status)

    def get_fpm_transfers(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve constitutional FPM transfer history for a municipality."""
        return self.treasury_connector.get_fpm_transfers(id_municipio, ano=ano)

    def get_capag_rating(self, id_municipio: str) -> dict[str, Any] | None:
        """Retrieve official CAPAG payment capacity rating and indicators for a municipality."""
        return self.treasury_connector.get_capag_rating(id_municipio)

    @property
    def siconfi_connector(self) -> Any:
        if not hasattr(self, "_siconfi_conn") or self._siconfi_conn is None:
            from public_data_gateway.connectors.macrofinanceiro.stn_siconfi import (
                StnSiconfiConnector,
            )

            self._siconfi_conn = StnSiconfiConnector(settings=self._sdk.settings)
        return self._siconfi_conn

    def get_siconfi_ente(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve SICONFI federative entity registration profile by IBGE 7D or name."""
        return self.siconfi_connector.get_ente_profile(id_municipio_or_name)

    def get_siconfi_dca(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve Annual Accounts Declaration (DCA/Finbra) balance sheet for a municipality."""
        return self.siconfi_connector.get_dca_balance(id_municipio, ano=ano)

    def get_siconfi_rreo(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve Budget Execution Summary (RREO) rows for a municipality."""
        return self.siconfi_connector.get_rreo_execution(id_municipio, ano=ano)

    def get_siconfi_rgf(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve Fiscal Management Report (RGF) LRF personnel/debt limits for a municipality."""
        return self.siconfi_connector.get_rgf_limits(id_municipio, ano=ano)

    def get_siconfi_extratos(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Retrieve SICONFI statement submission compliance extracts for a municipality."""
        return self.siconfi_connector.get_delivery_extracts(id_municipio, ano=ano)

    def get_siconfi_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SICONFI fiscal & LRF feature store profile for a municipality."""
        return self.siconfi_connector.get_siconfi_features(id_municipio_or_name)

    def list_top_siconfi_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SICONFI accounting solidity score."""
        return self.siconfi_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def capag_ifgf_connector(self) -> Any:
        if not hasattr(self, "_capag_ifgf_conn") or self._capag_ifgf_conn is None:
            from public_data_gateway.connectors.macrofinanceiro.stn_capag import (
                StnCapagConnector,
            )

            self._capag_ifgf_conn = StnCapagConnector(settings=self._sdk.settings)
        return self._capag_ifgf_conn

    def get_capag_municipal_evaluation(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve official STN CAPAG municipal evaluation and ICF ranking."""
        return self.capag_ifgf_connector.get_municipal_evaluation(
            id_municipio_or_name
        )

    def get_capag_state_evaluation(
        self, sigla_uf_or_id: str
    ) -> list[dict[str, Any]]:
        """Retrieve official STN CAPAG state evaluation and fiscal space."""
        return self.capag_ifgf_connector.get_state_evaluation(sigla_uf_or_id)

    def get_capag_ifgf_index(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve FIRJAN Municipal Fiscal Management Index (IFGF) components."""
        return self.capag_ifgf_connector.get_ifgf_index(id_municipio_or_name)

    def get_capag_sovereign_guarantees(
        self, id_municipio_or_name: str
    ) -> list[dict[str, Any]]:
        """Retrieve Union sovereign guarantee stock and credit operations."""
        return self.capag_ifgf_connector.get_sovereign_guarantees(
            id_municipio_or_name
        )

    def get_capag_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold subnational sovereign credit rating and CAPAG/IFGF features."""
        return self.capag_ifgf_connector.get_capag_features(id_municipio_or_name)

    def list_top_capag_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by composite CAPAG + IFGF sovereign credit score."""
        return self.capag_ifgf_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )


class SocioeconomicFacade:
    """Socioeconomic, GDP and demographic query interface powered by IBGE SIDRA."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._connector: Any = None

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
                IBGESidraConnector,
            )

            self._connector = IBGESidraConnector(settings=self._sdk.settings)
        return self._connector

    def gdp(self, id_municipio: str) -> dict[str, Any] | None:
        """Get official municipal GDP indicators by 7-digit IBGE code."""
        return self.connector.get_municipality_gdp(id_municipio)

    def profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Get comprehensive Gold socioeconomic profile and indicators for a municipality."""
        return self.connector.get_municipality_socioeconomic_profile(id_municipio)

    def ipca_history(self, limit: int = 12) -> list[dict[str, Any]]:
        """Get IPCA inflation historical series."""
        return self.connector.get_ipca_history(limit=limit)

    def gini_uf(self, sigla_uf: str) -> dict[str, Any] | None:
        """Get Gini index and household income by State (UF)."""
        return self.connector.get_gini_uf(sigla_uf)

    def top_municipalities_by_gdp(self, limit: int = 10) -> list[dict[str, Any]]:
        """List top municipalities ranked by total GDP."""
        return self.connector.list_top_municipalities_by_gdp(limit=limit)

    @property
    def censo_connector(self) -> Any:
        if not hasattr(self, "_censo_conn") or self._censo_conn is None:
            from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
                IBGECenso2022Connector,
            )

            self._censo_conn = IBGECenso2022Connector(settings=self._sdk.settings)
        return self._censo_conn

    def census_summary(self, id_municipio: str) -> dict[str, Any] | None:
        """Get official Censo 2022 population and household counts by 7-digit IBGE code."""
        return self.censo_connector.get_census_summary(id_municipio)

    def demographic_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Get comprehensive Gold demographic profile (age structure, literacy, scores) from Censo 2022."""
        return self.censo_connector.get_demographic_profile(id_municipio)

    def literacy_indicators(self, id_municipio: str) -> dict[str, Any] | None:
        """Get official literacy rates and literate population counts from Censo 2022."""
        return self.censo_connector.get_literacy_indicators(id_municipio)

    def highest_aging_municipalities(self, limit: int = 10) -> list[dict[str, Any]]:
        """List municipalities with highest aging index ratio from Censo 2022."""
        return self.censo_connector.list_highest_aging_municipalities(limit=limit)

    @property
    def pib_municipal_connector(self) -> Any:
        if not hasattr(self, "_pib_conn") or self._pib_conn is None:
            from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
                IBGEPibMunicipalConnector,
            )

            self._pib_conn = IBGEPibMunicipalConnector(settings=self._sdk.settings)
        return self._pib_conn

    def municipal_gdp_detail(self, id_municipio: str, year: int = 2023) -> dict[str, Any] | None:
        """Get comprehensive municipal macro accounts (PIB, sectoral VAB, taxes, shares) from IBGE."""
        return self.pib_municipal_connector.get_municipal_gdp_detail(id_municipio, year)

    def municipal_gdp_features(self, id_municipio: str) -> dict[str, Any] | None:
        """Get Gold features and productive diversification scores from IBGE municipal GDP."""
        return self.pib_municipal_connector.get_municipal_gdp_features(id_municipio)

    def top_municipalities_pib(self, limit: int = 10) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by total municipal GDP."""
        return self.pib_municipal_connector.list_top_municipalities(limit=limit)


class LaborFacade:
    """Unified developer facade for formal employment, labor, and wages."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._connector: Any = None
        self._pnad_connector: Any = None
        self._rais_connector: Any = None

    @property
    def connector(self) -> Any:
        if self._connector is None:
            from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
                MTENovoCagedConnector,
            )

            self._connector = MTENovoCagedConnector(settings=self._sdk.settings)
        return self._connector

    @property
    def pnad_connector(self) -> Any:
        if self._pnad_connector is None:
            from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
                IBGEPnadContinuaConnector,
            )

            self._pnad_connector = IBGEPnadContinuaConnector(settings=self._sdk.settings)
        return self._pnad_connector

    @property
    def rais_connector(self) -> Any:
        if self._rais_connector is None:
            from public_data_gateway.connectors.trabalho.mte_rais.connector import (
                MTERaisConnector,
            )

            self._rais_connector = MTERaisConnector(settings=self._sdk.settings)
        return self._rais_connector

    def rais_summary(self, id_municipio: str, ano: int | None = None) -> dict[str, Any] | None:
        """Return annual RAIS formal employment stock for a municipality."""
        return self.rais_connector.get_formal_employment_summary(id_municipio, ano=ano)

    def rais_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Return municipal RAIS Gold formalization features."""
        return self.rais_connector.get_rais_market_profile(id_municipio)

    def rais_sectors(self, id_municipio: str, ano: int | None = None) -> list[dict[str, Any]]:
        """Return RAIS employment stock broken down by CNAE sector."""
        return self.rais_connector.get_sectoral_employment(id_municipio, ano=ano)

    def rais_top_formalized(self, limit: int = 10) -> list[dict[str, Any]]:
        """Rank municipalities by RAIS formalization score."""
        return self.rais_connector.list_top_formalized_municipalities(limit=limit)

    @property
    def mps_inss_connector(self) -> Any:
        if (
            not hasattr(self, "_mps_inss_connector")
            or self._mps_inss_connector is None
        ):
            from public_data_gateway.connectors.trabalho.mps_inss_beneficios import (
                MpsInssBeneficiosConnector,
            )

            self._mps_inss_connector = MpsInssBeneficiosConnector(
                settings=self._sdk.settings
            )
        return self._mps_inss_connector

    def get_inss_issued_benefits(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INSS active issued benefits by code, species, group, clientele, IBGE, UF, or city."""
        return self.mps_inss_connector.get_inss_issued_benefits(termo)

    def get_inss_benefit_grants(self, termo: str) -> list[dict[str, Any]]:
        """Lookup annual INSS benefit grants, denial rates, and concession lead times."""
        return self.mps_inss_connector.get_inss_benefit_grants(termo)

    def get_mps_social_contributions(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal RGPS active contributors, collections, and contributor-to-beneficiary ratio."""
        return self.mps_inss_connector.get_mps_social_contributions(
            id_municipio_or_name
        )

    def get_inss_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal INSS benefits count, monthly cash injection, and BPC share."""
        return self.mps_inss_connector.get_inss_municipal_summary(
            id_municipio_or_name
        )

    def get_inss_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold MPS/INSS social security coverage score and sustainability tier."""
        return self.mps_inss_connector.get_inss_features(id_municipio_or_name)

    def list_top_inss_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by INSS coverage score or monthly benefit payout volume."""
        return self.mps_inss_connector.list_top_inss_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )

    def employment_summary(
        self, id_municipio: str, competencia: str | None = None
    ) -> dict[str, Any] | None:
        """Get official Novo CAGED employment movements and balance for a municipality."""
        return self.connector.get_employment_summary(id_municipio, competencia=competencia)

    def market_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Get comprehensive Gold labor market profile (salaries, turnover, heating score) for a municipality."""
        return self.connector.get_labor_market_profile(id_municipio)

    def sectoral(self, id_municipio: str) -> list[dict[str, Any]]:
        """Get employment movements broken down by major CNAE sector for a municipality."""
        return self.connector.get_sectoral_employment(id_municipio)

    def top_job_creators(self, limit: int = 10) -> list[dict[str, Any]]:
        """List municipalities with highest net job creation balance from Novo CAGED."""
        return self.connector.list_top_job_creating_municipalities(limit=limit)

    def force_indicators(
        self, id_or_sigla_uf: str, ano_trimestre: str | None = None
    ) -> dict[str, Any] | None:
        """Get official quarterly labor force and unemployment indicators for a Brazilian UF (PNAD Contínua)."""
        return self.pnad_connector.get_labor_force_indicators(id_or_sigla_uf, ano_trimestre=ano_trimestre)

    def state_profile(self, id_or_sigla_uf: str) -> dict[str, Any] | None:
        """Get comprehensive Gold labor market resilience and earnings profile for a Brazilian UF (PNAD Contínua)."""
        return self.pnad_connector.get_state_labor_profile(id_or_sigla_uf)

    def earnings(self, id_or_sigla_uf: str) -> dict[str, Any] | None:
        """Get real habitual and effective monthly earnings for a Brazilian UF (PNAD Contínua)."""
        return self.pnad_connector.get_earnings_summary(id_or_sigla_uf)

    def lowest_unemployment_states(self, limit: int = 10) -> list[dict[str, Any]]:
        """List Brazilian UFs with lowest unemployment rate from PNAD Contínua."""
        return self.pnad_connector.list_lowest_unemployment_states(limit=limit)

    def highest_earnings_states(self, limit: int = 10) -> list[dict[str, Any]]:
        """List Brazilian UFs with highest average real monthly earnings from PNAD Contínua."""
        return self.pnad_connector.list_highest_earnings_states(limit=limit)


class HealthFacade:
    """Unified developer facade for public health infrastructure, CNES networks, SIH hospitalizations, SIM mortality, SINASC live births, ANVISA CMED prices, and OpenDataSUS epidemiological surveillance analytics."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._cnes_connector: Any = None
        self._sih_connector: Any = None
        self._sim_connector: Any = None
        self._sinasc_connector: Any = None
        self._cmed_connector: Any = None
        self._vigilancia_connector: Any = None

    @property
    def cnes_connector(self) -> Any:
        if self._cnes_connector is None:
            from public_data_gateway.connectors.saude.datasus_cnes.connector import (
                DataSUSCnesConnector,
            )

            self._cnes_connector = DataSUSCnesConnector(settings=self._sdk.settings)
        return self._cnes_connector

    @property
    def sih_connector(self) -> Any:
        if self._sih_connector is None:
            from public_data_gateway.connectors.saude.datasus_sih.connector import (
                DataSUSSihConnector,
            )

            self._sih_connector = DataSUSSihConnector(settings=self._sdk.settings)
        return self._sih_connector

    @property
    def sim_connector(self) -> Any:
        if self._sim_connector is None:
            from public_data_gateway.connectors.saude.datasus_sim.connector import (
                DataSUSSimConnector,
            )

            self._sim_connector = DataSUSSimConnector(settings=self._sdk.settings)
        return self._sim_connector

    @property
    def sinasc_connector(self) -> Any:
        if self._sinasc_connector is None:
            from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
                DataSUSSinascConnector,
            )

            self._sinasc_connector = DataSUSSinascConnector(settings=self._sdk.settings)
        return self._sinasc_connector

    @property
    def cmed_connector(self) -> Any:
        if self._cmed_connector is None:
            from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
                ANVISACmedConnector,
            )

            self._cmed_connector = ANVISACmedConnector(settings=self._sdk.settings)
        return self._cmed_connector

    @property
    def vigilancia_connector(self) -> Any:
        if self._vigilancia_connector is None:
            from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
                OpenDataSUSVigilanciaConnector,
            )

            self._vigilancia_connector = OpenDataSUSVigilanciaConnector(
                settings=self._sdk.settings
            )
        return self._vigilancia_connector

    def get_cnes_establishments(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal summary of active health establishments and SUS network share (DataSUS CNES)."""
        return self.cnes_connector.get_establishments_summary(id_municipio)

    def get_cnes_beds_icu(self, id_municipio: str) -> dict[str, Any]:
        """Get hospital bed capacity and ICU beds (Adulto/Pediatrico/Neonatal) per municipality (DataSUS CNES)."""
        return self.cnes_connector.get_hospital_beds_icu(id_municipio)

    def get_cnes_equipment(self, id_municipio: str) -> dict[str, Any]:
        """Get high-complexity diagnostic imaging and dialysis equipment availability (DataSUS CNES)."""
        return self.cnes_connector.get_diagnostic_equipment(id_municipio)

    def list_cnes_workforce(
        self, id_municipio: str, categoria_profissional: str | None = None
    ) -> list[dict[str, Any]]:
        """List health professionals and Primary Care eSF teams per municipality (DataSUS CNES)."""
        return self.cnes_connector.list_health_workforce(
            id_municipio, categoria_profissional=categoria_profissional
        )

    def get_cnes_health_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold health capacity and ICU complexity features per municipality (DataSUS CNES)."""
        return self.cnes_connector.get_health_capacity_features(id_municipio)

    def list_top_health_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite CNES hospital capacity score."""
        return self.cnes_connector.list_top_health_hubs(limit=limit, sigla_uf=sigla_uf)

    def get_sih_admissions(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal hospital admissions summary (AIH), urgency share, and mean length of stay (DataSUS SIH/SUS)."""
        return self.sih_connector.get_admissions_summary(id_municipio)

    def get_sih_hospital_costs(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal SIH/SUS approved AIH hospital billing and ICU cost share (DataSUS SIH/SUS)."""
        return self.sih_connector.get_hospital_costs(id_municipio)

    def list_sih_cid10_morbidity(
        self, id_municipio: str, capitulo_cid10: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal SIH/SUS hospital admissions broken down by priority CID-10 chapters."""
        return self.sih_connector.list_cid10_morbidity(
            id_municipio, capitulo_cid10=capitulo_cid10
        )

    def get_sih_mortality_outcomes(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal SIH/SUS clinical outcomes (discharges, hospital deaths, ICU use, and ICSAP share)."""
        return self.sih_connector.get_mortality_outcomes(id_municipio)

    def get_sih_morbidity_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold SIH hospital complexity and epidemiological pressure features."""
        return self.sih_connector.get_sih_morbidity_features(id_municipio)

    def list_top_sih_complexity_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite DataSUS SIH/SUS hospital complexity score."""
        return self.sih_connector.list_top_sih_complexity_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sim_general_mortality(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal DataSUS SIM general mortality summary, hospital deaths, and ill-defined causes."""
        return self.sim_connector.get_general_mortality(id_municipio)

    def list_sim_cid10_causes(
        self, id_municipio: str, capitulo_cid10: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal DataSUS SIM deaths broken down by major CID-10 chapters."""
        return self.sim_connector.list_cid10_mortality_causes(
            id_municipio, capitulo_cid10=capitulo_cid10
        )

    def get_sim_maternal_infant_mortality(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal infant, neonatal, maternal mortality indicators and investigation share (DataSUS SIM)."""
        return self.sim_connector.get_maternal_infant_mortality(id_municipio)

    def get_sim_premature_dcnt_external(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal premature NCD mortality (30-69y), external causes, and YPLL (DataSUS SIM)."""
        return self.sim_connector.get_premature_dcnt_external_mortality(id_municipio)

    def get_sim_mortality_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold DataSUS SIM surveillance quality and preventable burden features."""
        return self.sim_connector.get_sim_mortality_features(id_municipio)

    def list_top_sim_surveillance_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite DataSUS SIM surveillance quality score."""
        return self.sim_connector.list_top_sim_surveillance_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sinasc_live_births(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal DataSUS SINASC live births volume, birth rate, hospital births, and cesarean share."""
        return self.sinasc_connector.get_live_births_summary(id_municipio)

    def list_sinasc_prenatal_care(
        self, id_municipio: str, faixa_assistencia_prenatal: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal DataSUS SINASC live births stratified by prenatal visits bracket."""
        return self.sinasc_connector.list_prenatal_care_breakdown(
            id_municipio, faixa_assistencia_prenatal=faixa_assistencia_prenatal
        )

    def get_sinasc_maternal_profile(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal maternal demographic profile (mean age, adolescent and >=12y schooling shares)."""
        return self.sinasc_connector.get_maternal_demographic_profile(id_municipio)

    def get_sinasc_newborn_vitality(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal newborn vitality indicators (low birth weight, prematurity, Apgar 5m, congenital anomalies)."""
        return self.sinasc_connector.get_newborn_vitality_conditions(id_municipio)

    def get_sinasc_perinatal_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold DataSUS SINASC prenatal care quality and perinatal vitality features."""
        return self.sinasc_connector.get_sinasc_perinatal_features(id_municipio)

    def list_top_sinasc_prenatal_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite DataSUS SINASC prenatal quality score."""
        return self.sinasc_connector.list_top_sinasc_prenatal_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_cmed_presentations_summary(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal ANVISA CMED registered drug presentations inventory and generics share."""
        return self.cmed_connector.get_presentations_summary(id_municipio)

    def list_cmed_therapeutic_class_prices(
        self, id_municipio: str, classe_terapeutica_atc: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal ANVISA CMED regulated PF, PMC and PMVG prices by ATC therapeutic class."""
        return self.cmed_connector.list_therapeutic_class_prices(
            id_municipio, classe_terapeutica_atc=classe_terapeutica_atc
        )

    def get_cmed_public_procurement_pmvg(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal CAP coefficient coverage, PMVG price ceilings, and SUS procurement compliance."""
        return self.cmed_connector.get_public_procurement_pmvg(id_municipio)

    def get_cmed_tax_regime_piscofins(self, id_municipio: str) -> dict[str, Any]:
        """Get state modal ICMS rate, PIS/COFINS positive/negative lists, and active commercialization rate."""
        return self.cmed_connector.get_tax_regime_piscofins(id_municipio)

    def get_cmed_drug_pricing_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold ANVISA CMED generics competitiveness and PMVG/CAP regulatory efficiency features."""
        return self.cmed_connector.get_cmed_drug_pricing_features(id_municipio)

    def list_top_cmed_regulatory_efficiency_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top pharmaceutical hub municipalities ranked by ANVISA CMED PMVG/CAP regulatory efficiency score."""
        return self.cmed_connector.list_top_cmed_regulatory_efficiency_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_vigilancia_srag_respiratory_surveillance(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get OpenDataSUS SRAG severe acute respiratory syndrome surveillance summary for a municipality."""
        return self.vigilancia_connector.get_vigilancia_srag_respiratory_surveillance(
            id_municipio
        )

    def list_vigilancia_sinan_notifiable_diseases(
        self, id_municipio: str, grupo_agravo: str | None = None
    ) -> list[dict[str, Any]]:
        """List SINAN notifiable diseases and arboviruses records for a municipality."""
        return self.vigilancia_connector.list_vigilancia_sinan_notifiable_diseases(
            id_municipio, grupo_agravo=grupo_agravo
        )

    def get_vigilancia_sipni_vaccination_coverage(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get SI-PNI / RNDS immunization coverage metrics for a municipality."""
        return self.vigilancia_connector.get_vigilancia_sipni_vaccination_coverage(
            id_municipio
        )

    def get_vigilancia_cievs_response_capacity(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get CIEVS and LACEN epidemiological laboratory surveillance capacity metrics for a municipality."""
        return self.vigilancia_connector.get_vigilancia_cievs_response_capacity(
            id_municipio
        )

    def get_vigilancia_epidemiological_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get consolidated OpenDataSUS epidemiological and immunization ML features for a municipality."""
        return self.vigilancia_connector.get_vigilancia_epidemiological_features(
            id_municipio
        )

    def list_top_vigilancia_surveillance_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by OpenDataSUS epidemiological surveillance and immunization resilience score."""
        return self.vigilancia_connector.list_top_vigilancia_surveillance_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def ans_connector(self) -> Any:
        if (
            not hasattr(self, "_ans_connector")
            or self._ans_connector is None
        ):
            from public_data_gateway.connectors.saude.ans_saude_suplementar import (
                AnsSaudeSuplementarConnector,
            )

            self._ans_connector = AnsSaudeSuplementarConnector(
                settings=self._sdk.settings
            )
        return self._ans_connector

    def get_ans_beneficiaries(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANS private health plan beneficiaries by code, care line, IBGE, UF, or city."""
        return self.ans_connector.get_beneficiaries(termo)

    def get_ans_operator_quality(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANS operators IDSS quality score, average ticket, and NIP complaints."""
        return self.ans_connector.get_operator_quality_idss(termo)

    def get_ans_diops_loss_ratio(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve ANS DIOPS revenue, medical loss ratio (sinistralidade), and solvency ratio."""
        return self.ans_connector.get_diops_loss_ratio(id_municipio_or_name)

    def get_ans_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal ANS private beneficiaries, coverage, IDSS, and loss ratio."""
        return self.ans_connector.get_ans_summary(id_municipio_or_name)

    def get_ans_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold ANS supplementary health market maturity score and tier."""
        return self.ans_connector.get_ans_features(id_municipio_or_name)

    def list_top_ans_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_beneficiaries: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by ANS supplementary health score or total beneficiaries."""
        return self.ans_connector.list_top_ans_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_beneficiaries=sort_by_beneficiaries
        )

    @property
    def datasus_extra_connector(self) -> Any:
        if (
            not hasattr(self, "_datasus_extra_connector")
            or self._datasus_extra_connector is None
        ):
            from public_data_gateway.connectors.saude.datasus_extra import (
                DatasusExtraConnector,
            )

            self._datasus_extra_connector = DatasusExtraConnector(
                settings=self._sdk.settings
            )
        return self._datasus_extra_connector

    def get_datasus_sia_production(self, termo: str) -> list[dict[str, Any]]:
        """Lookup DATASUS SIA outpatient production by code, procedure, disease, IBGE, UF, or city."""
        return self.datasus_extra_connector.get_datasus_sia_production(termo)

    def get_datasus_sinan_notifications(self, termo: str) -> list[dict[str, Any]]:
        """Lookup DATASUS SINAN notifiable disease counts and timeliness rate."""
        return self.datasus_extra_connector.get_datasus_sinan_notifications(termo)

    def get_datasus_sisvan_nutrition(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal SISVAN/PNS nutritional surveillance and chronic condition coverage."""
        return self.datasus_extra_connector.get_datasus_sisvan_nutrition(
            id_municipio_or_name
        )

    def get_datasus_extra_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal DATASUS SIA volume, SINAN notifications, and SISVAN coverage."""
        return self.datasus_extra_connector.get_datasus_extra_summary(
            id_municipio_or_name
        )

    def get_datasus_extra_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold DATASUS Extra SUS ambulatory resolutivity score and tier."""
        return self.datasus_extra_connector.get_datasus_extra_features(
            id_municipio_or_name
        )

    def list_top_datasus_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_sia: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SUS ambulatory resolutivity score or SIA production volume."""
        return self.datasus_extra_connector.list_top_datasus_extra_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_sia=sort_by_sia
        )


class EducationFacade:
    """Unified developer facade for basic education, school infrastructure, and INEP Censo Escolar analytics."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._censo_escolar_connector: Any = None
        self._censo_superior_connector: Any = None
        self._ideb_connector: Any = None
        self._enem_connector: Any = None
        self._sucupira_connector: Any = None

    @property
    def censo_escolar_connector(self) -> Any:
        if self._censo_escolar_connector is None:
            from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
                INEPCensoEscolarConnector,
            )

            self._censo_escolar_connector = INEPCensoEscolarConnector(
                settings=self._sdk.settings
            )
        return self._censo_escolar_connector

    @property
    def censo_superior_connector(self) -> Any:
        if self._censo_superior_connector is None:
            from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
                INEPCensoSuperiorConnector,
            )

            self._censo_superior_connector = INEPCensoSuperiorConnector(
                settings=self._sdk.settings
            )
        return self._censo_superior_connector

    @property
    def ideb_connector(self) -> Any:
        if self._ideb_connector is None:
            from public_data_gateway.connectors.educacao.inep_ideb.connector import (
                INEPIdebConnector,
            )

            self._ideb_connector = INEPIdebConnector(settings=self._sdk.settings)
        return self._ideb_connector

    @property
    def enem_connector(self) -> Any:
        if self._enem_connector is None:
            from public_data_gateway.connectors.educacao.inep_enem.connector import (
                INEPEnemConnector,
            )

            self._enem_connector = INEPEnemConnector(settings=self._sdk.settings)
        return self._enem_connector

    @property
    def sucupira_connector(self) -> Any:
        if self._sucupira_connector is None:
            from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
                CapesSucupiraConnector,
            )

            self._sucupira_connector = CapesSucupiraConnector(
                settings=self._sdk.settings
            )
        return self._sucupira_connector

    def get_censo_escolar_infrastructure(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal active schools inventory, broadband internet, labs, and PcD accessibility (INEP)."""
        return self.censo_escolar_connector.get_school_infrastructure(id_municipio)

    def list_censo_escolar_enrollments(
        self, id_municipio: str, etapa_ensino: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal basic education enrollments broken down by educational stage (INEP)."""
        return self.censo_escolar_connector.list_enrollments_by_stage(
            id_municipio, etapa_ensino=etapa_ensino
        )

    def get_censo_escolar_teachers(self, id_municipio: str) -> dict[str, Any]:
        """Get teaching workforce qualification, higher-ed share, and INEP AFD indicator per municipality."""
        return self.censo_escolar_connector.get_teacher_qualifications(id_municipio)

    def get_censo_escolar_yield_flow(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal approval, retention/dropout, full-time enrollment, and age-grade distortion rates (INEP)."""
        return self.censo_escolar_connector.get_yield_flow_indicators(id_municipio)

    def get_censo_escolar_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold INEP Censo Escolar school infrastructure and educational flow quality features."""
        return self.censo_escolar_connector.get_censo_escolar_features(id_municipio)

    def list_top_censo_escolar_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite INEP school infrastructure score."""
        return self.censo_escolar_connector.list_top_censo_escolar_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_censo_superior_institutions(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal higher education institutions (IES) profile from INEP Censo da Educação Superior."""
        return self.censo_superior_connector.get_institutions_summary(id_municipio)

    def list_censo_superior_cine_courses(
        self, id_municipio: str, area_cine_rotulo: str | None = None
    ) -> list[dict[str, Any]]:
        """List degree courses and CINE areas by municipality from INEP Censo da Educação Superior."""
        return self.censo_superior_connector.list_cine_area_courses(
            id_municipio=id_municipio, area_cine_rotulo=area_cine_rotulo
        )

    def get_censo_superior_enrollments_graduates(self, id_municipio: str) -> dict[str, Any]:
        """Get higher education enrollments, entrants, graduates, and dropout/retention metrics."""
        return self.censo_superior_connector.get_enrollments_and_graduates(id_municipio)

    def get_censo_superior_faculty_profile(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal higher education faculty qualifications (PhD/Master/DE) profile."""
        return self.censo_superior_connector.get_faculty_profile(id_municipio)

    def get_censo_superior_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold INEP Censo da Educação Superior university hub and academic features."""
        return self.censo_superior_connector.get_censo_superior_features(id_municipio)

    def list_top_censo_superior_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite INEP higher education excellence score."""
        return self.censo_superior_connector.list_top_censo_superior_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_ideb_municipal_results(self, id_municipio: str) -> dict[str, Any]:
        """Get consolidated municipal IDEB scores, P flow indicator, SAEB N score, and INEP goal fulfillment."""
        return self.ideb_connector.get_ideb_municipal_results(id_municipio)

    def list_ideb_stage_network_scores(
        self, id_municipio: str, etapa_ciclo: str | None = None
    ) -> list[dict[str, Any]]:
        """List observed IDEB vs. INEP projected goal by educational stage and network."""
        return self.ideb_connector.list_ideb_stage_network_scores(
            id_municipio=id_municipio, etapa_ciclo=etapa_ciclo
        )

    def get_ideb_saeb_proficiency(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal SAEB Portuguese/Mathematics proficiencies and adequate learning rates."""
        return self.ideb_connector.get_ideb_saeb_proficiency(id_municipio)

    def get_ideb_biennial_goals_evolution(self, id_municipio: str) -> dict[str, Any]:
        """Get biennial IDEB evolution vs. 2021, goal gaps, and socioeconomic INSE equity ratio."""
        return self.ideb_connector.get_ideb_biennial_goals_evolution(id_municipio)

    def get_ideb_education_quality_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold INEP IDEB education performance and SAEB proficiency/equity features."""
        return self.ideb_connector.get_ideb_education_quality_features(id_municipio)

    def list_top_ideb_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite INEP IDEB educational quality score."""
        return self.ideb_connector.list_top_ideb_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_enem_municipal_area_scores(self, id_municipio: str) -> dict[str, Any]:
        """Get official municipal INEP ENEM mean TRI scores across all four knowledge areas and 2-day attendance."""
        return self.enem_connector.get_enem_municipal_area_scores(id_municipio)

    def list_enem_scores_by_school_network(
        self, id_municipio: str, dependencia_rede: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal INEP ENEM scores disaggregated by school administrative network."""
        return self.enem_connector.list_enem_scores_by_school_network(
            id_municipio=id_municipio, dependencia_rede=dependencia_rede
        )

    def get_enem_essay_competencies(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal INEP ENEM Essay mean score, Competencies I/III/V, and 900+ excellence share."""
        return self.enem_connector.get_enem_essay_competencies(id_municipio)

    def get_enem_socioeconomic_equity(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal INEP ENEM QSE socioeconomic equity, Q025 internet, and SiSU/ProUni eligibility."""
        return self.enem_connector.get_enem_socioeconomic_equity(id_municipio)

    def get_enem_performance_equity_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get normalized Gold INEP ENEM cognitive performance and higher-education equity features."""
        return self.enem_connector.get_enem_performance_equity_features(id_municipio)

    def list_top_enem_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite INEP ENEM performance and equity score."""
        return self.enem_connector.list_top_enem_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sucupira_program_evaluation(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal CAPES Sucupira stricto sensu programs count, excellence 6-7 share, and mean grade."""
        return self.sucupira_connector.get_sucupira_program_evaluation(id_municipio)

    def list_sucupira_students_by_area(
        self, id_municipio: str, grande_area_conhecimento: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal CAPES Sucupira master's/doctoral students, graduates, and scholarships by field."""
        return self.sucupira_connector.list_sucupira_students_by_area(
            id_municipio=id_municipio,
            grande_area_conhecimento=grande_area_conhecimento,
        )

    def get_sucupira_faculty_research(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal CAPES Sucupira permanent faculty, CNPq PQ fellows, and active research lines."""
        return self.sucupira_connector.get_sucupira_faculty_research(id_municipio)

    def get_sucupira_scientific_production(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal CAPES Sucupira Qualis A1/A2 papers, theses, patents/software, and estimated grants."""
        return self.sucupira_connector.get_sucupira_scientific_production(id_municipio)

    def get_sucupira_postgrad_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold CAPES Sucupira postgraduate density, scientific production, and tier features."""
        return self.sucupira_connector.get_sucupira_postgrad_features(id_municipio)

    def list_top_sucupira_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite CAPES Sucupira postgraduate ecosystem score."""
        return self.sucupira_connector.list_top_sucupira_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )


class RealEstateFacade:
    """Electronic real estate registry, property transactions, credit guarantees & SERP efficiency (ONR/SREI)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._onr_connector: Any = None
        self._iptu_itbi_connector: Any = None

    @property
    def iptu_itbi_connector(self) -> Any:
        if self._iptu_itbi_connector is None:
            from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
                CapitaisIptuItbiConnector,
            )

            self._iptu_itbi_connector = CapitaisIptuItbiConnector(settings=self._sdk.settings)
        return self._iptu_itbi_connector

    @property
    def onr_connector(self) -> Any:
        if self._onr_connector is None:
            from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
                ONRRegistroImoveisConnector,
            )

            self._onr_connector = ONRRegistroImoveisConnector(settings=self._sdk.settings)
        return self._onr_connector

    def get_onr_property_registry(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal active CNM property registry stock and SREI digital certificate emissions (ONR)."""
        return self.onr_connector.get_onr_property_registry(id_municipio)

    def list_onr_transactions_by_nature(
        self, id_municipio: str, natureza_ato: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal real estate acts and declared financial volume by legal nature (ONR)."""
        return self.onr_connector.list_onr_transactions_by_nature(
            id_municipio=id_municipio, natureza_ato=natureza_ato
        )

    def get_onr_real_estate_credit_guarantees(self, id_municipio: str) -> dict[str, Any]:
        """Get fiduciary lien credit volume, Home Equity CGI contracts, and foreclosure rate (ONR)."""
        return self.onr_connector.get_onr_real_estate_credit_guarantees(id_municipio)

    def get_onr_serp_interoperability(self, id_municipio: str) -> dict[str, Any]:
        """Get SERP/SAEC interoperability, nato-digital titles, SLA, and CNIB/Penhora orders (ONR)."""
        return self.onr_connector.get_onr_serp_interoperability(id_municipio)

    def get_onr_real_estate_features(self, id_municipio: str) -> dict[str, Any]:
        """Get Gold ONR/SREI digital maturity score, real estate liquidity score, and tier features."""
        return self.onr_connector.get_onr_real_estate_features(id_municipio)

    def list_top_onr_real_estate_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by ONR SREI/SERP digital maturity and liquidity."""
        return self.onr_connector.list_top_onr_real_estate_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_iptu_itbi_property_cadastre(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal IPTU property cadastre, PGV venal value, revenue, and compliance."""
        return self.iptu_itbi_connector.get_iptu_itbi_property_cadastre(id_municipio)

    def list_itbi_transactions_by_segment(
        self, id_municipio: str, segmento_imovel: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal ITBI real estate transactions by property segment or usage category."""
        return self.iptu_itbi_connector.list_itbi_transactions_by_segment(
            id_municipio=id_municipio, segmento_imovel=segmento_imovel
        )

    def get_iptu_itbi_price_dynamics(self, id_municipio: str) -> dict[str, Any]:
        """Get ITBI declared price per m², annual growth, PGV-to-market ratio, and liquidity."""
        return self.iptu_itbi_connector.get_iptu_itbi_price_dynamics(id_municipio)

    def get_iptu_itbi_fiscal_governance(self, id_municipio: str) -> dict[str, Any]:
        """Get CTM georeferenced coverage, digital ITBI, and SINTER/CNO/ONR interoperability."""
        return self.iptu_itbi_connector.get_iptu_itbi_fiscal_governance(id_municipio)

    def get_iptu_itbi_real_estate_features(self, id_municipio: str) -> dict[str, Any]:
        """Get Gold IPTU/ITBI market dynamism score, fiscal governance score, and tier features."""
        return self.iptu_itbi_connector.get_iptu_itbi_real_estate_features(id_municipio)

    def list_top_iptu_itbi_capitals(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian capitals ranked by ITBI real estate dynamism and CTM governance."""
        return self.iptu_itbi_connector.list_top_iptu_itbi_capitals(
            limit=limit, sigla_uf=sigla_uf
        )


class EnvironmentFacade:
    """Environmental monitoring, deforestation alerts, hydro telemetry & ESG/EUDR compliance (INPE TerraBrasilis & ANA HidroWeb)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._terrabrasilis_connector: Any = None
        self._hidroweb_connector: Any = None
        self._mapbiomas_connector: Any = None
        self._sfb_sicar_connector: Any = None
        self._mapa_zarc_connector: Any = None

    @property
    def terrabrasilis_connector(self) -> Any:
        if self._terrabrasilis_connector is None:
            from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
                InpeTerrabrasilisConnector,
            )

            self._terrabrasilis_connector = InpeTerrabrasilisConnector(
                settings=self._sdk.settings
            )
        return self._terrabrasilis_connector

    @property
    def hidroweb_connector(self) -> Any:
        if self._hidroweb_connector is None:
            from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria.connector import (
                AnaHidrowebConnector,
            )

            self._hidroweb_connector = AnaHidrowebConnector(
                settings=self._sdk.settings
            )
        return self._hidroweb_connector

    def get_terrabrasilis_prodes_deforestation(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get municipal annual PRODES deforestation increment and remaining forest cover (INPE)."""
        return self.terrabrasilis_connector.get_terrabrasilis_prodes_deforestation(
            id_municipio
        )

    def list_terrabrasilis_deter_alerts(
        self, id_municipio: str, classe_alerta_deter: str | None = None
    ) -> list[dict[str, Any]]:
        """List municipal INPE DETER-B alerts broken down by alert class."""
        return self.terrabrasilis_connector.list_terrabrasilis_deter_alerts(
            id_municipio=id_municipio,
            classe_alerta_deter=classe_alerta_deter,
        )

    def get_terrabrasilis_land_cover(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal native vegetation, agricultural land use, and Forest Code compliance."""
        return self.terrabrasilis_connector.get_terrabrasilis_land_cover(id_municipio)

    def get_terrabrasilis_esg_governance(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal IBAMA embargoes, CAR regularization, and EUDR/ESG rural credit eligibility."""
        return self.terrabrasilis_connector.get_terrabrasilis_esg_governance(
            id_municipio
        )

    def get_terrabrasilis_environmental_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Get normalized Gold INPE TerraBrasilis deforestation pressure and ESG/EUDR risk features."""
        return self.terrabrasilis_connector.get_terrabrasilis_environmental_features(
            id_municipio
        )

    def list_top_terrabrasilis_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite INPE TerraBrasilis ESG compliance score."""
        return self.terrabrasilis_connector.list_top_terrabrasilis_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_hidroweb_stations(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal ANA HidroWeb telemetric station network coverage and availability."""
        return self.hidroweb_connector.get_hidroweb_stations(id_municipio)

    def list_hidroweb_flows(
        self,
        id_municipio: str,
        tipo_monitoramento_hidrologico: str | None = None,
    ) -> list[dict[str, Any]]:
        """List municipal ANA HidroWeb river levels and flows by hydrological segment."""
        return self.hidroweb_connector.list_hidroweb_flows(
            id_municipio=id_municipio,
            tipo_monitoramento_hidrologico=tipo_monitoramento_hidrologico,
        )

    def get_hidroweb_rainfall(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal ANA HidroWeb rainfall accumulation, anomalies, and flood/drought alerts."""
        return self.hidroweb_connector.get_hidroweb_rainfall(id_municipio)

    def get_hidroweb_water_security(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal ANA HidroWeb water quality (IQA), security (ISH), and grant regularization."""
        return self.hidroweb_connector.get_hidroweb_water_security(id_municipio)

    def get_hidroweb_hydrology_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold ANA HidroWeb hydrology features and 4-tier water security classification."""
        return self.hidroweb_connector.get_hidroweb_hydrology_features(id_municipio)

    def list_top_hidroweb_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite ANA HidroWeb water security score."""
        return self.hidroweb_connector.list_top_hidroweb_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def mapbiomas_connector(self) -> Any:
        if self._mapbiomas_connector is None:
            from public_data_gateway.connectors.ambiental.mapbiomas_cobertura.connector import (
                MapBiomasCoberturaConnector,
            )

            self._mapbiomas_connector = MapBiomasCoberturaConnector(
                settings=self._sdk.settings
            )
        return self._mapbiomas_connector

    def get_mapbiomas_land_cover(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal MapBiomas annual native vegetation, agricultural area, and water surface."""
        return self.mapbiomas_connector.get_mapbiomas_land_cover(id_municipio)

    def list_mapbiomas_transitions(
        self,
        id_municipio: str,
        classe_transicao_mapbiomas: str | None = None,
    ) -> list[dict[str, Any]]:
        """List municipal MapBiomas land-use transition matrix by transition class."""
        return self.mapbiomas_connector.list_mapbiomas_transitions(
            id_municipio=id_municipio,
            classe_transicao_mapbiomas=classe_transicao_mapbiomas,
        )

    def get_mapbiomas_agriculture(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal MapBiomas pastures, crops, degradation, and irrigated area."""
        return self.mapbiomas_connector.get_mapbiomas_agriculture(id_municipio)

    def get_mapbiomas_alerts(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal MapBiomas Alerta validated deforestation alerts and overlaps."""
        return self.mapbiomas_connector.get_mapbiomas_alerts(id_municipio)

    def get_mapbiomas_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold MapBiomas land-cover sustainability score and 4-tier classification."""
        return self.mapbiomas_connector.get_mapbiomas_features(id_municipio)

    def list_top_mapbiomas_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite MapBiomas sustainability score."""
        return self.mapbiomas_connector.list_top_mapbiomas_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def sfb_sicar_connector(self) -> Any:
        if self._sfb_sicar_connector is None:
            from public_data_gateway.connectors.ambiental.sfb_sicar.connector import (
                SfbSicarConnector,
            )

            self._sfb_sicar_connector = SfbSicarConnector(
                settings=self._sdk.settings
            )
        return self._sfb_sicar_connector

    def get_sicar_properties(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal SFB/SICAR rural property registry, area coverage, and IBAMA embargo metrics."""
        return self.sfb_sicar_connector.get_sicar_properties(id_municipio)

    def list_sicar_legal_reserve(
        self,
        id_municipio: str,
        categoria_imovel_car: str | None = None,
    ) -> list[dict[str, Any]]:
        """List municipal SFB/SICAR Legal Reserve and APP compliance by property size category."""
        return self.sfb_sicar_connector.list_sicar_legal_reserve(
            id_municipio,
            categoria_imovel_car=categoria_imovel_car,
        )

    def get_sicar_pra_compliance(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal SFB/SICAR Environmental Regularization Program (PRA) compliance metrics."""
        return self.sfb_sicar_connector.get_sicar_pra_compliance(id_municipio)

    def get_sicar_geometry(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal SFB/SICAR geospatial overlap, precision, and SIGEF integration metrics."""
        return self.sfb_sicar_connector.get_sicar_geometry(id_municipio)

    def get_sicar_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold SFB/SICAR rural environmental compliance score and 4-tier classification."""
        return self.sfb_sicar_connector.get_sicar_features(id_municipio)

    def list_top_sicar_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite SFB/SICAR compliance score."""
        return self.sfb_sicar_connector.list_top_sicar_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def mapa_zarc_connector(self) -> Any:
        if self._mapa_zarc_connector is None:
            from public_data_gateway.connectors.ambiental.mapa_zarc.connector import (
                MapaZarcConnector,
            )

            self._mapa_zarc_connector = MapaZarcConnector(
                settings=self._sdk.settings
            )
        return self._mapa_zarc_connector

    def get_zarc_climate_aptitude(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal MAPA ZARC agro-climatic aptitude and suitable planting decendios."""
        return self.mapa_zarc_connector.get_zarc_climate_aptitude(id_municipio)

    def list_zarc_planting_windows(
        self,
        id_municipio: str,
        cultura_zarc: str | None = None,
    ) -> list[dict[str, Any]]:
        """List municipal MAPA ZARC decendial planting windows by crop and AD soil group."""
        return self.mapa_zarc_connector.list_zarc_planting_windows(
            id_municipio,
            cultura_zarc=cultura_zarc,
        )

    def get_zarc_proagro_coverage(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal MAPA ZARC PROAGRO and Rural Insurance (PSR) coverage metrics."""
        return self.mapa_zarc_connector.get_zarc_proagro_coverage(id_municipio)

    def get_zarc_soil_risk(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal MAPA ZARC soil water retention (AD1-AD6) and no-till resilience."""
        return self.mapa_zarc_connector.get_zarc_soil_risk(id_municipio)

    def get_zarc_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold MAPA ZARC agro-climatic risk score and 4-tier classification."""
        return self.mapa_zarc_connector.get_zarc_features(id_municipio)

    def list_top_zarc_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite MAPA ZARC aptitude score."""
        return self.mapa_zarc_connector.list_top_zarc_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def ibama_connector(self) -> Any:
        if not hasattr(self, "_ibama_connector") or self._ibama_connector is None:
            from public_data_gateway.connectors.ambiental.ibama_fiscalizacao import (
                IbamaFiscalizacaoConnector,
            )

            self._ibama_connector = IbamaFiscalizacaoConnector(
                settings=self._sdk.settings
            )
        return self._ibama_connector

    def get_ibama_infractions(self, termo: str) -> list[dict[str, Any]]:
        """Lookup IBAMA environmental infraction notices by AI number, CNPJ/CPF, IBGE, UF, or name."""
        return self.ibama_connector.get_infractions(termo)

    def get_ibama_embargoes(self, termo: str) -> list[dict[str, Any]]:
        """Lookup IBAMA environmental embargo orders by TAD/AI number, CNPJ/CPF, IBGE, UF, or biome."""
        return self.ibama_connector.get_embargoes(termo)

    def get_ibama_fines_judgment(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal IBAMA fine judgment and PGFN debt recovery summary."""
        return self.ibama_connector.get_fines_judgment(id_municipio_or_name)

    def get_ibama_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal IBAMA SIFISC infraction and embargo totals."""
        return self.ibama_connector.get_municipal_summary(id_municipio_or_name)

    def get_ibama_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold IBAMA environmental enforcement compliance and EUDR risk features."""
        return self.ibama_connector.get_ibama_features(id_municipio_or_name)

    def list_top_ibama_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_risk: bool = False,
    ) -> list[dict[str, Any]]:
        """List Brazilian municipalities ranked by IBAMA compliance score or embargo pressure."""
        return self.ibama_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_risk=sort_by_risk
        )

    @property
    def bcb_sicor_connector(self) -> Any:
        if not hasattr(self, "_bcb_sicor_connector") or self._bcb_sicor_connector is None:
            from public_data_gateway.connectors.ambiental.bcb_sicor import (
                BcbSicorConnector,
            )

            self._bcb_sicor_connector = BcbSicorConnector(
                settings=self._sdk.settings
            )
        return self._bcb_sicor_connector

    def get_sicor_custeio(self, termo: str) -> list[dict[str, Any]]:
        """Lookup BACEN SICOR operating rural credit (custeio) by IBGE, UF, crop, or program."""
        return self.bcb_sicor_connector.get_custeio(termo)

    def get_sicor_investimento(self, termo: str) -> list[dict[str, Any]]:
        """Lookup BACEN SICOR rural investment lines (RenovAgro ABC+, Pronaf Mais Alimentos)."""
        return self.bcb_sicor_connector.get_investimento(termo)

    def get_sicor_pronaf_proagro(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal Pronaf share, Proagro coverage, RenovAgro ABC+, and Res. BCB 140 rate."""
        return self.bcb_sicor_connector.get_pronaf_proagro(id_municipio_or_name)

    def get_sicor_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal BACEN SICOR contracts and rural credit volumes."""
        return self.bcb_sicor_connector.get_municipal_summary(id_municipio_or_name)

    def get_sicor_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold BACEN SICOR rural credit dynamism and ESG sustainability features."""
        return self.bcb_sicor_connector.get_sicor_features(id_municipio_or_name)

    def list_top_sicor_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_pronaf: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold BACEN SICOR score or Pronaf smallholder share."""
        return self.bcb_sicor_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_pronaf=sort_by_pronaf
        )

    @property
    def ibge_agro_connector(self) -> Any:
        if not hasattr(self, "_ibge_agro_connector") or self._ibge_agro_connector is None:
            from public_data_gateway.connectors.ambiental.ibge_agro_municipal import (
                IbgeAgroMunicipalConnector,
            )

            self._ibge_agro_connector = IbgeAgroMunicipalConnector(
                settings=self._sdk.settings
            )
        return self._ibge_agro_connector

    def get_agro_pam_crops(self, termo: str) -> list[dict[str, Any]]:
        """Lookup IBGE PAM agricultural production by IBGE 7D, UF, crop, or municipality."""
        return self.ibge_agro_connector.get_pam_crops(termo)

    def get_agro_ppm_livestock(self, termo: str) -> list[dict[str, Any]]:
        """Lookup IBGE PPM livestock herd heads and milk production by IBGE, UF, or species."""
        return self.ibge_agro_connector.get_ppm_livestock(termo)

    def get_agro_pevs_forestry(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal IBGE PEVS planted forest area, timber m³, and forestry value."""
        return self.ibge_agro_connector.get_pevs_forestry(id_municipio_or_name)

    def get_agro_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal IBGE PAM/PPM/PEVS agricultural production value."""
        return self.ibge_agro_connector.get_municipal_summary(id_municipio_or_name)

    def get_agro_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold IBGE Agro Municipal agricultural power score and vocation features."""
        return self.ibge_agro_connector.get_agro_features(id_municipio_or_name)

    def list_top_agro_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_vbp: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by IBGE agricultural power score or total VBP value."""
        return self.ibge_agro_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_vbp=sort_by_vbp
        )

    @property
    def rfb_cafir_connector(self) -> Any:
        if not hasattr(self, "_rfb_cafir_connector") or self._rfb_cafir_connector is None:
            from public_data_gateway.connectors.ambiental.rfb_cafir import (
                RfbCafirConnector,
            )

            self._rfb_cafir_connector = RfbCafirConnector(
                settings=self._sdk.settings
            )
        return self._rfb_cafir_connector

    def get_cafir_rural_properties(self, termo: str) -> list[dict[str, Any]]:
        """Lookup RFB CAFIR rural properties by NIRF/CIB, SNCR, CNPJ/CPF, IBGE, UF, or name."""
        return self.rfb_cafir_connector.get_rural_properties(termo)

    def get_cafir_cnir_linkage(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal CAFIR-CNIR-SIGEF linkage and integration readiness."""
        return self.rfb_cafir_connector.get_cnir_linkage(id_municipio_or_name)

    def get_cafir_land_concentration(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal agrarian land concentration Gini and fiscal module distribution."""
        return self.rfb_cafir_connector.get_land_concentration(id_municipio_or_name)

    def get_cafir_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal RFB CAFIR rural property counts, area, and VTN."""
        return self.rfb_cafir_connector.get_municipal_summary(id_municipio_or_name)

    def get_cafir_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold RFB CAFIR rural land regularization and CNIR structure features."""
        return self.rfb_cafir_connector.get_cafir_features(id_municipio_or_name)

    def list_top_cafir_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_area: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by Gold CAFIR regularization score or total rural area."""
        return self.rfb_cafir_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_area=sort_by_area
        )

    @property
    def inpe_queimadas_seeg_connector(self) -> Any:
        if (
            not hasattr(self, "_inpe_queimadas_seeg_connector")
            or self._inpe_queimadas_seeg_connector is None
        ):
            from public_data_gateway.connectors.ambiental.inpe_queimadas_seeg import (
                InpeQueimadasSeegConnector,
            )

            self._inpe_queimadas_seeg_connector = InpeQueimadasSeegConnector(
                settings=self._sdk.settings
            )
        return self._inpe_queimadas_seeg_connector

    def get_queimadas_hotspots(self, termo: str) -> list[dict[str, Any]]:
        """Lookup INPE active fire hotspots by code, satellite, biome, IBGE, UF, or name."""
        return self.inpe_queimadas_seeg_connector.get_fire_hotspots(termo)

    def get_seeg_emissions(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SEEG municipal GHG emissions by code, sector, IBGE 7D, UF, or municipality."""
        return self.inpe_queimadas_seeg_connector.get_seeg_emissions(termo)

    def get_queimadas_fire_risk(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal INPE seasonal fire risk, FRP intensity, and burned scar area."""
        return self.inpe_queimadas_seeg_connector.get_fire_risk(id_municipio_or_name)

    def get_queimadas_seeg_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal INPE hotspots count and SEEG net GHG emissions."""
        return self.inpe_queimadas_seeg_connector.get_municipal_summary(
            id_municipio_or_name
        )

    def get_queimadas_seeg_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold INPE Queimadas & SEEG GHG climate resilience and ESG risk features."""
        return self.inpe_queimadas_seeg_connector.get_queimadas_seeg_features(
            id_municipio_or_name
        )

    def list_top_queimadas_seeg_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_emissions: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by climate resilience score or highest GHG emissions."""
        return self.inpe_queimadas_seeg_connector.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_emissions=sort_by_emissions
        )

    @property
    def aneel_gd_connector(self) -> Any:
        if (
            not hasattr(self, "_aneel_gd_connector")
            or self._aneel_gd_connector is None
        ):
            from public_data_gateway.connectors.ambiental.aneel_geracao_distribuida import (
                AneelGeracaoDistribuidaConnector,
            )

            self._aneel_gd_connector = AneelGeracaoDistribuidaConnector(
                settings=self._sdk.settings
            )
        return self._aneel_gd_connector

    def get_mmgd_plants(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANEEL MMGD plants by code, generation type, class, utility, IBGE, or UF."""
        return self.aneel_gd_connector.get_mmgd_plants(termo)

    def get_mmgd_sources_by_municipality(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANEEL MMGD installed capacity by renewable source, IBGE 7D, UF, or city."""
        return self.aneel_gd_connector.get_mmgd_sources_by_municipality(termo)

    def get_mmgd_utility_grid(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal distribution utility SLA and reverse-flow metrics."""
        return self.aneel_gd_connector.get_mmgd_utility_grid(id_municipio_or_name)

    def get_mmgd_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal ANEEL MMGD installed kW and rural solar share."""
        return self.aneel_gd_connector.get_mmgd_municipal_summary(
            id_municipio_or_name
        )

    def get_mmgd_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold ANEEL MMGD energy transition score and solar maturity tier."""
        return self.aneel_gd_connector.get_mmgd_features(id_municipio_or_name)

    def list_top_mmgd_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_power: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by MMGD energy transition score or total kW."""
        return self.aneel_gd_connector.list_top_mmgd_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_power=sort_by_power
        )

    @property
    def anp_combustiveis_connector(self) -> Any:
        if (
            not hasattr(self, "_anp_combustiveis_connector")
            or self._anp_combustiveis_connector is None
        ):
            from public_data_gateway.connectors.ambiental.anp_combustiveis import (
                AnpCombustiveisConnector,
            )

            self._anp_combustiveis_connector = AnpCombustiveisConnector(
                settings=self._sdk.settings
            )
        return self._anp_combustiveis_connector

    def get_anp_fuel_prices(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANP weekly fuel prices and margins by product, code, IBGE, UF, or city."""
        return self.anp_combustiveis_connector.get_anp_fuel_prices(termo)

    def get_anp_fuel_sales(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ANP biofuel and petroleum derivative sales volumes by code, category, or UF."""
        return self.anp_combustiveis_connector.get_anp_fuel_sales(termo)

    def get_anp_retail_network(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal ANP gas stations network and PMQC fuel quality compliance."""
        return self.anp_combustiveis_connector.get_anp_retail_network(
            id_municipio_or_name
        )

    def get_anp_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal ANP prices, ethanol parity ratio, and biofuel share."""
        return self.anp_combustiveis_connector.get_anp_municipal_summary(
            id_municipio_or_name
        )

    def get_anp_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve Gold ANP Combustiveis competitiveness score and fuel matrix tier."""
        return self.anp_combustiveis_connector.get_anp_features(id_municipio_or_name)

    def list_top_anp_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_biofuel: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by ANP competitiveness score or biofuel share (%)."""
        return self.anp_combustiveis_connector.list_top_anp_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_biofuel=sort_by_biofuel
        )

    @property
    def mdr_snis_connector(self) -> Any:
        if (
            not hasattr(self, "_mdr_snis_connector")
            or self._mdr_snis_connector is None
        ):
            from public_data_gateway.connectors.ambiental.mdr_snis import (
                MdrSnisConnector,
            )

            self._mdr_snis_connector = MdrSnisConnector(settings=self._sdk.settings)
        return self._mdr_snis_connector

    def get_snis_water_sewage(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SNIS/SINISA water and sewage indicators by code, utility, IBGE, UF, or city."""
        return self.mdr_snis_connector.get_snis_water_sewage(termo)

    def get_snis_solid_waste(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SNIS/SINISA municipal solid waste, selective collection, and recycling metrics."""
        return self.mdr_snis_connector.get_snis_solid_waste(termo)

    def get_snis_financial_investments(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal SNIS average tariffs, cash sufficiency, and capex per capita."""
        return self.mdr_snis_connector.get_snis_financial_investments(
            id_municipio_or_name
        )

    def get_snis_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SNIS water, sewage, losses, and capex summary."""
        return self.mdr_snis_connector.get_snis_municipal_summary(
            id_municipio_or_name
        )

    def get_snis_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SNIS/SINISA 2033 sanitation universalization score and readiness tier."""
        return self.mdr_snis_connector.get_snis_features(id_municipio_or_name)

    def list_top_snis_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_sewage: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SNIS sanitation universalization score or sewage coverage."""
        return self.mdr_snis_connector.list_top_snis_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_sewage=sort_by_sewage
        )

    @property
    def sedec_desastres_connector(self) -> Any:
        if (
            not hasattr(self, "_sedec_desastres_connector")
            or self._sedec_desastres_connector is None
        ):
            from public_data_gateway.connectors.ambiental.sedec_desastres import (
                SedecDesastresConnector,
            )

            self._sedec_desastres_connector = SedecDesastresConnector(
                settings=self._sdk.settings
            )
        return self._sedec_desastres_connector

    def get_sedec_disasters(self, termo: str) -> list[dict[str, Any]]:
        """Lookup S2ID/COBRADE natural disaster occurrences by code, typology, IBGE, UF, or city."""
        return self.sedec_desastres_connector.get_sedec_disasters(termo)

    def get_sedec_decrees(self, termo: str) -> list[dict[str, Any]]:
        """Lookup federal emergency (SE) and calamity (ECP) decrees and funds by code, IBGE, UF, or city."""
        return self.sedec_desastres_connector.get_sedec_decrees(termo)

    def get_sedec_cemaden_monitoring(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal CEMADEN pluviometer network, risk-area exposure, and alerts."""
        return self.sedec_desastres_connector.get_sedec_cemaden_monitoring(
            id_municipio_or_name
        )

    def get_sedec_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated 5-year municipal S2ID disaster count, losses, and CEMADEN alerts."""
        return self.sedec_desastres_connector.get_sedec_municipal_summary(
            id_municipio_or_name
        )

    def get_sedec_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold SEDEC/CEMADEN disaster resilience score and vulnerability tier."""
        return self.sedec_desastres_connector.get_sedec_features(id_municipio_or_name)

    def list_top_resilient_sedec_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_damages: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by SEDEC disaster resilience score or highest economic damages."""
        return self.sedec_desastres_connector.list_top_resilient_sedec_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_damages=sort_by_damages
        )

    @property
    def mme_ons_energia_connector(self) -> Any:
        if (
            not hasattr(self, "_mme_ons_energia_connector")
            or self._mme_ons_energia_connector is None
        ):
            from public_data_gateway.connectors.ambiental.mme_ons_energia import (
                MmeOnsEnergiaConnector,
            )

            self._mme_ons_energia_connector = MmeOnsEnergiaConnector(
                settings=self._sdk.settings
            )
        return self._mme_ons_energia_connector

    def get_ons_generation_capacity(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SIN power plants by ONS code, CEG, source, subsystem, IBGE, UF, or city."""
        return self.mme_ons_energia_connector.get_ons_generation_capacity(termo)

    def get_ons_load_balance(self, termo: str) -> list[dict[str, Any]]:
        """Lookup ONS subsystem energy load, stored energy EAR (%), and CMO by code, subsystem, IBGE, UF, or city."""
        return self.mme_ons_energia_connector.get_ons_load_balance(termo)

    def get_mme_grid_security(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve municipal MME/ONS high-voltage substations, DEC continuity, and SIN availability."""
        return self.mme_ons_energia_connector.get_mme_grid_security(
            id_municipio_or_name
        )

    def get_ons_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Retrieve consolidated municipal SIN installed MW, renewable share, and energy load."""
        return self.mme_ons_energia_connector.get_ons_municipal_summary(
            id_municipio_or_name
        )

    def get_ons_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Retrieve Gold MME/ONS energy security score and SIN dispatch tier."""
        return self.mme_ons_energia_connector.get_ons_features(id_municipio_or_name)

    def list_top_ons_energy_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_renewable: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by MME/ONS energy security score or renewable share (%)."""
        return self.mme_ons_energia_connector.list_top_ons_energy_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_renewable=sort_by_renewable
        )

    @property
    def anm_mineracao_connector(self) -> Any:
        if (
            not hasattr(self, "_anm_mineracao_connector")
            or self._anm_mineracao_connector is None
        ):
            from public_data_gateway.connectors.ambiental.anm_mineracao import (
                AnmMineracaoConnector,
            )

            self._anm_mineracao_connector = AnmMineracaoConnector(
                settings=self._sdk.settings
            )
        return self._anm_mineracao_connector

    def get_anm_sigmine_processes(self, termo: str) -> list[dict[str, Any]]:
        """Lookup SIGMINE mining processes by ANM code, substance, phase, IBGE, UF, or city."""
        return self.anm_mineracao_connector.get_anm_sigmine_processes(termo)

    def get_anm_cfem_royalties(self, termo: str) -> list[dict[str, Any]]:
        """Lookup CFEM mineral royalty collections by guide code, substance, IBGE, UF, or city."""
        return self.anm_mineracao_connector.get_anm_cfem_royalties(termo)

    def get_anm_sigbm_barragens(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Get municipal SIGBM mining-dam inventory, PAEBM alerts, and ESG conformity."""
        return self.anm_mineracao_connector.get_anm_sigbm_barragens(
            id_municipio_or_name
        )

    def get_anm_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Get consolidated municipal ANM active titles, titled hectares, and CFEM revenue."""
        return self.anm_mineracao_connector.get_anm_municipal_summary(
            id_municipio_or_name
        )

    def get_anm_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Get Gold ANM mineral ESG maturity score and sustainable mining tier."""
        return self.anm_mineracao_connector.get_anm_features(id_municipio_or_name)

    def list_top_anm_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_cfem: bool = False,
    ) -> list[dict[str, Any]]:
        """List municipalities ranked by ANM mineral ESG maturity score or CFEM royalties."""
        return self.anm_mineracao_connector.list_top_anm_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_cfem=sort_by_cfem
        )


class MetaportalsFacade:
    """Sub-facade for Metaportals & Aggregator Hubs (Datalake Federado SQL, Dados.gov.br CKAN)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._datalake_connector: Any = None
        self._dadosgov_connector: Any = None

    @property
    def datalake_connector(self) -> Any:
        if self._datalake_connector is None:
            from public_data_gateway.connectors.agregadores.datalake_federado_bq.connector import (
                DatalakeFederadoBqConnector,
            )

            self._datalake_connector = DatalakeFederadoBqConnector(
                settings=self._sdk.settings
            )
        return self._datalake_connector

    def get_bd_coverage(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal Federated Public Datalake (DuckDB / BigQuery) treated dataset and table coverage."""
        return self.datalake_connector.get_bd_coverage(id_municipio)

    def list_bd_datasets(
        self,
        id_municipio: str,
        dataset_bd_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List municipal Federated Public Datalake datasets by thematic domain."""
        return self.datalake_connector.list_bd_datasets(
            id_municipio,
            dataset_bd_id=dataset_bd_id,
        )

    def get_bd_governance(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal Federated Public Datalake dictionary standardization and canonical key governance."""
        return self.datalake_connector.get_bd_governance(id_municipio)

    def get_bd_consumption(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal Federated Public Datalake SQL query volume and scan efficiency."""
        return self.datalake_connector.get_bd_consumption(id_municipio)

    def get_bd_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold Federated Public Datalake maturity score and 4-tier classification."""
        return self.datalake_connector.get_bd_features(id_municipio)

    def list_top_bd_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite Federated Public Datalake maturity score."""
        return self.datalake_connector.list_top_bd_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    @property
    def dadosgov_connector(self) -> Any:
        if self._dadosgov_connector is None:
            from public_data_gateway.connectors.agregadores.dadosgov_ckan.connector import (
                DadosGovCkanConnector,
            )

            self._dadosgov_connector = DadosGovCkanConnector(settings=self._sdk.settings)
        return self._dadosgov_connector

    def get_ckan_organizations(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal Dados.gov.br CKAN publishing organizations and open dataset supply."""
        return self.dadosgov_connector.get_ckan_organizations(id_municipio)

    def list_ckan_formats(
        self,
        id_municipio: str,
        formato_recurso_ckan: str | None = None,
    ) -> list[dict[str, Any]]:
        """List municipal Dados.gov.br CKAN resource formats and machine-readability metrics."""
        return self.dadosgov_connector.list_ckan_formats(
            id_municipio,
            formato_recurso_ckan=formato_recurso_ckan,
        )

    def get_ckan_governance(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal Dados.gov.br CKAN open license, metadata dictionary, and LAI governance."""
        return self.dadosgov_connector.get_ckan_governance(id_municipio)

    def get_ckan_engagement(self, id_municipio: str) -> dict[str, Any]:
        """Get municipal Dados.gov.br CKAN resource downloads, API calls, and civic reuse."""
        return self.dadosgov_connector.get_ckan_engagement(id_municipio)

    def get_ckan_features(self, id_municipio: str) -> dict[str, Any]:
        """Get normalized Gold Dados.gov.br CKAN transparency score and 4-tier classification."""
        return self.dadosgov_connector.get_ckan_features(id_municipio)

    def list_top_ckan_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top Brazilian municipalities ranked by composite Dados.gov.br CKAN transparency score."""
        return self.dadosgov_connector.list_top_ckan_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )


class CrossDomainFacade:
    """Sub-facade for Gold Cross-Domain Marts, Consolidated ML Feature Store & Solution Pack Insights."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._service: Any = None

    @property
    def service(self) -> Any:
        if self._service is None:
            from public_data_gateway.core.cross_domain import CrossDomainService

            self._service = CrossDomainService(settings=self._sdk.settings)
        return self._service

    def get_features(self, id_municipio: str) -> dict[str, Any]:
        """Get consolidated ML Feature Store row (with ASOF macro context) for a municipality."""
        return self.service.get_cross_domain_features(id_municipio)

    def list_top_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by composite cross-domain B2B propensity score."""
        return self.service.list_top_cross_domain_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_solution_pack_insights(
        self, pack_id: str, sigla_uf: str | None = None, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get commercial and operational insights for a Solution Pack (P01..P10)."""
        return self.service.get_solution_pack_insights(
            pack_id=pack_id, sigla_uf=sigla_uf, limit=limit
        )

    def get_macro_territory_b2b(self, sigla_uf: str) -> dict[str, Any]:
        """Get cross-domain Territory x B2B x Macro x Public Procurement mart for a state."""
        return self.service.get_macro_territory_b2b(sigla_uf)

    def get_esg_agro_infra(self, id_municipio: str) -> dict[str, Any]:
        """Get cross-domain ESG x Agro x Water Security x 5G Telecom mart for a municipality."""
        return self.service.get_esg_agro_infra(id_municipio)


class LifecycleFacade:
    """Sub-facade for 9-Stage Solution Pack Lifecycle Reports (S01..S09) and Sales Dossiers."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._service: Any = None

    @property
    def service(self) -> Any:
        if self._service is None:
            from public_data_gateway.core.lifecycle_reports import LifecycleReportsService

            self._service = LifecycleReportsService(settings=self._sdk.settings)
        return self._service

    def get_stage_report(
        self,
        stage_id: str,
        pack_id: str,
        sigla_uf: str | None = None,
        id_municipio: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Query a lifecycle stage mart (S01..S09) for a Solution Pack (P01..P10)."""
        return self.service.get_stage_report(
            stage_id=stage_id,
            pack_id=pack_id,
            sigla_uf=sigla_uf,
            id_municipio=id_municipio,
            limit=limit,
        )

    def build_sales_dossier(
        self,
        pack_id: str,
        sigla_uf: str,
        id_municipio: str | None = None,
        limit: int = 5,
    ) -> dict[str, Any]:
        """Build a complete 9-stage commercial and delivery dossier for a Solution Pack."""
        return self.service.build_sales_dossier(
            pack_id=pack_id,
            sigla_uf=sigla_uf,
            id_municipio=id_municipio,
            limit=limit,
        )


class PublicApisHubFacade:
    """Facade exposing Phase 10 Public & Global APIs (KYC/Geo, Market, Spatial ML, Research)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk
        self._service: Any = None

    @property
    def service(self) -> Any:
        if self._service is None:
            from public_data_gateway.connectors.public_apis_hub import PublicApisHubService

            self._service = PublicApisHubService(settings=self._sdk.settings)
        return self._service

    def list_catalog(
        self,
        *,
        block: str | None = None,
        pack_id: str | None = None,
    ) -> dict[str, Any]:
        """List curated Public & Global APIs filtered by block or Solution Pack."""
        return self.service.load_catalog(block=block, pack_id=pack_id)

    def enrich_cnpj_cascade(self, cnpj: str) -> dict[str, Any]:
        """Enrich CNPJ with automatic cascade fallback (Minha Receita -> CNPJa -> BrasilAPI -> ReceitaWS)."""
        return self.service.enrich_cnpj_cascade(cnpj)

    def lookup_cep_cascade(self, cep: str) -> dict[str, Any]:
        """Lookup CEP with automatic cascade fallback (ViaCEP -> OpenCEP -> BrasilAPI)."""
        return self.service.lookup_cep_cascade(cep)

    def count_osm_pois_radius(
        self,
        lat: float,
        lon: float,
        *,
        amenity: str = "school",
        radius_m: int = 1000,
    ) -> dict[str, Any]:
        """Count OpenStreetMap POIs within radius_m around (lat, lon) via Overpass API."""
        return self.service.count_osm_pois_radius(
            lat, lon, amenity=amenity, radius_m=radius_m
        )

    def compute_osrm_route(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        *,
        profile: str = "driving",
    ) -> dict[str, Any]:
        """Compute real road distance (km) and duration (min) via OSRM."""
        return self.service.compute_osrm_route(lat1, lon1, lat2, lon2, profile=profile)

    def fetch_ipeadata_series(self, sercodigo: str, limit: int = 24, *, persist: bool = False) -> list[dict[str, Any]]:
        """Fetch IPEA Data OData v4 series via Medallion connector."""
        from public_data_gateway.connectors.macrofinanceiro.ipeadata_odata import (
            IpeadataOdataConnector,
        )

        conn = IpeadataOdataConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(sercodigo=sercodigo, limit=limit).to_pylist()
        return conn.normalize_records(conn.fetch_series(sercodigo=sercodigo, limit=limit), sercodigo=sercodigo)

    def fetch_brapi_quote(self, tickers: str = "PETR4", *, persist: bool = False) -> list[dict[str, Any]]:
        """Fetch B3 stock/FII quotes via Medallion brapi.dev connector."""
        from public_data_gateway.connectors.macrofinanceiro.brapi_b3 import BrapiB3Connector

        conn = BrapiB3Connector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(tickers=tickers).to_pylist()
        return conn.normalize_records(conn.fetch_quotes(tickers=tickers))

    def fetch_awesomeapi_rates(
        self, pairs: str = "USD-BRL,EUR-BRL,BTC-BRL", *, persist: bool = False
    ) -> list[dict[str, Any]]:
        """Fetch real-time FX & crypto quotes via Medallion AwesomeAPI connector."""
        from public_data_gateway.connectors.macrofinanceiro.awesomeapi_fx import (
            AwesomeApiFxConnector,
        )

        conn = AwesomeApiFxConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(pairs=pairs).to_pylist()
        return conn.normalize_records(conn.fetch_rates(pairs=pairs))

    def query_ckan_datastore(
        self,
        resource_id: str,
        *,
        portal: str = "aneel",
        limit: int = 20,
        persist: bool = False,
    ) -> list[dict[str, Any]]:
        """Query ANEEL/ANP CKAN DataStore via Medallion connector."""
        from public_data_gateway.connectors.agregadores.aneel_anp_ckan import AneelAnpCkanConnector

        conn = AneelAnpCkanConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(portal=portal, resource_id=resource_id, limit=limit).to_pylist()
        return conn.normalize_records(
            conn.fetch_datastore(resource_id=resource_id, portal=portal, limit=limit),
            portal=portal,
            resource_id=resource_id,
        )

    def fetch_camara_deputados(
        self, pagina: int = 1, itens: int = 20, *, persist: bool = False
    ) -> list[dict[str, Any]]:
        """Fetch active federal deputies via Medallion Camara Dados Abertos connector."""
        from public_data_gateway.connectors.compliance.camara_senado_dados_abertos import (
            CamaraSenadoDadosAbertosConnector,
        )

        conn = CamaraSenadoDadosAbertosConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(pagina=pagina, itens=itens).to_pylist()
        return conn.normalize_records(conn.fetch_deputados(pagina=pagina, itens=itens))

    def fetch_world_bank_indicator(
        self,
        country: str = "BRA",
        indicator: str = "NY.GDP.MKTP.KD.ZG",
        per_page: int = 10,
        *,
        persist: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch World Bank v2 indicator series via Medallion connector."""
        from public_data_gateway.connectors.macrofinanceiro.world_bank_v2 import (
            WorldBankV2Connector,
        )

        conn = WorldBankV2Connector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(country=country, indicator=indicator, per_page=per_page).to_pylist()
        return conn.normalize_records(
            conn.fetch_indicator(country=country, indicator=indicator, per_page=per_page),
            country=country,
            indicator=indicator,
        )

    def fetch_who_gho_indicator(
        self,
        indicator: str = "WHOSIS_000001",
        top: int = 10,
        *,
        persist: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch WHO GHO OData health indicator via Medallion connector."""
        from public_data_gateway.connectors.saude.who_gho_odata import WhoGhoOdataConnector

        conn = WhoGhoOdataConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table(indicator=indicator, top=top).to_pylist()
        return conn.normalize_records(conn.fetch_indicator(indicator=indicator, top=top), indicator=indicator)

    def fetch_open_meteo_forecast(
        self,
        lat: float = -23.5505,
        lon: float = -46.6333,
        *,
        persist: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch Open-Meteo daily forecast via Medallion connector."""
        from public_data_gateway.connectors.ambiental.open_meteo import OpenMeteoConnector

        conn = OpenMeteoConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_forecast(lat=lat, lon=lon))

    def fetch_nasa_power_daily(
        self,
        lat: float = -23.5505,
        lon: float = -46.6333,
        *,
        start: str = "20261101",
        end: str = "20261105",
        persist: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch NASA POWER daily agro-climate data via Medallion connector."""
        from public_data_gateway.connectors.ambiental.nasa_power import NasaPowerConnector

        conn = NasaPowerConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_daily(lat=lat, lon=lon, start=start, end=end))

    def fetch_elevation(
        self,
        lat: float = -23.5505,
        lon: float = -46.6333,
        *,
        persist: bool = False,
    ) -> dict[str, Any]:
        """Fetch SRTM terrain elevation via Medallion Open-Elevation connector."""
        from public_data_gateway.connectors.geografico.open_elevation import OpenElevationConnector

        conn = OpenElevationConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()[0]
        return conn.fetch_elevation(lat=lat, lon=lon)

    def run_hf_inference(self, model: str, inputs: str) -> list[dict[str, Any]]:
        """Run Hugging Face Inference via Medallion connector."""
        from public_data_gateway.connectors.agregadores.huggingface_inference import (
            HuggingFaceInferenceConnector,
        )

        conn = HuggingFaceInferenceConnector(settings=self._sdk.settings)
        return conn.normalize_records(conn.run_inference(model=model, inputs=inputs), model_id=model, inputs=inputs)

    def fetch_alpha_vantage_daily(self, symbol: str = "EWZ") -> list[dict[str, Any]]:
        """Fetch Alpha Vantage daily series via Medallion connector."""
        from public_data_gateway.connectors.macrofinanceiro.alpha_vantage import (
            AlphaVantageConnector,
        )

        conn = AlphaVantageConnector(settings=self._sdk.settings)
        return conn.normalize_records(conn.fetch_daily(symbol=symbol), symbol=symbol)

    def fetch_nasa_apod(self, date: str | None = None, *, persist: bool = False) -> list[dict[str, Any]]:
        """Fetch NASA APOD asset metadata via Medallion connector."""
        from public_data_gateway.connectors.agregadores.nasa_youtube_open_ml import (
            NasaYoutubeOpenMlConnector,
        )

        conn = NasaYoutubeOpenMlConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_nasa_apod(date=date))

    def describe_upgini_features(self) -> dict[str, Any]:
        """Describe Upgini public ML feature groups via Medallion connector."""
        from public_data_gateway.connectors.agregadores.upgini_adapter import UpginiAdapterConnector

        return UpginiAdapterConnector(settings=self._sdk.settings).describe_features()

    def geocode_nominatim(
        self, query: str, *, limit: int = 3, persist: bool = False
    ) -> list[dict[str, Any]]:
        """Forward geocode an address using OpenStreetMap Nominatim Medallion connector."""
        from public_data_gateway.connectors.geografico.nominatim_osm import NominatimOsmConnector

        conn = NominatimOsmConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_geocode(query=query, limit=limit))

    def lookup_rdap_registro_br(self, domain: str, *, persist: bool = False) -> dict[str, Any]:
        """Lookup .br domain registration & age metadata via RDAP Registro.br Medallion connector."""
        from public_data_gateway.connectors.compliance.rdap_registro_br import (
            RdapRegistroBrConnector,
        )

        conn = RdapRegistroBrConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()[0]
        return conn.fetch_domain(domain=domain)

    def lookup_gleif_lei(
        self, query: str, *, limit: int = 5, persist: bool = False
    ) -> list[dict[str, Any]]:
        """Search global Legal Entity Identifiers (LEI) via GLEIF Medallion connector."""
        from public_data_gateway.connectors.empresarial.gleif_lei import GleifLeiConnector

        conn = GleifLeiConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_lei_records(query=query, limit=limit))

    def lookup_ip_intel(self, ip: str, *, persist: bool = False) -> dict[str, Any]:
        """Lookup IP ASN and geolocation via BGPView / ipwho.is Medallion connector."""
        from public_data_gateway.connectors.compliance.bgpview_ip_intel import (
            BgpviewIpIntelConnector,
        )

        conn = BgpviewIpIntelConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()[0]
        return conn.fetch_ip(ip=ip)

    def lookup_b2b_domain_profile(self, domain: str, *, persist: bool = False) -> dict[str, Any]:
        """Lookup optional freemium B2B domain profile via CompanyEnrich Medallion connector."""
        from public_data_gateway.connectors.empresarial.company_enrich_apollo_bdc import (
            CompanyEnrichConnector,
        )

        conn = CompanyEnrichConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()[0]
        return conn.fetch_b2b_profile(domain=domain)

    def lookup_country(self, code: str = "BRA", *, persist: bool = False) -> dict[str, Any]:
        """Lookup ISO-3166 country metadata via REST Countries Medallion connector."""
        from public_data_gateway.connectors.demografico.rest_countries import RestCountriesConnector

        conn = RestCountriesConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()[0]
        return conn.fetch_country(code=code)

    def search_openalex_works(
        self, query: str, *, per_page: int = 5, persist: bool = False
    ) -> list[dict[str, Any]]:
        """Search scientific works & citations via OpenAlex Medallion connector."""
        from public_data_gateway.connectors.educacao.openalex_api import OpenAlexApiConnector

        conn = OpenAlexApiConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_works(query=query, per_page=per_page))

    def query_gdelt_doc(
        self, query: str, *, maxrecords: int = 5, persist: bool = False
    ) -> list[dict[str, Any]]:
        """Query global news articles via GDELT Project Doc 2.0 Medallion connector."""
        from public_data_gateway.connectors.agregadores.gdelt_doc_api import GdeltDocApiConnector

        conn = GdeltDocApiConnector(settings=self._sdk.settings)
        if persist:
            return conn.extract_table().to_pylist()
        return conn.normalize_records(conn.fetch_articles(query=query, maxrecords=maxrecords))


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
        self.company = CompanyFacade(self)
        self.finance = FinanceFacade(self)
        self.trade = TradeFacade(self)
        self.funds = FundsFacade(self)
        self.banking = BankingFacade(self)
        self.treasury = TreasuryFacade(self)
        self.socioeconomic = SocioeconomicFacade(self)
        self.labor = LaborFacade(self)
        self.health = HealthFacade(self)
        self.education = EducationFacade(self)
        self.real_estate = RealEstateFacade(self)
        self.environment = EnvironmentFacade(self)
        self.metaportals = MetaportalsFacade(self)
        self.cross_domain = CrossDomainFacade(self)
        self.lifecycle = LifecycleFacade(self)
        self.public_apis = PublicApisHubFacade(self)
        self.lakehouse = LakehouseFacade(self)

    def list_sources(
        self,
        wave: int | None = None,
        segment_id: str | None = None,
        status: str | None = None,
        product_id: str | None = None,
        pack_id: str | None = None,
    ) -> list[SourceEntry]:
        """List catalog sources filtered by wave, segment, status, or solution pack."""
        return self.catalog.list_sources(
            wave=wave,
            segment_id=segment_id,
            status=status,
            product_id=product_id,
            pack_id=pack_id,
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

    def get_pgfn_debtor(self, documento_or_name: str) -> list[dict[str, Any]]:
        """Consultar inscrições ativas em Dívida Ativa da União por CNPJ, CPF ou município (PGFN)."""
        return self.compliance.get_pgfn_debtor(documento_or_name)

    def get_pgfn_previdenciario(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Consultar débitos previdenciários inscritos em Dívida Ativa da União (PGFN)."""
        return self.compliance.get_pgfn_previdenciario(id_municipio_or_doc)

    def get_pgfn_fgts(self, id_municipio_or_doc: str) -> list[dict[str, Any]]:
        """Consultar débitos de FGTS inscritos perante a PGFN/Caixa."""
        return self.compliance.get_pgfn_fgts(id_municipio_or_doc)

    def get_pgfn_municipal_summary(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar consolidado municipal do estoque de Dívida Ativa e FGTS (PGFN)."""
        return self.compliance.get_pgfn_municipal_summary(id_municipio_or_name)

    def get_pgfn_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold de risco de inadimplência fiscal e ajuizamento (PGFN)."""
        return self.compliance.get_pgfn_features(id_municipio_or_name)

    def list_top_pgfn_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios por estoque consolidado de Dívida Ativa da União e FGTS (PGFN)."""
        return self.compliance.list_top_pgfn_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sinesp_crimes(self, termo: str) -> list[dict[str, Any]]:
        """Consultar ocorrências criminais municipais SINESP-VDE (roubo de carga, veículos e estelionato)."""
        return self.compliance.get_sinesp_crimes(termo)

    def get_sinesp_mvi_victims(self, termo: str) -> list[dict[str, Any]]:
        """Consultar indicadores municipais de Mortes Violentas Intencionais (MVI) e elucidação SINESP/FBSP."""
        return self.compliance.get_sinesp_mvi_victims(termo)

    def get_sinesp_security_capacity(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar delegacias, guarda municipal, efetivo policial e investimento per capita (SINESP)."""
        return self.compliance.get_sinesp_security_capacity(id_municipio_or_name)

    def get_sinesp_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de risco criminal, MVI e roubo de cargas SINESP/FBSP."""
        return self.compliance.get_sinesp_municipal_summary(id_municipio_or_name)

    def get_sinesp_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de segurança pública e tier de risco patrimonial (SINESP/FBSP)."""
        return self.compliance.get_sinesp_features(id_municipio_or_name)

    def list_top_safest_sinesp_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_cargo_theft: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por segurança pública SINESP/FBSP ou taxa de roubo de carga."""
        return self.compliance.list_top_safest_sinesp_municipalities(
            limit=limit,
            sigla_uf=sigla_uf,
            sort_by_cargo_theft=sort_by_cargo_theft,
        )

    def get_cgu_direct_expenses(self, termo: str) -> list[dict[str, Any]]:
        """Consultar gastos diretos e transferências federais CGU por código, órgão SIAFI, IBGE, UF ou cidade."""
        return self.compliance.get_cgu_direct_expenses(termo)

    def get_cgu_constitutional_transfers(self, termo: str) -> list[dict[str, Any]]:
        """Consultar transferências constitucionais FPM, FUNDEB e SUS no Portal da Transparência por IBGE, UF ou cidade."""
        return self.compliance.get_cgu_constitutional_transfers(termo)

    def get_cgu_amendments_covenants(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar convênios ativos, emendas parlamentares, prestação de contas e CPGF no município (CGU)."""
        return self.compliance.get_cgu_amendments_covenants(id_municipio_or_name)

    def get_cgu_expenses_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de gastos públicos federais, execução orçamentária e gasto per capita."""
        return self.compliance.get_cgu_expenses_summary(id_municipio_or_name)

    def get_cgu_expenses_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold CGU de integridade fiscal, execução orçamentária e transparência."""
        return self.compliance.get_cgu_expenses_features(id_municipio_or_name)

    def list_top_cgu_transparency_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_spending: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por integridade de execução CGU ou volume de gastos federais."""
        return self.compliance.list_top_cgu_transparency_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_spending=sort_by_spending
        )

    def get_siape_active_staff(self, termo: str) -> list[dict[str, Any]]:
        """Consultar servidores públicos federais SIAPE por código, órgão, regime jurídico, IBGE, UF ou cidade."""
        return self.compliance.get_siape_active_staff(termo)

    def get_siape_payroll(self, termo: str) -> list[dict[str, Any]]:
        """Consultar folha bruta SIAPE, teto constitucional e gratificações DAS/FCE por município."""
        return self.compliance.get_siape_payroll(termo)

    def get_cgu_staff_compliance(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar dedicação exclusiva, alertas de acúmulo ilícito e integridade funcional CGU/SIAPE."""
        return self.compliance.get_cgu_staff_compliance(id_municipio_or_name)

    def get_siape_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de efetivo federal SIAPE, massa salarial bruta e % DAS."""
        return self.compliance.get_siape_municipal_summary(id_municipio_or_name)

    def get_siape_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold CGU/SIAPE de governança de pessoal e integridade funcional."""
        return self.compliance.get_siape_features(id_municipio_or_name)

    def list_top_siape_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_payroll: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por integridade funcional SIAPE ou maior folha salarial federal."""
        return self.compliance.list_top_siape_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_payroll=sort_by_payroll
        )

    def get_tse_candidacies(self, termo: str) -> list[dict[str, Any]]:
        """Consultar candidaturas e pluralismo partidário TSE por código, cargo, IBGE, UF ou município."""
        return self.compliance.get_tse_candidacies(termo)

    def get_tse_turnout(self, termo: str) -> list[dict[str, Any]]:
        """Consultar eleitorado apto, comparecimento, abstenção e votos válidos TSE por município."""
        return self.compliance.get_tse_turnout(termo)

    def get_tse_campaign_finance(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar prestação de contas, receitas e doadores distintos de campanha TSE."""
        return self.compliance.get_tse_campaign_finance(id_municipio_or_name)

    def get_tse_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal TSE de eleitorado, candidaturas e custo por eleitor."""
        return self.compliance.get_tse_municipal_summary(id_municipio_or_name)

    def get_tse_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold TSE de maturidade e integridade eleitoral."""
        return self.compliance.get_tse_features(id_municipio_or_name)

    def list_top_tse_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_electorate: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por maturidade eleitoral TSE ou tamanho do eleitorado."""
        return self.compliance.list_top_tse_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_electorate=sort_by_electorate
        )

    def get_litigation_risk(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Assess corporate or individual litigation risk profile across all courts (CNJ DataJud)."""
        return self.judicial.litigation_risk(cnpj_or_cpf)

    def get_cndt(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Query official CNDT labor compliance certificate (TST)."""
        return self.judicial.cndt(cnpj_or_cpf)

    def get_labor_risk(self, cnpj_or_cpf: str) -> dict[str, Any]:
        """Assess labor compliance and BNDT risk profile (TST)."""
        return self.judicial.labor_risk(cnpj_or_cpf)

    def get_justica_aberta_offices(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cadastro de cartórios/serventias extrajudiciais e provimento titular (CNJ)."""
        return self.judicial.get_justica_aberta_offices(id_municipio)

    def list_justica_aberta_productivity(
        self, id_municipio: str, atribuicao_cartorial: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar produtividade cartorial de atos, certidões e e-Notariado por atribuição (CNJ)."""
        return self.judicial.list_justica_aberta_productivity(
            id_municipio=id_municipio, atribuicao_cartorial=atribuicao_cartorial
        )

    def get_justica_aberta_emoluments(self, id_municipio: str) -> dict[str, Any]:
        """Consultar arrecadação de emolumentos, repasses e ticket médio por ato (CNJ)."""
        return self.judicial.get_justica_aberta_emoluments(id_municipio)

    def get_justica_aberta_digitalization(self, id_municipio: str) -> dict[str, Any]:
        """Consultar digitalização histórica e integração SREI/ONR/CRC dos cartórios (CNJ)."""
        return self.judicial.get_justica_aberta_digitalization(id_municipio)

    def get_justica_aberta_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de eficiência cartorial e dinamismo notarial/imobiliário (CNJ)."""
        return self.judicial.get_justica_aberta_features(id_municipio)

    def list_top_justica_aberta_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em eficiência e modernização cartorial (CNJ Justiça Aberta)."""
        return self.judicial.list_top_justica_aberta_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_improbidade_condenacoes(
        self, documento_or_name: str
    ) -> list[dict[str, Any]]:
        """Consultar condenações por improbidade administrativa (CNJ CNCIAI)."""
        return self.judicial.get_improbidade_condenacoes(documento_or_name)

    def get_improbidade_proibicoes_contratar(
        self, id_municipio_or_doc: str
    ) -> list[dict[str, Any]]:
        """Consultar sanções ativas de proibição de contratar com o poder público (CNJ CNCIAI)."""
        return self.judicial.get_improbidade_proibicoes_contratar(
            id_municipio_or_doc
        )

    def get_improbidade_ressarcimentos(
        self, id_municipio_or_doc: str
    ) -> list[dict[str, Any]]:
        """Consultar ressarcimentos ao erário em ações de improbidade (CNJ CNCIAI)."""
        return self.judicial.get_improbidade_ressarcimentos(id_municipio_or_doc)

    def get_improbidade_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar sumário municipal consolidado de improbidade administrativa (CNJ CNCIAI)."""
        return self.judicial.get_improbidade_municipal_summary(
            id_municipio_or_name
        )

    def get_improbidade_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de risco de improbidade e inidoneidade (CNJ CNCIAI)."""
        return self.judicial.get_improbidade_features(id_municipio_or_name)

    def list_top_improbidade_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios ordenados por exposição a improbidade administrativa (CNJ CNCIAI)."""
        return self.judicial.list_top_improbidade_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_suframa_status(self, cnpj_or_inscricao: str) -> dict[str, Any]:
        """Query official SUFRAMA registration and incentive status (PIM / ZFM / ALC)."""
        return self.company.suframa(cnpj_or_inscricao)

    def get_tax_incentives(self, cnpj: str) -> dict[str, Any]:
        """Assess corporate fiscal incentive score and industrial capacity profile (PIM/ZFM/ALC)."""
        return self.company.tax_incentives(cnpj)

    def get_cno_works(self, termo: str) -> list[dict[str, Any]]:
        """Consultar obras civis registradas no Cadastro Nacional de Obras CNO (RFB)."""
        return self.company.get_cno_works(termo)

    def get_cno_area_breakdown(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar área construída (m²) por destinação no CNO municipal (RFB)."""
        return self.company.get_cno_area_breakdown(id_municipio_or_name)

    def get_cno_responsible_links(self, termo: str) -> list[dict[str, Any]]:
        """Consultar vínculos de construtoras e proprietários de obras no CNO (RFB)."""
        return self.company.get_cno_responsible_links(termo)

    def get_cno_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar sumário municipal de obras ativas, paralisadas e encerradas no CNO (RFB)."""
        return self.company.get_cno_municipal_summary(id_municipio_or_name)

    def get_cno_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de dinamismo construtivo e CAPEX físico no CNO (RFB)."""
        return self.company.get_cno_features(id_municipio_or_name)

    def list_top_cno_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em expansão da construção civil no CNO (RFB)."""
        return self.company.list_top_cno_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_inpi_patentes(self, termo: str) -> list[dict[str, Any]]:
        """Consultar depósitos e concessões de patentes no INPI/RPI por código, natureza, IPC/Nice, IBGE, UF ou cidade."""
        return self.company.get_inpi_patentes(termo)

    def get_inpi_marcas(self, termo: str) -> list[dict[str, Any]]:
        """Consultar marcas registradas e desenhos industriais ativos no INPI por município ou UF."""
        return self.company.get_inpi_marcas(termo)

    def get_inpi_software_transferencia(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar registros de programa de computador, ICTs e contratos de transferência de tecnologia no INPI."""
        return self.company.get_inpi_software_transferencia(id_municipio_or_name)

    def get_inpi_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal INPI de patentes, marcas, software e densidade por 100 mil hab."""
        return self.company.get_inpi_municipal_summary(id_municipio_or_name)

    def get_inpi_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold INPI de inovação tecnológica e propriedade intelectual."""
        return self.company.get_inpi_features(id_municipio_or_name)

    def list_top_inpi_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_patentes: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por inovação em propriedade intelectual INPI ou depósitos de patentes."""
        return self.company.list_top_inpi_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_patentes=sort_by_patentes
        )

    def get_cvm_company(self, cnpj_or_codigo: str) -> dict[str, Any]:
        """Query CVM publicly traded company registration and financial profile."""
        return self.finance.cvm_company(cnpj_or_codigo)

    def get_financial_ratios(self, cnpj_or_codigo: str) -> dict[str, Any]:
        """Assess corporate financial health, solvency score, and fundamental ratios from CVM."""
        return self.finance.financial_ratios(cnpj_or_codigo)

    def get_rcvm160_offerings(self, termo: str) -> list[dict[str, Any]]:
        """Consultar ofertas públicas RCVM 160 na CVM por ativo, rito, código IBGE, UF ou município."""
        return self.finance.get_rcvm160_offerings(termo)

    def get_fii_administrators(self, termo: str) -> list[dict[str, Any]]:
        """Consultar administradores de carteiras, gestoras FII/FIP e patrimônio sob gestão na CVM."""
        return self.finance.get_fii_administrators(termo)

    def get_investment_advisors(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar assessores de investimento credenciados (AAI), sociedades e alertas PASM CVM."""
        return self.finance.get_investment_advisors(id_municipio_or_name)

    def get_cvm_extra_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal CVM de captações RCVM 160, gestoras e assessoria."""
        return self.finance.get_cvm_extra_summary(id_municipio_or_name)

    def get_cvm_extra_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold CVM de sofisticação e ecossistema de mercado de capitais."""
        return self.finance.get_cvm_extra_features(id_municipio_or_name)

    def list_top_cvm_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por ecossistema de mercado de capitais CVM ou volume RCVM 160."""
        return self.finance.list_top_cvm_extra_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )

    def get_susep_premios_ramo(self, termo: str) -> list[dict[str, Any]]:
        """Consultar prêmios diretos emitidos e sinistralidade SUSEP/SES por código, ramo, IBGE, UF ou cidade."""
        return self.finance.get_susep_premios_ramo(termo)

    def get_susep_previdencia_vgbl(self, termo: str) -> list[dict[str, Any]]:
        """Consultar contribuições VGBL/PGBL e provisões técnicas SUSEP por município ou UF."""
        return self.finance.get_susep_previdencia_vgbl(termo)

    def get_susep_corretoras_solvencia(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar corretoras de seguros ativas na SUSEP e índice de solvência regulatória."""
        return self.finance.get_susep_corretoras_solvencia(id_municipio_or_name)

    def get_susep_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal SUSEP de prêmios emitidos, sinistros e prêmio per capita."""
        return self.finance.get_susep_municipal_summary(id_municipio_or_name)

    def get_susep_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold SUSEP de maturidade do mercado segurador e solvência."""
        return self.finance.get_susep_features(id_municipio_or_name)

    def list_top_susep_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_premios: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por maturidade do mercado segurador SUSEP ou prêmios emitidos."""
        return self.finance.list_top_susep_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_premios=sort_by_premios
        )

    def get_municipality_trade(self, id_municipio: str) -> dict[str, Any]:
        """Obter estatísticas de comércio exterior e balança comercial de um município (MDIC)."""
        return self.trade.municipality_trade(id_municipio)

    def get_ncm_trade(self, codigo_ncm: str) -> dict[str, Any]:
        """Consultar histórico e fluxo estatístico de uma mercadoria NCM de 8 dígitos (MDIC)."""
        return self.trade.ncm_trade(codigo_ncm)

    def get_trade_balance(self) -> dict[str, Any]:
        """Obter o panorama macro da balança comercial brasileira (MDIC)."""
        return self.trade.trade_balance_profile()

    def get_fund_profile(self, cnpj: str) -> dict[str, Any] | None:
        """Consultar cadastro regulatório de um fundo de investimento (CVM)."""
        return self.funds.get_fund_by_cnpj(cnpj)

    def get_fund_daily_reports(
        self, cnpj: str, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Consultar informes operacionais diários de um fundo de investimento (CVM)."""
        return self.funds.get_daily_reports(cnpj, start_date=start_date, end_date=end_date)

    def get_asset_managers(
        self, query: str | None = None, min_aum: float = 0.0
    ) -> list[dict[str, Any]]:
        """Consultar ranking de sociedades gestoras de recursos por AuM (CVM)."""
        return self.funds.get_asset_managers(query=query, min_aum=min_aum)

    def get_fund_features(self, cnpj: str) -> dict[str, Any] | None:
        """Consultar features e score de resiliência e liquidez de um fundo de investimento (CVM)."""
        return self.funds.get_fund_features(cnpj)

    def get_bank_profile(self, cnpj: str) -> dict[str, Any] | None:
        """Consultar cadastro regulatório prudencial e segmento (S1 a S5) de uma instituição financeira (BACEN)."""
        return self.banking.get_bank_by_cnpj(cnpj)

    def get_bank_balance_sheet(self, cnpj: str) -> dict[str, Any] | None:
        """Consultar balanço patrimonial e DRE trimestral de uma instituição financeira (BACEN IF.data)."""
        return self.banking.get_bank_balance_sheet(cnpj)

    def get_banking_ranking(self, limit: int = 10) -> list[dict[str, Any]]:
        """Consultar ranking das maiores instituições financeiras do Brasil por Ativo Total e Índice de Basiléia (BACEN)."""
        return self.banking.get_banking_ranking(limit=limit)

    def get_bank_solidity(self, cnpj: str) -> dict[str, Any] | None:
        """Consultar features Gold e score contínuo de solidez prudencial (0 a 100) de uma instituição bancária (BACEN)."""
        return self.banking.get_bank_solidity(cnpj)

    def get_estban_municipal_balance(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar balancete bancário mensal ESTBAN COSIF de um município (BACEN)."""
        return self.banking.get_estban_municipal_balance(id_municipio_or_name)

    def get_estban_credit_portfolio(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar carteira setorial de crédito municipal (rural, imobiliário, PCLD) no ESTBAN."""
        return self.banking.get_estban_credit_portfolio(id_municipio_or_name)

    def get_estban_deposit_funding(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar captações e depósitos bancários municipais no ESTBAN (BACEN)."""
        return self.banking.get_estban_deposit_funding(id_municipio_or_name)

    def get_estban_local_institutions(
        self, id_municipio_or_cnpj: str
    ) -> list[dict[str, Any]]:
        """Consultar instituições financeiras atuantes por município no ESTBAN (BACEN)."""
        return self.banking.get_estban_local_institutions(id_municipio_or_cnpj)

    def get_estban_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de profundidade bancária e liquidez municipal (ESTBAN)."""
        return self.banking.get_estban_features(id_municipio_or_name)

    def list_top_estban_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em profundidade e liquidez bancária no ESTBAN (BACEN)."""
        return self.banking.list_top_estban_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_bcb_agencias(
        self, id_municipio_or_cnpj: str
    ) -> list[dict[str, Any]]:
        """Consultar cadastro de agências bancárias registradas no BACEN (Olinda OData)."""
        return self.banking.get_bcb_agencias(id_municipio_or_cnpj)

    def get_bcb_postos_atendimento(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar postos de atendimento bancário PA e PAEs eletrônicos municipais (BACEN)."""
        return self.banking.get_bcb_postos_atendimento(id_municipio_or_name)

    def get_bcb_cobertura_municipal(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar cobertura física de agências bancárias por 100 mil habitantes (BACEN)."""
        return self.banking.get_bcb_cobertura_municipal(id_municipio_or_name)

    def get_bcb_inclusao_financeira(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar status de inclusão financeira e dependência de correspondentes (BACEN)."""
        return self.banking.get_bcb_inclusao_financeira(id_municipio_or_name)

    def get_bcb_agencia_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de capilaridade bancária presencial por município (BACEN)."""
        return self.banking.get_bcb_agencia_features(id_municipio_or_name)

    def list_top_bcb_agencia_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios ordenados por capilaridade física de agências bancárias (BACEN)."""
        return self.banking.list_top_bcb_agencia_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_bndes_operations(self, termo: str) -> list[dict[str, Any]]:
        """Consultar operações diretas FINEM e indiretas BNDES por código, setor, IBGE, UF ou município."""
        return self.banking.get_bndes_operations(termo)

    def get_bndes_disbursements(self, termo: str) -> list[dict[str, Any]]:
        """Consultar desembolsos anuais BNDES, economia verde/ESG e participação de MPMEs."""
        return self.banking.get_bndes_disbursements(termo)

    def get_bndes_portfolio_risk(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar taxa média TLP/TJLP, inadimplência, cobertura FGI e projetos sustentáveis BNDES."""
        return self.banking.get_bndes_portfolio_risk(id_municipio_or_name)

    def get_bndes_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal BNDES de operações contratadas, desembolsos e fomento per capita."""
        return self.banking.get_bndes_municipal_summary(id_municipio_or_name)

    def get_bndes_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold BNDES de dinamismo de fomento produtivo e tier de maturidade."""
        return self.banking.get_bndes_features(id_municipio_or_name)

    def list_top_bndes_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por dinamismo de fomento BNDES ou volume total contratado."""
        return self.banking.list_top_bndes_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )

    def get_municipio_fiscal_profile(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar saúde fiscal consolidada, classificação CAPAG e endividamento de um município (STN)."""
        return self.treasury.get_municipio_fiscal_profile(id_municipio_or_name)

    def get_credit_operations_pvl(
        self, id_municipio_or_uf: str, status: str | None = None
    ) -> list[dict[str, Any]]:
        """Consultar operações de crédito e financiamentos subnacionais no SADIPEM (STN)."""
        return self.treasury.get_credit_operations_pvl(id_municipio_or_uf, status=status)

    def get_fpm_transfers(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Consultar histórico de repasses constitucionais do FPM de um município (STN)."""
        return self.treasury.get_fpm_transfers(id_municipio, ano=ano)

    def get_capag_rating(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar nota da Capacidade de Pagamento CAPAG (A, B, C) e indicadores LRF (STN)."""
        return self.treasury.get_capag_rating(id_municipio)

    def get_siconfi_ente(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar cadastro oficial do ente federativo no SICONFI (STN)."""
        return self.treasury.get_siconfi_ente(id_municipio_or_name)

    def get_siconfi_dca(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Consultar Declaração de Contas Anuais (DCA/Finbra) de um município (STN SICONFI)."""
        return self.treasury.get_siconfi_dca(id_municipio, ano=ano)

    def get_siconfi_rreo(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Consultar Relatório Resumido da Execução Orçamentária RREO (STN SICONFI)."""
        return self.treasury.get_siconfi_rreo(id_municipio, ano=ano)

    def get_siconfi_rgf(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Consultar Relatório de Gestão Fiscal RGF e limites de pessoal LRF (STN SICONFI)."""
        return self.treasury.get_siconfi_rgf(id_municipio, ano=ano)

    def get_siconfi_extratos(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Consultar extrato de entregas e homologações fiscais no SICONFI (STN)."""
        return self.treasury.get_siconfi_extratos(id_municipio, ano=ano)

    def get_siconfi_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold de solidez contábil, autonomia tributária e LRF (SICONFI)."""
        return self.treasury.get_siconfi_features(id_municipio_or_name)

    def list_top_siconfi_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em solidez contábil e fiscal no SICONFI (STN)."""
        return self.treasury.list_top_siconfi_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_capag_municipal_evaluation(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar avaliação oficial CAPAG municipal e Ranking ICF (STN)."""
        return self.treasury.get_capag_municipal_evaluation(
            id_municipio_or_name
        )

    def get_capag_state_evaluation(
        self, sigla_uf_or_id: str
    ) -> list[dict[str, Any]]:
        """Consultar avaliação oficial CAPAG estadual e espaço fiscal (STN)."""
        return self.treasury.get_capag_state_evaluation(sigla_uf_or_id)

    def get_capag_ifgf_index(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar Índice FIRJAN de Gestão Fiscal (IFGF) e subíndices municipais."""
        return self.treasury.get_capag_ifgf_index(id_municipio_or_name)

    def get_capag_sovereign_guarantees(
        self, id_municipio_or_name: str
    ) -> list[dict[str, Any]]:
        """Consultar estoque de garantias da União em operações de crédito subnacionais."""
        return self.treasury.get_capag_sovereign_guarantees(id_municipio_or_name)

    def get_capag_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de crédito soberano subnacional CAPAG + IFGF."""
        return self.treasury.get_capag_features(id_municipio_or_name)

    def list_top_capag_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios ordenados por rating soberano subnacional CAPAG + IFGF."""
        return self.treasury.list_top_capag_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def search_catmat(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Pesquisar materiais no catálogo unificado de compras públicas CATMAT."""
        return self.procurement.search_materials(query=query, limit=limit)

    def search_catser(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Pesquisar serviços no catálogo unificado de compras públicas CATSER."""
        return self.procurement.search_services(query=query, limit=limit)

    def get_procurement_benchmark(self, codigo_pdm_or_item: str) -> dict[str, Any] | None:
        """Consultar benchmark oficial de preços praticados e risco de sobrepreço (Compras.gov.br)."""
        return self.procurement.get_price_benchmark(codigo_pdm_or_item)

    def get_procurement_supplier(self, cnpj: str) -> dict[str, Any] | None:
        """Consultar histórico e volume de fornecimento de uma empresa ao poder público."""
        return self.procurement.get_supplier_history(cnpj)

    def get_querido_diario_tenders(self, termo: str) -> list[dict[str, Any]]:
        """Consultar atos licitatórios e dispensas em Diários Oficiais Municipais (Querido Diário/OKBR)."""
        return self.procurement.get_querido_diario_tenders(termo)

    def get_querido_diario_excerpts(self, termo: str) -> list[dict[str, Any]]:
        """Consultar edições indexadas, menções temáticas Educação/Saúde/ESG e legibilidade OCR."""
        return self.procurement.get_querido_diario_excerpts(termo)

    def get_querido_diario_transparency(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar acervo histórico, formato aberto TXT/PDF e transparência ativa em Diários Oficiais."""
        return self.procurement.get_querido_diario_transparency(id_municipio_or_name)

    def get_querido_diario_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal do Querido Diário (edições, licitações e valores citados)."""
        return self.procurement.get_querido_diario_summary(id_municipio_or_name)

    def get_querido_diario_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold Querido Diário OKBR de transparência e maturidade oficial."""
        return self.procurement.get_querido_diario_features(id_municipio_or_name)

    def list_top_querido_diario_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por transparência em Diários Oficiais ou volume em extratos."""
        return self.procurement.list_top_querido_diario_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )

    def get_municipality_gdp(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar PIB municipal e valor adicionado por setor (IBGE SIDRA)."""
        return self.socioeconomic.gdp(id_municipio)

    def get_municipality_socioeconomic_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar perfil socioeconômico completo, PIB per capita e dinâmica produtiva (IBGE SIDRA)."""
        return self.socioeconomic.profile(id_municipio)

    def get_ipca_history(self, limit: int = 12) -> list[dict[str, Any]]:
        """Consultar série histórica e evolução do índice de inflação IPCA (IBGE SIDRA)."""
        return self.socioeconomic.ipca_history(limit=limit)

    def get_gini_uf(self, sigla_uf: str) -> dict[str, Any] | None:
        """Consultar índice de Gini e rendimento médio domiciliar por UF (IBGE SIDRA / PNADC)."""
        return self.socioeconomic.gini_uf(sigla_uf)

    def get_top_municipalities_by_gdp(self, limit: int = 10) -> list[dict[str, Any]]:
        """Consultar ranking dos municípios com maior PIB do Brasil (IBGE SIDRA)."""
        return self.socioeconomic.top_municipalities_by_gdp(limit=limit)

    def get_census_summary(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar contagem oficial de população residente e domicílios (Censo 2022)."""
        return self.socioeconomic.census_summary(id_municipio)

    def get_demographic_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar perfil demográfico Gold, envelhecimento e maturidade social (Censo 2022)."""
        return self.socioeconomic.demographic_profile(id_municipio)

    def get_highest_aging_municipalities(self, limit: int = 10) -> list[dict[str, Any]]:
        """Consultar ranking de municípios com maior índice de envelhecimento (Censo 2022)."""
        return self.socioeconomic.highest_aging_municipalities(limit=limit)

    def get_employment_summary(
        self, id_municipio: str, competencia: str | None = None
    ) -> dict[str, Any] | None:
        """Consultar movimentações de admissões, desligamentos e saldo formal (Novo CAGED)."""
        return self.labor.employment_summary(id_municipio, competencia=competencia)

    def get_labor_market_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar perfil Gold do mercado de trabalho e score de aquecimento (Novo CAGED)."""
        return self.labor.market_profile(id_municipio)

    def get_top_job_creators(self, limit: int = 10) -> list[dict[str, Any]]:
        """Consultar ranking de municípios com maior saldo líquido de empregos gerados (Novo CAGED)."""
        return self.labor.top_job_creators(limit=limit)

    def get_rais_employment_summary(
        self, id_municipio: str, ano: int | None = None
    ) -> dict[str, Any] | None:
        """Consultar estoque anual de vínculos formais RAIS no município."""
        return self.labor.rais_summary(id_municipio, ano=ano)

    def get_rais_market_profile(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar perfil Gold e score municipal de formalização RAIS."""
        return self.labor.rais_profile(id_municipio)

    def get_rais_sectoral_employment(
        self, id_municipio: str, ano: int | None = None
    ) -> list[dict[str, Any]]:
        """Consultar vínculos RAIS por setor CNAE."""
        return self.labor.rais_sectors(id_municipio, ano=ano)

    def get_top_formalized_municipalities(self, limit: int = 10) -> list[dict[str, Any]]:
        """Consultar ranking de formalização municipal RAIS."""
        return self.labor.rais_top_formalized(limit=limit)

    def get_inss_issued_benefits(self, termo: str) -> list[dict[str, Any]]:
        """Consultar benefícios ativos emitidos no INSS (RGPS e BPC/LOAS) por código, espécie, IBGE, UF ou município."""
        return self.labor.get_inss_issued_benefits(termo)

    def get_inss_benefit_grants(self, termo: str) -> list[dict[str, Any]]:
        """Consultar concessões anuais de benefícios INSS, taxa de indeferimento e tempo médio de concessão."""
        return self.labor.get_inss_benefit_grants(termo)

    def get_mps_social_contributions(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar contribuintes ativos, arrecadação previdenciária e razão contribuintes/beneficiários (MPS)."""
        return self.labor.get_mps_social_contributions(id_municipio_or_name)

    def get_inss_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de benefícios INSS emitidos, volume financeiro mensal e BPC/LOAS."""
        return self.labor.get_inss_municipal_summary(id_municipio_or_name)

    def get_inss_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold MPS/INSS de cobertura previdenciária e sustentabilidade RGPS."""
        return self.labor.get_inss_features(id_municipio_or_name)

    def list_top_inss_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_volume: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por cobertura previdenciária INSS ou maior volume financeiro pago."""
        return self.labor.list_top_inss_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_volume=sort_by_volume
        )

    def get_state_labor_profile(self, id_or_sigla_uf: str) -> dict[str, Any] | None:
        """Consultar perfil estadual do mercado de trabalho, taxa de desocupação e renda (PNAD Contínua)."""
        return self.labor.state_profile(id_or_sigla_uf)

    def get_labor_force_indicators(
        self, id_or_sigla_uf: str, ano_trimestre: str | None = None
    ) -> dict[str, Any] | None:
        """Consultar indicadores trimestrais de força de trabalho e desocupação da UF (PNAD Contínua)."""
        return self.labor.force_indicators(id_or_sigla_uf, ano_trimestre=ano_trimestre)

    def get_lowest_unemployment_states(self, limit: int = 10) -> list[dict[str, Any]]:
        """Consultar ranking de estados com menor taxa de desocupação (PNAD Contínua)."""
        return self.labor.lowest_unemployment_states(limit=limit)

    def get_cnefe_municipality_summary(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar sumário de endereços e edificações do município (IBGE CNEFE Censo 2022)."""
        return self.geo.cnefe_municipality_summary(id_municipio)

    def get_cnefe_cep_profile(self, cep: str) -> dict[str, Any] | None:
        """Consultar perfil de endereços, centroide e contagem predial por CEP (IBGE CNEFE)."""
        return self.geo.cnefe_cep_profile(cep)

    def get_cnefe_building_types(self, id_municipio: str) -> list[dict[str, Any]]:
        """Listar espécies de edificações e finalidades de uso no município (IBGE CNEFE)."""
        return self.geo.cnefe_building_types(id_municipio)

    def get_cnefe_top_address_density(self, limit: int = 10) -> list[dict[str, Any]]:
        """Listar municípios com maior densidade de endereços por km² (IBGE CNEFE)."""
        return self.geo.cnefe_top_address_density(limit=limit)

    def get_municipio_boundary(
        self, id_municipio: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obter malha vetorial GeoJSON oficial e métricas espaciais do município (IBGE Malhas)."""
        return self.geo.get_municipio_boundary(id_municipio, qualidade=qualidade)

    def get_uf_boundary(
        self, sigla_uf: str, qualidade: str = "minima"
    ) -> dict[str, Any]:
        """Obter malha vetorial GeoJSON oficial e métricas espaciais da UF (IBGE Malhas)."""
        return self.geo.get_uf_boundary(sigla_uf, qualidade=qualidade)

    def get_territorial_features(self, id_municipio: str) -> dict[str, Any]:
        """Obter features espaciais e score de compacidade do Feature Store Gold (IBGE Malhas)."""
        return self.geo.get_territorial_features(id_municipio)

    def list_top_compact_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios com maior compacidade territorial (Polsby-Popper score) no Brasil (IBGE Malhas)."""
        return self.geo.list_top_compact_municipalities(limit=limit, sigla_uf=sigla_uf)

    def get_telecom_stations(self, id_municipio: str) -> dict[str, Any]:
        """Consultar estações rádio base (ERBs SMP) licenciadas e cobertura 4G/5G municipal (ANATEL Mosaico)."""
        return self.geo.get_telecom_stations(id_municipio)

    def get_broadband_profile(self, id_municipio: str) -> dict[str, Any]:
        """Consultar perfil de banda larga fixa (SCM), fibra FTTH e backhaul municipal (ANATEL Mosaico)."""
        return self.geo.get_broadband_profile(id_municipio)

    def list_spectrum_allocations(
        self, id_municipio: str, faixa_frequencia_mhz: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar alocações de espectro de radiofrequência por faixa no município (ANATEL Mosaico)."""
        return self.geo.list_spectrum_allocations(
            id_municipio, faixa_frequencia_mhz=faixa_frequencia_mhz
        )

    def get_telecom_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de conectividade digital e maturidade 5G municipal (ANATEL Mosaico)."""
        return self.geo.get_telecom_features(id_municipio)

    def list_top_connected_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios com maior score de conectividade digital no Brasil (ANATEL Mosaico)."""
        return self.geo.list_top_connected_municipalities(limit=limit, sigla_uf=sigla_uf)

    def get_highway_corridors(self, id_municipio: str) -> dict[str, Any]:
        """Consultar corredores rodoviários federais, concessões ANTT, pedágios e VMD de pesados (ANTT)."""
        return self.geo.get_highway_corridors(id_municipio)

    def get_rntrc_fleet_profile(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cadastro RNTRC de transportadores (ETC/TAC/CTC), frota habilitada e capacidade (ANTT)."""
        return self.geo.get_rntrc_fleet_profile(id_municipio)

    def get_rail_intermodal_terminals(self, id_municipio: str) -> dict[str, Any]:
        """Consultar conectividade ferroviária, concessionárias e terminais intermodais de carga (ANTT)."""
        return self.geo.get_rail_intermodal_terminals(id_municipio)

    def list_modal_flows(
        self, id_municipio: str, modal_transporte: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar repartição modal de cargas, custo por TKU e confiabilidade do corredor (ANTT)."""
        return self.geo.list_modal_flows(
            id_municipio, modal_transporte=modal_transporte
        )

    def get_logistics_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de infraestrutura logística e intermodalidade municipal (ANTT)."""
        return self.geo.get_logistics_features(id_municipio)

    def list_top_logistics_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em score de infraestrutura logística e intermodalidade no Brasil (ANTT)."""
        return self.geo.list_top_logistics_hubs(limit=limit, sigla_uf=sigla_uf)

    def get_fleet_by_type(self, termo: str) -> list[dict[str, Any]]:
        """Consultar frota municipal ativa RENAVAM/SENATRAN por categoria veicular, UF ou município."""
        return self.geo.get_fleet_by_type(termo)

    def get_fleet_propulsion_ev(self, termo: str) -> list[dict[str, Any]]:
        """Consultar matriz energética veicular SENATRAN e participação de eletrificados (BEV/PHEV/HEV)."""
        return self.geo.get_fleet_propulsion_ev(termo)

    def get_fleet_age_renewal(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar idade média da frota, taxa de motorização e emplacamentos anuais (SENATRAN)."""
        return self.geo.get_fleet_age_renewal(id_municipio_or_name)

    def get_fleet_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal da frota total RENAVAM, caminhões e veículos eletrificados."""
        return self.geo.get_fleet_municipal_summary(id_municipio_or_name)

    def get_fleet_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de modernização da frota veicular e mobilidade elétrica (SENATRAN)."""
        return self.geo.get_fleet_features(id_municipio_or_name)

    def list_top_fleet_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_ev: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por modernização da frota SENATRAN ou volume de eletrificados BEV/HEV."""
        return self.geo.list_top_fleet_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_ev=sort_by_ev
        )

    def get_prf_accidents(self, termo: str) -> list[dict[str, Any]]:
        """Consultar boletins agregados de sinistros rodoviários PRF por código, rodovia BR, causa, IBGE, UF ou município."""
        return self.geo.get_prf_accidents(termo)

    def get_prf_corridors(self, termo: str) -> list[dict[str, Any]]:
        """Consultar corredores logísticos federais PRF e sinistralidade de carga pesada por código, BR, IBGE, UF ou município."""
        return self.geo.get_prf_corridors(termo)

    def get_prf_causes_profile(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar perfil causal de sinistros PRF (colisões, veículos pesados, clima e alcoolemia)."""
        return self.geo.get_prf_causes_profile(id_municipio_or_name)

    def get_prf_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado anual municipal de sinistros PRF, feridos, óbitos e letalidade."""
        return self.geo.get_prf_municipal_summary(id_municipio_or_name)

    def get_prf_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold PRF de segurança viária e severidade de corredores logísticos."""
        return self.geo.get_prf_features(id_municipio_or_name)

    def list_top_safe_prf_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_fatalities: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por segurança viária PRF ou maior número de óbitos em BRs."""
        return self.geo.list_top_safe_prf_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_fatalities=sort_by_fatalities
        )

    def get_municipal_gdp_detail(self, id_municipio: str, year: int = 2023) -> dict[str, Any] | None:
        """Consultar contas econômicas municipais detalhadas (PIB, VAB setorial, impostos e participações) do IBGE."""
        return self.socioeconomic.municipal_gdp_detail(id_municipio, year)

    def get_municipal_gdp_features(self, id_municipio: str) -> dict[str, Any] | None:
        """Consultar features Gold e scores de dinamismo e diversificação produtiva do município (IBGE)."""
        return self.socioeconomic.municipal_gdp_features(id_municipio)

    def list_top_municipal_gdp(self, limit: int = 10) -> list[dict[str, Any]]:
        """Listar municípios líderes em PIB municipal no Brasil (IBGE Contas Regionais)."""
        return self.socioeconomic.top_municipalities_pib(limit=limit)

    def get_cnes_establishments(self, id_municipio: str) -> dict[str, Any]:
        """Consultar resumo de estabelecimentos de saúde ativos e cobertura SUS por município (DataSUS CNES)."""
        return self.health.get_cnes_establishments(id_municipio)

    def get_cnes_beds_icu(self, id_municipio: str) -> dict[str, Any]:
        """Consultar capacidade de leitos hospitalares e leitos de UTI por município (DataSUS CNES)."""
        return self.health.get_cnes_beds_icu(id_municipio)

    def get_cnes_equipment(self, id_municipio: str) -> dict[str, Any]:
        """Consultar disponibilidade de equipamentos de diagnóstico por imagem e hemodiálise (DataSUS CNES)."""
        return self.health.get_cnes_equipment(id_municipio)

    def list_cnes_workforce(
        self, id_municipio: str, categoria_profissional: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar profissionais de saúde e equipes eSF por município e categoria (DataSUS CNES)."""
        return self.health.list_cnes_workforce(
            id_municipio, categoria_profissional=categoria_profissional
        )

    def get_cnes_health_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de capacidade assistencial hospitalar e alta complexidade UTI (DataSUS CNES)."""
        return self.health.get_cnes_health_features(id_municipio)

    def list_top_health_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar polos hospitalares líderes em capacidade assistencial de saúde no Brasil (DataSUS CNES)."""
        return self.health.list_top_health_hubs(limit=limit, sigla_uf=sigla_uf)

    def get_sih_admissions(self, id_municipio: str) -> dict[str, Any]:
        """Consultar produção mensal de internações hospitalares (AIH) e caráter de atendimento no SUS (DataSUS SIH)."""
        return self.health.get_sih_admissions(id_municipio)

    def get_sih_hospital_costs(self, id_municipio: str) -> dict[str, Any]:
        """Consultar faturamento aprovado de AIH, ticket médio e parcela UTI no município (DataSUS SIH)."""
        return self.health.get_sih_hospital_costs(id_municipio)

    def list_sih_cid10_morbidity(
        self, id_municipio: str, capitulo_cid10: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar distribuição das internações e custos por capítulos prioritários CID-10 no município (DataSUS SIH)."""
        return self.health.list_sih_cid10_morbidity(
            id_municipio, capitulo_cid10=capitulo_cid10
        )

    def get_sih_mortality_outcomes(self, id_municipio: str) -> dict[str, Any]:
        """Consultar desfechos hospitalares (altas, óbitos, utilização de UTI e internações evitáveis ICSAP) (DataSUS SIH)."""
        return self.health.get_sih_mortality_outcomes(id_municipio)

    def get_sih_morbidity_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de complexidade cirúrgica/intensiva e pressão de morbidade SIH/SUS."""
        return self.health.get_sih_morbidity_features(id_municipio)

    def list_top_sih_complexity_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar polos hospitalares líderes em complexidade assistencial SIH/SUS no Brasil."""
        return self.health.list_top_sih_complexity_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sim_general_mortality(self, id_municipio: str) -> dict[str, Any]:
        """Consultar mortalidade geral, óbitos hospitalares e causas mal definidas no município (DataSUS SIM)."""
        return self.health.get_sim_general_mortality(id_municipio)

    def list_sim_cid10_causes(
        self, id_municipio: str, capitulo_cid10: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar distribuição municipal dos óbitos por grandes capítulos da CID-10 (DataSUS SIM)."""
        return self.health.list_sim_cid10_causes(
            id_municipio, capitulo_cid10=capitulo_cid10
        )

    def get_sim_maternal_infant_mortality(self, id_municipio: str) -> dict[str, Any]:
        """Consultar mortalidade infantil, neonatal, materna e proporção de óbitos investigados (DataSUS SIM)."""
        return self.health.get_sim_maternal_infant_mortality(id_municipio)

    def get_sim_premature_dcnt_external(self, id_municipio: str) -> dict[str, Any]:
        """Consultar mortalidade prematura por DCNT (30-69 anos), causas externas e APVP por 1.000 hab (DataSUS SIM)."""
        return self.health.get_sim_premature_dcnt_external(id_municipio)

    def get_sim_mortality_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de qualidade da vigilância de óbitos e carga de mortalidade evitável (DataSUS SIM)."""
        return self.health.get_sim_mortality_features(id_municipio)

    def list_top_sim_surveillance_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em score de qualidade de vigilância epidemiológica de óbitos no Brasil (DataSUS SIM)."""
        return self.health.list_top_sim_surveillance_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sinasc_live_births(self, id_municipio: str) -> dict[str, Any]:
        """Consultar nascidos vivos, taxa bruta de natalidade, partos hospitalares e partos cesáreos no município (DataSUS SINASC)."""
        return self.health.get_sinasc_live_births(id_municipio)

    def list_sinasc_prenatal_care(
        self, id_municipio: str, faixa_assistencia_prenatal: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar nascidos vivos estratificados por faixa de consultas pré-natal e início oportuno no município (DataSUS SINASC)."""
        return self.health.list_sinasc_prenatal_care(
            id_municipio, faixa_assistencia_prenatal=faixa_assistencia_prenatal
        )

    def get_sinasc_maternal_profile(self, id_municipio: str) -> dict[str, Any]:
        """Consultar perfil demográfico materno (idade média, gravidez na adolescência e escolaridade >=12 anos) (DataSUS SINASC)."""
        return self.health.get_sinasc_maternal_profile(id_municipio)

    def get_sinasc_newborn_vitality(self, id_municipio: str) -> dict[str, Any]:
        """Consultar vitalidade neonatal (baixo peso <2.500g, prematuridade, Apgar 7-10 e anomalias congênitas) (DataSUS SINASC)."""
        return self.health.get_sinasc_newborn_vitality(id_municipio)

    def get_sinasc_perinatal_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de qualidade da assistência pré-natal e proteção perinatal (DataSUS SINASC)."""
        return self.health.get_sinasc_perinatal_features(id_municipio)

    def list_top_sinasc_prenatal_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em qualidade da assistência pré-natal e saúde perinatal no Brasil (DataSUS SINASC)."""
        return self.health.list_top_sinasc_prenatal_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_cmed_presentations_summary(self, id_municipio: str) -> dict[str, Any]:
        """Consultar inventário de apresentações ativas e participação de genéricos/similares na CMED (ANVISA)."""
        return self.health.get_cmed_presentations_summary(id_municipio)

    def list_cmed_therapeutic_class_prices(
        self, id_municipio: str, classe_terapeutica_atc: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar preços regulados médios PF, PMC e PMVG por grande classe terapêutica ATC (ANVISA CMED)."""
        return self.health.list_cmed_therapeutic_class_prices(
            id_municipio, classe_terapeutica_atc=classe_terapeutica_atc
        )

    def get_cmed_public_procurement_pmvg(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cobertura CAP, teto PMVG e conformidade em compras públicas SUS (ANVISA CMED)."""
        return self.health.get_cmed_public_procurement_pmvg(id_municipio)

    def get_cmed_tax_regime_piscofins(self, id_municipio: str) -> dict[str, Any]:
        """Consultar alíquota modal ICMS, listas PIS/COFINS e spread PMC/PF (ANVISA CMED)."""
        return self.health.get_cmed_tax_regime_piscofins(id_municipio)

    def get_cmed_drug_pricing_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de competitividade de genéricos e eficiência regulatória PMVG/CAP (ANVISA CMED)."""
        return self.health.get_cmed_drug_pricing_features(id_municipio)

    def list_top_cmed_regulatory_efficiency_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar polos farmacêuticos líderes em eficiência regulatória PMVG/CAP no Brasil (ANVISA CMED)."""
        return self.health.list_top_cmed_regulatory_efficiency_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_vigilancia_srag_respiratory_surveillance(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar resumo de vigilância de SRAG (OpenDataSUS / SIVEP-Gripe) para um município."""
        return self.health.get_vigilancia_srag_respiratory_surveillance(id_municipio)

    def list_vigilancia_sinan_notifiable_diseases(
        self, id_municipio: str, grupo_agravo: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar notificações de agravos SINAN (arboviroses e endemias) para um município."""
        return self.health.list_vigilancia_sinan_notifiable_diseases(
            id_municipio, grupo_agravo=grupo_agravo
        )

    def get_vigilancia_sipni_vaccination_coverage(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar cobertura vacinal SI-PNI / RNDS para um município."""
        return self.health.get_vigilancia_sipni_vaccination_coverage(id_municipio)

    def get_vigilancia_cievs_response_capacity(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar capacidade de resposta laboratorial e epidemiológica CIEVS/LACEN para um município."""
        return self.health.get_vigilancia_cievs_response_capacity(id_municipio)

    def get_vigilancia_epidemiological_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar features epidemiológicas e de imunização consolidadas OpenDataSUS para um município."""
        return self.health.get_vigilancia_epidemiological_features(id_municipio)

    def list_top_vigilancia_surveillance_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em score de vigilância epidemiológica e imunização OpenDataSUS."""
        return self.health.list_top_vigilancia_surveillance_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_ans_beneficiaries(self, termo: str) -> list[dict[str, Any]]:
        """Consultar beneficiários ativos SIB/ANS de planos privados por segmentação, IBGE, UF ou município."""
        return self.health.get_ans_beneficiaries(termo)

    def get_ans_operator_quality(self, termo: str) -> list[dict[str, Any]]:
        """Consultar notas IDSS de operadoras ANS, ticket médio mensal e reclamações NIP."""
        return self.health.get_ans_operator_quality(termo)

    def get_ans_diops_loss_ratio(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar receita de contraprestações, sinistralidade assistencial e solvência DIOPS/ANS."""
        return self.health.get_ans_diops_loss_ratio(id_municipio_or_name)

    def get_ans_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal ANS de beneficiários médico-odontológicos, cobertura e IDSS."""
        return self.health.get_ans_municipal_summary(id_municipio_or_name)

    def get_ans_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold ANS de maturidade do mercado suplementar de saúde."""
        return self.health.get_ans_features(id_municipio_or_name)

    def list_top_ans_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_beneficiaries: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por maturidade de saúde suplementar ANS ou total de beneficiários."""
        return self.health.list_top_ans_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_beneficiaries=sort_by_beneficiaries
        )

    def get_datasus_sia_production(self, termo: str) -> list[dict[str, Any]]:
        """Consultar produção ambulatorial SIA/SUS por código, procedimento, agravo, IBGE, UF ou cidade."""
        return self.health.get_datasus_sia_production(termo)

    def get_datasus_sinan_notifications(self, termo: str) -> list[dict[str, Any]]:
        """Consultar notificações epidemiológicas compulsórias SINAN e oportunidade de encerramento."""
        return self.health.get_datasus_sinan_notifications(termo)

    def get_datasus_sisvan_nutrition(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar cobertura nutricional SISVAN e indicadores de obesidade/crônicas PNS."""
        return self.health.get_datasus_sisvan_nutrition(id_municipio_or_name)

    def get_datasus_extra_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal DATASUS de produção SIA/SUS, agravos SINAN e cobertura SISVAN."""
        return self.health.get_datasus_extra_summary(id_municipio_or_name)

    def get_datasus_extra_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold DATASUS Extra de resolutividade ambulatorial e vigilância SUS."""
        return self.health.get_datasus_extra_features(id_municipio_or_name)

    def list_top_datasus_extra_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_sia: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por resolutividade ambulatorial SUS ou volume de atendimentos SIA."""
        return self.health.list_top_datasus_extra_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_sia=sort_by_sia
        )

    def get_censo_escolar_infrastructure(self, id_municipio: str) -> dict[str, Any]:
        """Consultar inventário de escolas ativas, banda larga, laboratórios e acessibilidade PcD (INEP Censo Escolar)."""
        return self.education.get_censo_escolar_infrastructure(id_municipio)

    def list_censo_escolar_enrollments(
        self, id_municipio: str, etapa_ensino: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar matrículas municipais da educação básica por etapa/modalidade e tempo integral (INEP Censo Escolar)."""
        return self.education.list_censo_escolar_enrollments(
            id_municipio, etapa_ensino=etapa_ensino
        )

    def get_censo_escolar_teachers(self, id_municipio: str) -> dict[str, Any]:
        """Consultar qualificação docente, formação superior, pós-graduação e indicador AFD por município (INEP)."""
        return self.education.get_censo_escolar_teachers(id_municipio)

    def get_censo_escolar_yield_flow(self, id_municipio: str) -> dict[str, Any]:
        """Consultar rendimento escolar (aprovação, reprovação, abandono) e distorção idade-série no município (INEP)."""
        return self.education.get_censo_escolar_yield_flow(id_municipio)

    def get_censo_escolar_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de infraestrutura escolar e qualidade do fluxo educacional básico (INEP)."""
        return self.education.get_censo_escolar_features(id_municipio)

    def list_top_censo_escolar_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em infraestrutura escolar e qualidade educacional básica no Brasil (INEP)."""
        return self.education.list_top_censo_escolar_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_censo_superior_institutions(self, id_municipio: str) -> dict[str, Any]:
        """Consultar perfil de instituições de ensino superior (IES) no município (INEP)."""
        return self.education.get_censo_superior_institutions(id_municipio)

    def list_censo_superior_cine_courses(
        self, id_municipio: str, area_cine_rotulo: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar cursos presenciais/EaD e áreas CINE por município (INEP Censo Superior)."""
        return self.education.list_censo_superior_cine_courses(
            id_municipio=id_municipio, area_cine_rotulo=area_cine_rotulo
        )

    def get_censo_superior_enrollments_graduates(self, id_municipio: str) -> dict[str, Any]:
        """Consultar matrículas, ingressantes, concluintes e fluxo acadêmico superior (INEP)."""
        return self.education.get_censo_superior_enrollments_graduates(id_municipio)

    def get_censo_superior_faculty_profile(self, id_municipio: str) -> dict[str, Any]:
        """Consultar perfil docente (doutores, mestres e dedicação exclusiva) no ensino superior."""
        return self.education.get_censo_superior_faculty_profile(id_municipio)

    def get_censo_superior_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de polos universitários e capital humano superior (INEP)."""
        return self.education.get_censo_superior_features(id_municipio)

    def list_top_censo_superior_hubs(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em excelência e pesquisa na educação superior (INEP)."""
        return self.education.list_top_censo_superior_hubs(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_ideb_municipal_results(self, id_municipio: str) -> dict[str, Any]:
        """Consultar resultados bienais do IDEB municipal por etapa e cumprimento de metas (INEP)."""
        return self.education.get_ideb_municipal_results(id_municipio)

    def list_ideb_stage_network_scores(
        self, id_municipio: str, etapa_ciclo: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar notas IDEB observadas versus metas INEP por etapa e dependência administrativa."""
        return self.education.list_ideb_stage_network_scores(
            id_municipio=id_municipio, etapa_ciclo=etapa_ciclo
        )

    def get_ideb_saeb_proficiency(self, id_municipio: str) -> dict[str, Any]:
        """Consultar proficiências SAEB em Língua Portuguesa e Matemática e aprendizado adequado (INEP)."""
        return self.education.get_ideb_saeb_proficiency(id_municipio)

    def get_ideb_biennial_goals_evolution(self, id_municipio: str) -> dict[str, Any]:
        """Consultar evolução bienal do IDEB vs. 2021, gaps de metas e equidade socioeconômica INSE."""
        return self.education.get_ideb_biennial_goals_evolution(id_municipio)

    def get_ideb_education_quality_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de desempenho educacional no IDEB e proficiência/equidade SAEB."""
        return self.education.get_ideb_education_quality_features(id_municipio)

    def list_top_ideb_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em desempenho no IDEB e proficiência SAEB no Brasil (INEP)."""
        return self.education.list_top_ideb_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_enem_municipal_area_scores(self, id_municipio: str) -> dict[str, Any]:
        """Consultar notas médias TRI por área de conhecimento e taxa de presença no ENEM (INEP)."""
        return self.education.get_enem_municipal_area_scores(id_municipio)

    def list_enem_scores_by_school_network(
        self, id_municipio: str, dependencia_rede: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar desempenho no ENEM por dependência administrativa da escola de ensino médio (INEP)."""
        return self.education.list_enem_scores_by_school_network(
            id_municipio=id_municipio, dependencia_rede=dependencia_rede
        )

    def get_enem_essay_competencies(self, id_municipio: str) -> dict[str, Any]:
        """Consultar nota média na Redação do ENEM, competências I/III/V e proporção 900+ (INEP)."""
        return self.education.get_enem_essay_competencies(id_municipio)

    def get_enem_socioeconomic_equity(self, id_municipio: str) -> dict[str, Any]:
        """Consultar indicadores socioeconômicos QSE, isenção, internet Q025 e elegibilidade SiSU/ProUni (INEP)."""
        return self.education.get_enem_socioeconomic_equity(id_municipio)

    def get_enem_performance_equity_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar features Gold de desempenho cognitivo TRI e equidade no ENEM (INEP)."""
        return self.education.get_enem_performance_equity_features(id_municipio)

    def list_top_enem_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em desempenho cognitivo e equidade no ENEM no Brasil (INEP)."""
        return self.education.list_top_enem_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sucupira_program_evaluation(self, id_municipio: str) -> dict[str, Any]:
        """Consultar avaliação quadrienal CAPES Sucupira dos programas stricto sensu no município."""
        return self.education.get_sucupira_program_evaluation(id_municipio)

    def list_sucupira_students_by_area(
        self, id_municipio: str, grande_area_conhecimento: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar discentes matriculados, titulados e bolsistas CAPES por grande área de conhecimento."""
        return self.education.list_sucupira_students_by_area(
            id_municipio=id_municipio,
            grande_area_conhecimento=grande_area_conhecimento,
        )

    def get_sucupira_faculty_research(self, id_municipio: str) -> dict[str, Any]:
        """Consultar corpo docente permanente, bolsistas PQ CNPq e linhas de pesquisa (CAPES Sucupira)."""
        return self.education.get_sucupira_faculty_research(id_municipio)

    def get_sucupira_scientific_production(self, id_municipio: str) -> dict[str, Any]:
        """Consultar produção científica Qualis A1/A2, teses, patentes/software e fomento (CAPES Sucupira)."""
        return self.education.get_sucupira_scientific_production(id_municipio)

    def get_sucupira_postgrad_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de pós-graduação stricto sensu e produção científica (CAPES Sucupira)."""
        return self.education.get_sucupira_postgrad_features(id_municipio)

    def list_top_sucupira_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes no ecossistema de pós-graduação stricto sensu no Brasil (CAPES Sucupira)."""
        return self.education.list_top_sucupira_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_onr_property_registry(self, id_municipio: str) -> dict[str, Any]:
        """Consultar acervo municipal de matrículas CNM e emissões digitais SREI/ONR."""
        return self.real_estate.get_onr_property_registry(id_municipio)

    def list_onr_transactions_by_nature(
        self, id_municipio: str, natureza_ato: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar atos registrais e volume financeiro imobiliário por natureza jurídica (ONR)."""
        return self.real_estate.list_onr_transactions_by_nature(
            id_municipio=id_municipio, natureza_ato=natureza_ato
        )

    def get_onr_real_estate_credit_guarantees(self, id_municipio: str) -> dict[str, Any]:
        """Consultar garantias imobiliárias, alienação fiduciária, Home Equity CGI e execuções (ONR)."""
        return self.real_estate.get_onr_real_estate_credit_guarantees(id_municipio)

    def get_onr_serp_interoperability(self, id_municipio: str) -> dict[str, Any]:
        """Consultar interoperabilidade SERP/SAEC, títulos nato-digitais, SLA e ordens CNIB/Penhora."""
        return self.real_estate.get_onr_serp_interoperability(id_municipio)

    def get_onr_real_estate_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de maturidade digital SREI/SERP e liquidez imobiliária (ONR)."""
        return self.real_estate.get_onr_real_estate_features(id_municipio)

    def list_top_onr_real_estate_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em maturidade digital SREI/SERP e liquidez imobiliária (ONR)."""
        return self.real_estate.list_top_onr_real_estate_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_iptu_itbi_property_cadastre(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cadastro imobiliário tributário municipal (IPTU, valor venal e adimplência)."""
        return self.real_estate.get_iptu_itbi_property_cadastre(id_municipio)

    def list_itbi_transactions_by_segment(
        self, id_municipio: str, segmento_imovel: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar transações imobiliárias ITBI do município por segmento de imóvel."""
        return self.real_estate.list_itbi_transactions_by_segment(
            id_municipio=id_municipio, segmento_imovel=segmento_imovel
        )

    def get_iptu_itbi_price_dynamics(self, id_municipio: str) -> dict[str, Any]:
        """Consultar dinâmica de preços por m² no ITBI, defasagem PGV e liquidez."""
        return self.real_estate.get_iptu_itbi_price_dynamics(id_municipio)

    def get_iptu_itbi_fiscal_governance(self, id_municipio: str) -> dict[str, Any]:
        """Consultar governança cadastral georreferenciada CTM, ITBI digital e SINTER/ONR."""
        return self.real_estate.get_iptu_itbi_fiscal_governance(id_municipio)

    def get_iptu_itbi_real_estate_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de dinamismo imobiliário e governança cadastral IPTU/ITBI."""
        return self.real_estate.get_iptu_itbi_real_estate_features(id_municipio)

    def list_top_iptu_itbi_capitals(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar capitais ranqueadas por dinamismo imobiliário ITBI e governança cadastral CTM."""
        return self.real_estate.list_top_iptu_itbi_capitals(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_terrabrasilis_prodes_deforestation(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar incremento anual de desmatamento PRODES e cobertura florestal (INPE TerraBrasilis)."""
        return self.environment.get_terrabrasilis_prodes_deforestation(id_municipio)

    def list_terrabrasilis_deter_alerts(
        self, id_municipio: str, classe_alerta_deter: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar alertas DETER-B por classe de desmatamento e degradação (INPE TerraBrasilis)."""
        return self.environment.list_terrabrasilis_deter_alerts(
            id_municipio=id_municipio,
            classe_alerta_deter=classe_alerta_deter,
        )

    def get_terrabrasilis_land_cover(self, id_municipio: str) -> dict[str, Any]:
        """Consultar balanço de vegetação nativa, uso agropecuário e Código Florestal (INPE TerraBrasilis)."""
        return self.environment.get_terrabrasilis_land_cover(id_municipio)

    def get_terrabrasilis_esg_governance(self, id_municipio: str) -> dict[str, Any]:
        """Consultar embargos IBAMA, regularização CAR e elegibilidade EUDR/ESG (INPE TerraBrasilis)."""
        return self.environment.get_terrabrasilis_esg_governance(id_municipio)

    def get_terrabrasilis_environmental_features(
        self, id_municipio: str
    ) -> dict[str, Any]:
        """Consultar features Gold de pressão de desmatamento e risco ESG/EUDR (INPE TerraBrasilis)."""
        return self.environment.get_terrabrasilis_environmental_features(id_municipio)

    def list_top_terrabrasilis_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em conformidade verde e baixo risco ESG (INPE TerraBrasilis)."""
        return self.environment.list_top_terrabrasilis_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_hidroweb_stations(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cobertura e disponibilidade da rede telemétrica HidroWeb (ANA)."""
        return self.environment.get_hidroweb_stations(id_municipio)

    def list_hidroweb_flows(
        self,
        id_municipio: str,
        tipo_monitoramento_hidrologico: str | None = None,
    ) -> list[dict[str, Any]]:
        """Listar séries de cotas fluviais e vazões por segmento hidrológico (ANA HidroWeb)."""
        return self.environment.list_hidroweb_flows(
            id_municipio=id_municipio,
            tipo_monitoramento_hidrologico=tipo_monitoramento_hidrologico,
        )

    def get_hidroweb_rainfall(self, id_municipio: str) -> dict[str, Any]:
        """Consultar pluviometria acumulada, anomalias e alertas de cheias/estiagens (ANA HidroWeb)."""
        return self.environment.get_hidroweb_rainfall(id_municipio)

    def get_hidroweb_water_security(self, id_municipio: str) -> dict[str, Any]:
        """Consultar qualidade da água (IQA), segurança hídrica (ISH) e outorgas (ANA HidroWeb)."""
        return self.environment.get_hidroweb_water_security(id_municipio)

    def get_hidroweb_hydrology_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de telemetria e segurança hídrica municipal (ANA HidroWeb)."""
        return self.environment.get_hidroweb_hydrology_features(id_municipio)

    def list_top_hidroweb_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em segurança hídrica e resiliência telemétrica (ANA HidroWeb)."""
        return self.environment.list_top_hidroweb_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_mapbiomas_land_cover(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cobertura vegetal nativa, agropecuária e água municipal (MapBiomas)."""
        return self.environment.get_mapbiomas_land_cover(id_municipio)

    def list_mapbiomas_transitions(
        self,
        id_municipio: str,
        classe_transicao_mapbiomas: str | None = None,
    ) -> list[dict[str, Any]]:
        """Listar matriz de transições de uso e cobertura da terra por classe (MapBiomas)."""
        return self.environment.list_mapbiomas_transitions(
            id_municipio=id_municipio,
            classe_transicao_mapbiomas=classe_transicao_mapbiomas,
        )

    def get_mapbiomas_agriculture(self, id_municipio: str) -> dict[str, Any]:
        """Consultar pastagens, lavouras temporárias/perenes e áreas irrigadas (MapBiomas)."""
        return self.environment.get_mapbiomas_agriculture(id_municipio)

    def get_mapbiomas_alerts(self, id_municipio: str) -> dict[str, Any]:
        """Consultar alertas de desmatamento validados e sobreposições (MapBiomas Alerta)."""
        return self.environment.get_mapbiomas_alerts(id_municipio)

    def get_mapbiomas_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de cobertura e sustentabilidade do solo (MapBiomas)."""
        return self.environment.get_mapbiomas_features(id_municipio)

    def list_top_mapbiomas_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em sustentabilidade de cobertura do solo (MapBiomas)."""
        return self.environment.list_top_mapbiomas_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_sicar_properties(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cadastro de imóveis rurais, cobertura e embargos IBAMA (SFB/SICAR)."""
        return self.environment.get_sicar_properties(id_municipio)

    def list_sicar_legal_reserve(
        self,
        id_municipio: str,
        categoria_imovel_car: str | None = None,
    ) -> list[dict[str, Any]]:
        """Listar conformidade de Reserva Legal e APP por categoria fundiária (SFB/SICAR)."""
        return self.environment.list_sicar_legal_reserve(
            id_municipio,
            categoria_imovel_car=categoria_imovel_car,
        )

    def get_sicar_pra_compliance(self, id_municipio: str) -> dict[str, Any]:
        """Consultar regularização ambiental no Programa de Regularização Ambiental - PRA (SFB/SICAR)."""
        return self.environment.get_sicar_pra_compliance(id_municipio)

    def get_sicar_geometry(self, id_municipio: str) -> dict[str, Any]:
        """Consultar precisão geoespacial, sobreposições e integração SIGEF/INCRA (SFB/SICAR)."""
        return self.environment.get_sicar_geometry(id_municipio)

    def get_sicar_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de conformidade ambiental rural CAR/EUDR (SFB/SICAR)."""
        return self.environment.get_sicar_features(id_municipio)

    def list_top_sicar_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em conformidade ambiental rural CAR/SICAR (SFB)."""
        return self.environment.list_top_sicar_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_zarc_climate_aptitude(self, id_municipio: str) -> dict[str, Any]:
        """Consultar aptidão climática e decêndios aptos no ZARC (MAPA/Embrapa)."""
        return self.environment.get_zarc_climate_aptitude(id_municipio)

    def list_zarc_planting_windows(
        self,
        id_municipio: str,
        cultura_zarc: str | None = None,
    ) -> list[dict[str, Any]]:
        """Listar janelas decendiais de plantio ZARC por cultura e solo AD (MAPA)."""
        return self.environment.list_zarc_planting_windows(
            id_municipio,
            cultura_zarc=cultura_zarc,
        )

    def get_zarc_proagro_coverage(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cobertura PROAGRO, Seguro Rural (PSR) e sinistralidade (MAPA ZARC)."""
        return self.environment.get_zarc_proagro_coverage(id_municipio)

    def get_zarc_soil_risk(self, id_municipio: str) -> dict[str, Any]:
        """Consultar retenção hídrica do solo (AD1-AD6) e plantio direto (MAPA ZARC)."""
        return self.environment.get_zarc_soil_risk(id_municipio)

    def get_zarc_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de risco agroclimático ZARC/PROAGRO (MAPA)."""
        return self.environment.get_zarc_features(id_municipio)

    def list_top_zarc_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em aptidão agroclimática ZARC (MAPA)."""
        return self.environment.list_top_zarc_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_ibama_infractions(self, termo: str) -> list[dict[str, Any]]:
        """Consultar autos de infração ambiental do IBAMA (SIFISC) por auto, CNPJ/CPF, município ou UF."""
        return self.environment.get_ibama_infractions(termo)

    def get_ibama_embargoes(self, termo: str) -> list[dict[str, Any]]:
        """Consultar termos de embargo ambientais (TAD) do IBAMA/EUDR por TAD, CNPJ/CPF, município ou bioma."""
        return self.environment.get_ibama_embargoes(termo)

    def get_ibama_fines_judgment(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar julgamentos, arrecadação e inscrição em dívida ativa das multas IBAMA no município."""
        return self.environment.get_ibama_fines_judgment(id_municipio_or_name)

    def get_ibama_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de autuações e embargos ativos do IBAMA (SIFISC)."""
        return self.environment.get_ibama_municipal_summary(id_municipio_or_name)

    def get_ibama_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de risco ambiental, conformidade IBAMA e alertas EUDR por município."""
        return self.environment.get_ibama_features(id_municipio_or_name)

    def list_top_ibama_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_risk: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por conformidade ambiental IBAMA ou pressão de embargos."""
        return self.environment.list_top_ibama_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_risk=sort_by_risk
        )

    def get_sicor_custeio(self, termo: str) -> list[dict[str, Any]]:
        """Consultar operações de crédito de custeio rural BACEN SICOR por município, UF, cultura ou programa."""
        return self.environment.get_sicor_custeio(termo)

    def get_sicor_investimento(self, termo: str) -> list[dict[str, Any]]:
        """Consultar operações de investimento agropecuário BACEN SICOR (RenovAgro ABC+, Pronaf Mais Alimentos)."""
        return self.environment.get_sicor_investimento(termo)

    def get_sicor_pronaf_proagro(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar participação PRONAF, cobertura PROAGRO, crédito ABC+ e glosas Res. BCB 140/2021."""
        return self.environment.get_sicor_pronaf_proagro(id_municipio_or_name)

    def get_sicor_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de contratos e volume financeiro de crédito rural no BACEN SICOR."""
        return self.environment.get_sicor_municipal_summary(id_municipio_or_name)

    def get_sicor_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de dinamismo do crédito rural e sustentabilidade no BACEN SICOR."""
        return self.environment.get_sicor_features(id_municipio_or_name)

    def list_top_sicor_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_pronaf: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por dinamismo de crédito rural SICOR ou participação PRONAF."""
        return self.environment.list_top_sicor_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_pronaf=sort_by_pronaf
        )

    def get_agro_pam_crops(self, termo: str) -> list[dict[str, Any]]:
        """Consultar produção agrícola municipal IBGE PAM (Tabela 5457) por cultura, UF ou município."""
        return self.environment.get_agro_pam_crops(termo)

    def get_agro_ppm_livestock(self, termo: str) -> list[dict[str, Any]]:
        """Consultar efetivos pecuários e produção de leite IBGE PPM (Tabela 3939) por espécie ou município."""
        return self.environment.get_agro_ppm_livestock(termo)

    def get_agro_pevs_forestry(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar extração vegetal e silvicultura municipal IBGE PEVS (Tabela 289)."""
        return self.environment.get_agro_pevs_forestry(id_municipio_or_name)

    def get_agro_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal do Valor Bruto da Produção Agropecuária IBGE (PAM/PPM/PEVS)."""
        return self.environment.get_agro_municipal_summary(id_municipio_or_name)

    def get_agro_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de potência agropecuária e vocação produtiva municipal (IBGE)."""
        return self.environment.get_agro_features(id_municipio_or_name)

    def list_top_agro_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_vbp: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em produção agropecuária (PAM/PPM/PEVS) ou Valor Bruto (VBP)."""
        return self.environment.list_top_agro_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_vbp=sort_by_vbp
        )

    def get_cafir_rural_properties(self, termo: str) -> list[dict[str, Any]]:
        """Consultar imóveis rurais registrados no RFB CAFIR/CIB por NIRF, SNCR, CNPJ/CPF ou município."""
        return self.environment.get_cafir_rural_properties(termo)

    def get_cafir_cnir_linkage(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar vinculação CAFIR-CNIR/INCRA, georreferenciamento SIGEF e paridade CAR no município."""
        return self.environment.get_cafir_cnir_linkage(id_municipio_or_name)

    def get_cafir_land_concentration(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar índice de Gini fundiário e distribuição de área por módulo fiscal (CAFIR/RFB)."""
        return self.environment.get_cafir_land_concentration(id_municipio_or_name)

    def get_cafir_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de imóveis rurais, área total e VTN estimado (CAFIR/RFB)."""
        return self.environment.get_cafir_municipal_summary(id_municipio_or_name)

    def get_cafir_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de regularização fundiária CAFIR/CNIR e risco cadastral rural."""
        return self.environment.get_cafir_features(id_municipio_or_name)

    def list_top_cafir_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_area: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por regularização fundiária CAFIR/CNIR ou maior área rural."""
        return self.environment.list_top_cafir_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_area=sort_by_area
        )

    def get_queimadas_hotspots(self, termo: str) -> list[dict[str, Any]]:
        """Consultar focos de calor ativos INPE Queimadas por código, satélite, bioma, UF ou município."""
        return self.environment.get_queimadas_hotspots(termo)

    def get_seeg_emissions(self, termo: str) -> list[dict[str, Any]]:
        """Consultar inventário municipal de emissões de GEE (SEEG) por setor, UF ou município."""
        return self.environment.get_seeg_emissions(termo)

    def get_queimadas_fire_risk(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar índice de risco de fogo INPE, severidade FRP e área de cicatriz queimada."""
        return self.environment.get_queimadas_fire_risk(id_municipio_or_name)

    def get_queimadas_seeg_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de focos INPE e emissões líquidas de GEE SEEG (tCO2e)."""
        return self.environment.get_queimadas_seeg_municipal_summary(
            id_municipio_or_name
        )

    def get_queimadas_seeg_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de resiliência climática, pressão de incêndios e emissões GEE."""
        return self.environment.get_queimadas_seeg_features(id_municipio_or_name)

    def list_top_queimadas_seeg_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_emissions: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por resiliência climática INPE/SEEG ou maiores emissões tCO2e."""
        return self.environment.list_top_queimadas_seeg_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_emissions=sort_by_emissions
        )

    def get_mmgd_plants(self, termo: str) -> list[dict[str, Any]]:
        """Consultar usinas e empreendimentos de Geração Distribuída (ANEEL MMGD) por código, fonte, distribuidora ou município."""
        return self.environment.get_mmgd_plants(termo)

    def get_mmgd_sources_by_municipality(self, termo: str) -> list[dict[str, Any]]:
        """Consultar potência instalada MMGD por fonte renovável (Solar UFV, Biogás/Biomassa) e município."""
        return self.environment.get_mmgd_sources_by_municipality(termo)

    def get_mmgd_utility_grid(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar indicadores da distribuidora elétrica (prazo de vistoria e inversão de fluxo MMGD)."""
        return self.environment.get_mmgd_utility_grid(id_municipio_or_name)

    def get_mmgd_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de usinas MMGD ANEEL, potência total (kW) e fração rural."""
        return self.environment.get_mmgd_municipal_summary(id_municipio_or_name)

    def get_mmgd_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold de transição energética MMGD e tier solar municipal (ANEEL)."""
        return self.environment.get_mmgd_features(id_municipio_or_name)

    def list_top_mmgd_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_power: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por score de transição energética MMGD ou maior potência instalada (kW)."""
        return self.environment.list_top_mmgd_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_power=sort_by_power
        )

    def get_anp_fuel_prices(self, termo: str) -> list[dict[str, Any]]:
        """Consultar preços médios semanais e margens ANP (LPC) por combustível, UF ou município."""
        return self.environment.get_anp_fuel_prices(termo)

    def get_anp_fuel_sales(self, termo: str) -> list[dict[str, Any]]:
        """Consultar volumes comercializados ANP de biocombustíveis e derivados por UF ou município."""
        return self.environment.get_anp_fuel_sales(termo)

    def get_anp_retail_network(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar rede municipal de postos revendedores autorizados ANP e índice de qualidade PMQC."""
        return self.environment.get_anp_retail_network(id_municipio_or_name)

    def get_anp_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal ANP de gasolina, etanol hidratado, diesel S10 e biocombustíveis."""
        return self.environment.get_anp_municipal_summary(id_municipio_or_name)

    def get_anp_features(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar features Gold ANP de competitividade logística e matriz de biocombustíveis."""
        return self.environment.get_anp_features(id_municipio_or_name)

    def list_top_anp_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_biofuel: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por competitividade ANP ou participação de biocombustíveis (%)."""
        return self.environment.list_top_anp_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_biofuel=sort_by_biofuel
        )

    def get_snis_water_sewage(self, termo: str) -> list[dict[str, Any]]:
        """Consultar indicadores municipais SNIS/SINISA de abastecimento de água e esgotamento sanitário."""
        return self.environment.get_snis_water_sewage(termo)

    def get_snis_solid_waste(self, termo: str) -> list[dict[str, Any]]:
        """Consultar dados municipais SNIS/SINISA de coleta de resíduos sólidos, aterros e reciclagem."""
        return self.environment.get_snis_solid_waste(termo)

    def get_snis_financial_investments(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar tarifas médias, suficiência de caixa e investimentos per capita em saneamento (SNIS)."""
        return self.environment.get_snis_financial_investments(id_municipio_or_name)

    def get_snis_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal SNIS de água, esgoto tratado, perdas e investimentos."""
        return self.environment.get_snis_municipal_summary(id_municipio_or_name)

    def get_snis_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold SNIS de universalização do saneamento básico (Novo Marco Legal 2033)."""
        return self.environment.get_snis_features(id_municipio_or_name)

    def list_top_snis_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_sewage: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por universalização do saneamento SNIS ou atendimento de esgoto (%)."""
        return self.environment.list_top_snis_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_sewage=sort_by_sewage
        )

    def get_sedec_disasters(self, termo: str) -> list[dict[str, Any]]:
        """Consultar ocorrências de desastres naturais S2ID/COBRADE por código, tipologia, IBGE, UF ou município."""
        return self.environment.get_sedec_disasters(termo)

    def get_sedec_decrees(self, termo: str) -> list[dict[str, Any]]:
        """Consultar decretações federais de emergência (SE) e calamidade (ECP) por código, IBGE, UF ou cidade."""
        return self.environment.get_sedec_decrees(termo)

    def get_sedec_cemaden_monitoring(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar rede pluviométrica CEMADEN, exposição em áreas de risco e alertas geohidrológicos."""
        return self.environment.get_sedec_cemaden_monitoring(id_municipio_or_name)

    def get_sedec_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal 5 anos de desastres S2ID, prejuízos econômicos e alertas CEMADEN."""
        return self.environment.get_sedec_municipal_summary(id_municipio_or_name)

    def get_sedec_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold SEDEC/CEMADEN de resiliência climática e vulnerabilidade a desastres."""
        return self.environment.get_sedec_features(id_municipio_or_name)

    def list_top_resilient_sedec_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_damages: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por resiliência a desastres SEDEC ou maiores prejuízos econômicos."""
        return self.environment.list_top_resilient_sedec_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_damages=sort_by_damages
        )

    def get_ons_generation_capacity(self, termo: str) -> list[dict[str, Any]]:
        """Consultar usinas e capacidade instalada no Sistema Interligado Nacional ONS por código, CEG, fonte, IBGE, UF ou cidade."""
        return self.environment.get_ons_generation_capacity(termo)

    def get_ons_load_balance(self, termo: str) -> list[dict[str, Any]]:
        """Consultar balanço de carga elétrica, energia armazenada EAR (%) e CMO ONS por código, subsistema, IBGE, UF ou cidade."""
        return self.environment.get_ons_load_balance(termo)

    def get_mme_grid_security(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar subestações de alta tensão MME/ONS, continuidade DEC e disponibilidade da rede SIN."""
        return self.environment.get_mme_grid_security(id_municipio_or_name)

    def get_ons_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de potência SIN, geração renovável e demanda média."""
        return self.environment.get_ons_municipal_summary(id_municipio_or_name)

    def get_ons_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold MME/ONS de segurança energética e tier de despacho renovável SIN."""
        return self.environment.get_ons_features(id_municipio_or_name)

    def list_top_ons_energy_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_renewable: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por segurança energética MME/ONS ou participação renovável SIN (%)."""
        return self.environment.list_top_ons_energy_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_renewable=sort_by_renewable
        )

    def get_anm_sigmine_processes(self, termo: str) -> list[dict[str, Any]]:
        """Consultar processos minerários SIGMINE/ANM por número, substância, fase, IBGE, UF ou município."""
        return self.environment.get_anm_sigmine_processes(termo)

    def get_anm_cfem_royalties(self, termo: str) -> list[dict[str, Any]]:
        """Consultar arrecadação municipal de royalties CFEM/ANM por guia, substância, IBGE, UF ou cidade."""
        return self.environment.get_anm_cfem_royalties(termo)

    def get_anm_sigbm_barragens(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar inventário SIGBM/ANM de barragens de rejeitos, alertas PAEBM e índice de conformidade."""
        return self.environment.get_anm_sigbm_barragens(id_municipio_or_name)

    def get_anm_municipal_summary(
        self, id_municipio_or_name: str
    ) -> dict[str, Any] | None:
        """Consultar consolidado municipal de títulos minerários ANM, área titulada (ha) e CFEM."""
        return self.environment.get_anm_municipal_summary(id_municipio_or_name)

    def get_anm_features(self, id_municipio_or_name: str) -> dict[str, Any] | None:
        """Consultar features Gold ANM de maturidade mineral ESG e classificação de mineração sustentável."""
        return self.environment.get_anm_features(id_municipio_or_name)

    def list_top_anm_municipalities(
        self,
        limit: int = 10,
        sigla_uf: str | None = None,
        sort_by_cfem: bool = False,
    ) -> list[dict[str, Any]]:
        """Listar municípios ranqueados por maturidade mineral ESG ou arrecadação CFEM ANM."""
        return self.environment.list_top_anm_municipalities(
            limit=limit, sigla_uf=sigla_uf, sort_by_cfem=sort_by_cfem
        )

    def get_bd_coverage(self, id_municipio: str) -> dict[str, Any]:
        """Consultar cobertura municipal de datasets e tabelas tratadas (Datalake Público Federado SQL)."""
        return self.metaportals.get_bd_coverage(id_municipio)

    def list_bd_datasets(
        self,
        id_municipio: str,
        dataset_bd_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Listar datasets públicos tratados por tema (Datalake Público Federado SQL)."""
        return self.metaportals.list_bd_datasets(
            id_municipio,
            dataset_bd_id=dataset_bd_id,
        )

    def get_bd_governance(self, id_municipio: str) -> dict[str, Any]:
        """Consultar governança de metadados e chaves canônicas (Datalake Público Federado SQL)."""
        return self.metaportals.get_bd_governance(id_municipio)

    def get_bd_consumption(self, id_municipio: str) -> dict[str, Any]:
        """Consultar consumo de consultas SQL federadas e eficiência (Datalake Público Federado SQL)."""
        return self.metaportals.get_bd_consumption(id_municipio)

    def get_bd_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de maturidade do datalake (Datalake Público Federado SQL)."""
        return self.metaportals.get_bd_features(id_municipio)

    def list_top_bd_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em maturidade no Datalake Público Federado (DuckDB / BigQuery)."""
        return self.metaportals.list_top_bd_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_ckan_organizations(self, id_municipio: str) -> dict[str, Any]:
        """Consultar organizações publicadoras, conjuntos abertos e PDA (Dados.gov.br CKAN)."""
        return self.metaportals.get_ckan_organizations(id_municipio)

    def list_ckan_formats(
        self,
        id_municipio: str,
        formato_recurso_ckan: str | None = None,
    ) -> list[dict[str, Any]]:
        """Listar formatos de recursos CKAN e legibilidade por máquina (Dados.gov.br)."""
        return self.metaportals.list_ckan_formats(
            id_municipio,
            formato_recurso_ckan=formato_recurso_ckan,
        )

    def get_ckan_governance(self, id_municipio: str) -> dict[str, Any]:
        """Consultar governança de licenças abertas, metadados e LAI (Dados.gov.br CKAN)."""
        return self.metaportals.get_ckan_governance(id_municipio)

    def get_ckan_engagement(self, id_municipio: str) -> dict[str, Any]:
        """Consultar downloads mensais, chamadas de API e reuso cívico (Dados.gov.br CKAN)."""
        return self.metaportals.get_ckan_engagement(id_municipio)

    def get_ckan_features(self, id_municipio: str) -> dict[str, Any]:
        """Consultar features Gold de transparência e abertura de dados (Dados.gov.br CKAN)."""
        return self.metaportals.get_ckan_features(id_municipio)

    def list_top_ckan_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """Listar municípios líderes em transparência de dados abertos no Dados.gov.br (CKAN)."""
        return self.metaportals.list_top_ckan_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_cross_domain_features(self, id_municipio: str) -> dict[str, Any]:
        """Return consolidated cross-domain ML Feature Store vector for a municipality."""
        return self.cross_domain.get_features(id_municipio)

    def list_top_cross_domain_municipalities(
        self, limit: int = 10, sigla_uf: str | None = None
    ) -> list[dict[str, Any]]:
        """List top municipalities ranked by composite cross-domain B2B propensity score."""
        return self.cross_domain.list_top_municipalities(
            limit=limit, sigla_uf=sigla_uf
        )

    def get_solution_pack_insights(
        self, pack_id: str, sigla_uf: str | None = None, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Return state-level commercial insights for a Solution Pack (P01..P10)."""
        return self.cross_domain.get_solution_pack_insights(
            pack_id=pack_id, sigla_uf=sigla_uf, limit=limit
        )

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
        """Enrich a dataset with official identifiers, enforcing Client Path Shield."""
        return self.enricher.enrich(
            entity_type=entity_type,
            key_column=key_column,
            input_path=input_path,
            input_table=input_table,
            uf_column=uf_column,
            output_path=output_path,
        )


class LakehouseFacade:
    """Cloud Lakehouse Mirror, CDC Delta, Freshness SLA, and Serverless Warehouse facade (Phase 11)."""

    def __init__(self, sdk: "BrazilData") -> None:
        self._sdk = sdk

    def audit_sla(
        self,
        source_id: str | None = None,
        include_public_apis: bool = True,
    ) -> dict[str, Any]:
        """Audit update_frequency, freshness_sla_hours, cron schedules, and local snapshot age."""
        from public_data_gateway.lakehouse.sla import FreshnessSlaAuditor

        return FreshnessSlaAuditor(settings=self._sdk.settings).audit_sources(
            source_id=source_id,
            include_public_apis=include_public_apis,
        )

    def cdc_plan(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
    ) -> dict[str, Any]:
        """Compute incremental CDC delta plan comparing local Parquet SHA-256 against state."""
        from public_data_gateway.lakehouse.cdc import CdcDeltaTracker

        return CdcDeltaTracker(settings=self._sdk.settings).build_delta_plan(
            layers=layers, sources=sources
        )

    def mirror_plan(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
        provider: str | None = None,
    ) -> dict[str, Any]:
        """Compute cloud lakehouse mirror plan (local, s3, r2) without transferring bytes."""
        from public_data_gateway.lakehouse.mirror import CloudLakehouseMirror

        return CloudLakehouseMirror(settings=self._sdk.settings).plan(
            layers=layers, sources=sources, provider=provider
        )

    def mirror_sync(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
        provider: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Execute incremental Silver/Gold Parquet ZSTD mirror sync and publish manifest."""
        from public_data_gateway.lakehouse.mirror import CloudLakehouseMirror

        return CloudLakehouseMirror(settings=self._sdk.settings).sync(
            layers=layers, sources=sources, provider=provider, dry_run=dry_run
        )

    def warehouse_ddl(
        self,
        provider: str | None = None,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate DuckDB, MotherDuck, or BigQuery serverless DDL statements."""
        from public_data_gateway.lakehouse.warehouse import PublicWarehouseAdapter

        return PublicWarehouseAdapter(settings=self._sdk.settings).generate_ddl(
            provider=provider, layers=layers, sources=sources
        )

    def warehouse_publish(
        self,
        provider: str | None = None,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Publish mirrored Parquet views to DuckDB, MotherDuck, or BigQuery."""
        from public_data_gateway.lakehouse.warehouse import PublicWarehouseAdapter

        return PublicWarehouseAdapter(settings=self._sdk.settings).publish_views(
            provider=provider, layers=layers, sources=sources, dry_run=dry_run
        )

    def warehouse_query(self, sql: str, limit: int = 100) -> list[dict[str, Any]]:
        """Execute a strictly read-only SQL query against the DuckDB warehouse."""
        from public_data_gateway.lakehouse.warehouse import PublicWarehouseAdapter

        return PublicWarehouseAdapter(settings=self._sdk.settings).query_sql(
            sql=sql, limit=limit
        )


PublicData = BrazilData
