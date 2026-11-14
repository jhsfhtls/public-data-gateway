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

    def get_suframa_status(self, cnpj_or_inscricao: str) -> dict[str, Any]:
        """Query official SUFRAMA registration and incentive status (PIM / ZFM / ALC)."""
        return self.company.suframa(cnpj_or_inscricao)

    def get_tax_incentives(self, cnpj: str) -> dict[str, Any]:
        """Assess corporate fiscal incentive score and industrial capacity profile (PIM/ZFM/ALC)."""
        return self.company.tax_incentives(cnpj)

    def get_cvm_company(self, cnpj_or_codigo: str) -> dict[str, Any]:
        """Query CVM publicly traded company registration and financial profile."""
        return self.finance.cvm_company(cnpj_or_codigo)

    def get_financial_ratios(self, cnpj_or_codigo: str) -> dict[str, Any]:
        """Assess corporate financial health, solvency score, and fundamental ratios from CVM."""
        return self.finance.financial_ratios(cnpj_or_codigo)

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
