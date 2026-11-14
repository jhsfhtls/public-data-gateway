"""Command-Line Interface (CLI) entrypoint for Brazil Data Gateway (bdg)."""

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from public_data_gateway.core.catalog import CatalogRegistry
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.core.contracts import load_contract
from public_data_gateway.core.storage import HybridStorageEngine
from public_data_gateway.enrichment.engine import EnrichmentEngine


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for the bdg CLI."""
    parser = argparse.ArgumentParser(
        prog="bdg",
        description="Brazil Data Gateway CLI: Query, profile, and enrich official Brazilian public datasets.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: status
    subparsers.add_parser("status", help="Show gateway status, directories, and storage stats.")

    # Subcommand: catalog
    cat_parser = subparsers.add_parser("catalog", help="List registered data sources from the catalog.")
    cat_parser.add_argument("--wave", type=int, help="Filter by roadmap wave (1, 2, 3)")
    cat_parser.add_argument("--segment", type=str, help="Filter by segment ID")
    cat_parser.add_argument("--status", type=str, help="Filter by status (production, planned, etc.)")
    cat_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: schema
    schema_parser = subparsers.add_parser("schema", help="View ODCS schema contract for a source.")
    schema_parser.add_argument("source_id", type=str, help="Source identifier")

    # Subcommand: dict
    dict_parser = subparsers.add_parser("dict", help="View dbt-standard data dictionary and metadata for a source.")
    dict_parser.add_argument("source_id", type=str, help="Source identifier")
    dict_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: profile
    prof_parser = subparsers.add_parser("profile", help="Generate profile metrics for a Parquet dataset.")
    prof_parser.add_argument("parquet_path", type=Path, help="Path to the Parquet file")
    prof_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: enrich
    enr_parser = subparsers.add_parser("enrich", help="Enrich external dataset with canonical codes.")
    enr_parser.add_argument("--input", required=True, type=Path, help="Input CSV or Parquet file path")
    enr_parser.add_argument("--out-file", required=True, type=Path, help="Output destination path")
    enr_parser.add_argument(
        "--entity",
        default="municipio",
        choices=["municipio", "cnpj", "cep"],
        help="Entity type to normalize and enrich",
    )
    enr_parser.add_argument("--key-col", required=True, type=str, help="Name of the key column")
    enr_parser.add_argument("--uf-col", type=str, help="Optional UF column for municipality disambiguation")
    enr_parser.add_argument("--as-of", type=str, help="Optional timestamp/date column for ASOF point-in-time join")
    enr_parser.add_argument("--json", action="store_true", help="Output summary report in JSON format")

    # Subcommand: sync
    sync_parser = subparsers.add_parser("sync", help="Trigger connector data sync from Bronze to Gold.")
    sync_parser.add_argument("source_id", type=str, help="Source identifier (e.g. ibge_localidades)")

    # Subcommand: geo
    geo_parser = subparsers.add_parser("geo", help="Query official IBGE geographic entities.")
    geo_sub = geo_parser.add_subparsers(dest="geo_action", required=True)

    geo_sub.add_parser("ufs", help="List all 27 Brazilian UFs.")

    muni_parser = geo_sub.add_parser("municipios", help="List municipalities with optional filters.")
    muni_parser.add_argument("--uf", type=str, help="Filter by UF state code (e.g. AM, SP)")
    muni_parser.add_argument("--regiao", type=str, help="Filter by region (Norte, Sul, etc.)")
    muni_parser.add_argument("--capitais", action="store_true", help="List only capital cities")
    muni_parser.add_argument("--limit", type=int, default=50, help="Max rows to return")
    muni_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    single_muni = geo_sub.add_parser("municipio", help="Query a single municipality by code or name.")
    single_muni.add_argument("query", type=str, help="IBGE 7D/6D code or city name")
    single_muni.add_argument("--uf", type=str, help="Optional UF state code")
    single_muni.add_argument("--json", action="store_true", help="Output in JSON format")

    simples_parser = subparsers.add_parser(
        "simples", help="Query Simples Nacional and MEI tax status by CNPJ."
    )
    simples_parser.add_argument("cnpj", type=str, help="14-digit CNPJ or 8-digit root")
    simples_parser.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: pncp
    pncp_parser = subparsers.add_parser(
        "pncp", help="Query public procurement and contracts from PNCP (Lei nº 14.133/2021)."
    )
    pncp_sub = pncp_parser.add_subparsers(dest="pncp_action", required=True)

    pncp_cont_p = pncp_sub.add_parser("contratacoes", help="List public procurement processes.")
    pncp_cont_p.add_argument("--uf", type=str, help="Filter by UF state code (e.g. PI, SP, MG)")
    pncp_cont_p.add_argument("--modalidade-id", type=int, help="Filter by modalidade ID (e.g. 6=Pregão)")
    pncp_cont_p.add_argument("--ano", type=int, help="Filter by procurement year (e.g. 2024)")
    pncp_cont_p.add_argument("--limit", type=int, default=10, help="Max rows to return")
    pncp_cont_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pncp_ct_p = pncp_sub.add_parser("contratos", help="List administrative contracts.")
    pncp_ct_p.add_argument("--cnpj", type=str, help="Filter by contracting agency CNPJ (14 digits)")
    pncp_ct_p.add_argument("--ni", type=str, help="Filter by vendor CNPJ/CPF")
    pncp_ct_p.add_argument("--ano", type=int, help="Filter by contract year")
    pncp_ct_p.add_argument("--limit", type=int, default=10, help="Max rows to return")
    pncp_ct_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: compliance
    comp_parser = subparsers.add_parser(
        "compliance", help="Query sanctions, ineligibility & anti-corruption compliance (CGU CEIS/CNEP/CEPIM)."
    )
    comp_sub = comp_parser.add_subparsers(dest="comp_action", required=True)

    comp_status_p = comp_sub.add_parser("status", help="Check integrity & compliance status for a CNPJ or CPF.")
    comp_status_p.add_argument("documento", type=str, help="CNPJ (14 digits) or CPF to verify")
    comp_status_p.add_argument("--json", action="store_true", help="Output in JSON format")

    comp_sanc_p = comp_sub.add_parser("sancoes", help="List administrative sanctions and penalties.")
    comp_sanc_p.add_argument("--cadastro", choices=["ceis", "cnep", "cepim"], help="Filter by specific registry")
    comp_sanc_p.add_argument("--uf", type=str, help="Filter by sanctioning authority UF")
    comp_sanc_p.add_argument("--todas", action="store_true", help="Include historical/expired sanctions (default active only)")
    comp_sanc_p.add_argument("--limit", type=int, default=10, help="Max rows to return")
    comp_sanc_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: pep
    pep_parser = subparsers.add_parser(
        "pep", help="Query Politically Exposed Persons (PEP) under COAF Resolução nº 40/2021."
    )
    pep_sub = pep_parser.add_subparsers(dest="pep_action", required=True)

    pep_status_p = pep_sub.add_parser("status", help="Check PEP status by CPF or name.")
    pep_status_p.add_argument("termo", type=str, help="CPF (masked or digits) or name substring to search")
    pep_status_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pep_list_p = pep_sub.add_parser("list", help="List Politically Exposed Persons with optional filters.")
    pep_list_p.add_argument("--uf", type=str, help="Filter by organ UF state code")
    pep_list_p.add_argument("--funcao", type=str, help="Filter by role code (e.g. MIN-EST, SEN, GOV)")
    pep_list_p.add_argument("--todos", action="store_true", help="Include historical PEP with expired cooling-off")
    pep_list_p.add_argument("--limit", type=int, default=10, help="Max rows to return")
    pep_list_p.add_argument("--json", action="store_true", help="Output in JSON format")

    jud_parser = subparsers.add_parser(
        "judicial", help="Query judicial proceedings and litigation risk profile (CNJ DataJud)."
    )
    jud_sub = jud_parser.add_subparsers(dest="jud_action", required=True)

    jud_risk_p = jud_sub.add_parser("risk", help="Assess litigation risk by CNPJ or CPF.")
    jud_risk_p.add_argument("documento", type=str, help="CNPJ or CPF to evaluate")
    jud_risk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    jud_search_p = jud_sub.add_parser("search", help="Search processes by NUP, party name, or court.")
    jud_search_p.add_argument("termo", type=str, help="Process number (NUP), party name or document")
    jud_search_p.add_argument("--tribunal", type=str, help="Filter by court acronym (e.g. TJSP, TRF1, TRT2)")
    jud_search_p.add_argument("--limit", type=int, default=10, help="Max rows to return")
    jud_search_p.add_argument("--json", action="store_true", help="Output in JSON format")

    jud_cndt_p = jud_sub.add_parser("cndt", help="Query official CNDT certificate by CNPJ or CPF.")
    jud_cndt_p.add_argument("documento", type=str, help="CNPJ or CPF to query")
    jud_cndt_p.add_argument("--json", action="store_true", help="Output in JSON format")

    jud_lrisk_p = jud_sub.add_parser("labor-risk", help="Assess labor compliance and BNDT debtor risk score.")
    jud_lrisk_p.add_argument("documento", type=str, help="CNPJ or CPF to evaluate")
    jud_lrisk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    jud_deb_p = jud_sub.add_parser("debitos", help="Search Banco Nacional de Devedores Trabalhistas (BNDT).")
    jud_deb_p.add_argument("termo", type=str, help="Document, court, or NUP")
    jud_deb_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # CNJ Justiça Aberta (Cartórios / Serventias Extrajudiciais) command group
    ja_p = subparsers.add_parser("justica-aberta", help="Query CNJ Justiça Aberta extrajudicial notary registry, productivity, emoluments, and hubs.")
    ja_sub = ja_p.add_subparsers(dest="justica_aberta_action", required=True)

    ja_off_p = ja_sub.add_parser("offices", help="Get municipal notary offices inventory and titular tenure (CNJ Justiça Aberta).")
    ja_off_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    ja_off_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ja_prd_p = ja_sub.add_parser("productivity", help="List semiannual notary acts and e-Notariado share by attribution.")
    ja_prd_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ja_prd_p.add_argument("--atribuicao", type=str, help="Optional cartorial attribution filter (e.g. Registro_Imoveis)")
    ja_prd_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ja_emo_p = ja_sub.add_parser("emoluments", help="Get municipal emoluments revenue, statutory transfers, and unit ticket (CNJ).")
    ja_emo_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ja_emo_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ja_dig_p = ja_sub.add_parser("digitalization", help="Get archive digitization, SREI/ONR/CRC integration, and transparency profile.")
    ja_dig_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ja_dig_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ja_ft_p = ja_sub.add_parser("features", help="Get Gold CNJ Justiça Aberta efficiency and real-estate dynamism features.")
    ja_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ja_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ja_rk_p = ja_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by CNJ cartorial efficiency score.")
    ja_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    ja_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    ja_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Company, Tax Regimes & Fiscal Incentives
    comp_p = subparsers.add_parser(
        "company", help="Query corporate registries, tax regimes and fiscal incentives (SUFRAMA/RFB)."
    )
    comp_sub = comp_p.add_subparsers(dest="company_action")

    c_suf_p = comp_sub.add_parser("suframa", help="Query SUFRAMA registration and incentive status.")
    c_suf_p.add_argument("documento", type=str, help="CNPJ or 9-digit SUFRAMA inscription number")
    c_suf_p.add_argument("--json", action="store_true", help="Output in JSON format")

    c_tax_p = comp_sub.add_parser("tax-incentives", help="Assess corporate fiscal incentives (PIM/ZFM/ALC).")
    c_tax_p.add_argument("cnpj", type=str, help="CNPJ to evaluate")
    c_tax_p.add_argument("--json", action="store_true", help="Output in JSON format")

    c_proj_p = comp_sub.add_parser("pim-projects", help="List industrial projects submitted to SUFRAMA CAS.")
    c_proj_p.add_argument("--cnpj", type=str, default=None, help="Filter by CNPJ or inscription")
    c_proj_p.add_argument("--status", type=str, default=None, help="Filter by project status (e.g. EM_OPERACAO)")
    c_proj_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcuts
    suf_p = subparsers.add_parser("suframa", help="Query SUFRAMA registration and incentive status.")
    suf_p.add_argument("documento", type=str, help="CNPJ or 9-digit SUFRAMA inscription number")
    suf_p.add_argument("--json", action="store_true", help="Output in JSON format")

    tax_p = subparsers.add_parser("tax-incentives", help="Assess corporate fiscal incentives (PIM/ZFM/ALC).")
    tax_p.add_argument("cnpj", type=str, help="CNPJ to evaluate")
    tax_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Finance, CVM & Capital Markets
    fin_p = subparsers.add_parser(
        "finance", help="Query capital markets, financial statements and solvency metrics (CVM DFP/ITR)."
    )
    fin_sub = fin_p.add_subparsers(dest="finance_action")

    f_cvm_p = fin_sub.add_parser("cvm", help="Query CVM publicly traded company registration and financials.")
    f_cvm_p.add_argument("documento", type=str, help="CNPJ or CVM registration code")
    f_cvm_p.add_argument("--json", action="store_true", help="Output in JSON format")

    f_rat_p = fin_sub.add_parser("ratios", help="Assess fundamental financial ratios and solvency score.")
    f_rat_p.add_argument("documento", type=str, help="CNPJ or CVM registration code")
    f_rat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    f_list_p = fin_sub.add_parser("list-cvm", help="List publicly traded companies registered with CVM.")
    f_list_p.add_argument("--setor", type=str, default=None, help="Filter by sector of activity")
    f_list_p.add_argument("--situacao", type=str, default=None, help="Filter by registration status (e.g. ATIVO)")
    f_list_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcuts
    cvm_p = subparsers.add_parser("cvm", help="Query CVM publicly traded company registration and financials.")
    cvm_p.add_argument("documento", type=str, help="CNPJ or CVM registration code")
    cvm_p.add_argument("--json", action="store_true", help="Output in JSON format")

    rat_p = subparsers.add_parser("ratios", help="Assess fundamental financial ratios and solvency score.")
    rat_p.add_argument("documento", type=str, help="CNPJ or CVM registration code")
    rat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: trade (MDIC Comex Stat)
    trade_p = subparsers.add_parser(
        "trade", help="Query foreign trade statistics and trade balance (MDIC Comex Stat)."
    )
    trade_sub = trade_p.add_subparsers(dest="trade_action", required=True)

    t_mun_p = trade_sub.add_parser("municipality", help="Query municipality foreign trade and balance.")
    t_mun_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    t_mun_p.add_argument("--json", action="store_true", help="Output in JSON format")

    t_ncm_p = trade_sub.add_parser("ncm", help="Query NCM commodity statistics and trade flow.")
    t_ncm_p.add_argument("codigo_ncm", type=str, help="8-digit NCM commodity code")
    t_ncm_p.add_argument("--json", action="store_true", help="Output in JSON format")

    t_bal_p = trade_sub.add_parser("balance", help="View national foreign trade balance overview.")
    t_bal_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: comex
    comex_p = subparsers.add_parser("comex", help="Query foreign trade statistics by municipality or NCM.")
    comex_p.add_argument("termo", type=str, help="IBGE 7D municipality code or NCM 8D")
    comex_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: funds (CVM Fundos de Investimento)
    funds_p = subparsers.add_parser(
        "funds", help="Query investment funds registration, daily NAV and asset managers (CVM)."
    )
    funds_sub = funds_p.add_subparsers(dest="funds_action", required=True)

    f_prof_p = funds_sub.add_parser("profile", help="Query investment fund registration profile.")
    f_prof_p.add_argument("cnpj", type=str, help="Investment fund CNPJ (14 digits)")
    f_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    f_daily_p = funds_sub.add_parser("daily", help="Query daily NAV operational reports.")
    f_daily_p.add_argument("cnpj", type=str, help="Investment fund CNPJ")
    f_daily_p.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    f_daily_p.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    f_daily_p.add_argument("--json", action="store_true", help="Output in JSON format")

    f_mgr_p = funds_sub.add_parser("managers", help="Query Asset Management firms ranked by AuM.")
    f_mgr_p.add_argument("--query", type=str, default=None, help="Filter by manager name or CNPJ")
    f_mgr_p.add_argument("--min-aum", type=float, default=0.0, help="Minimum AuM in BRL")
    f_mgr_p.add_argument("--json", action="store_true", help="Output in JSON format")

    f_feat_p = funds_sub.add_parser("features", help="Query Gold resilience and liquidity features.")
    f_feat_p.add_argument("cnpj", type=str, help="Investment fund CNPJ")
    f_feat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: cvm-fundos
    cvm_f_p = subparsers.add_parser("cvm-fundos", help="Shortcut to query investment fund profile or features.")
    cvm_f_p.add_argument("cnpj", type=str, help="Investment fund CNPJ")
    cvm_f_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: banking (BACEN IF.data)
    bank_p = subparsers.add_parser(
        "banking", help="Query prudential financial institutions, balances, and solidity scores (BACEN IF.data)."
    )
    bank_sub = bank_p.add_subparsers(dest="banking_action", required=True)

    b_prof_p = bank_sub.add_parser("profile", help="Query financial institution registration and segment.")
    b_prof_p.add_argument("cnpj", type=str, help="Financial institution CNPJ or BACEN code")
    b_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    b_bal_p = bank_sub.add_parser("balance", help="Query quarterly balance sheet, liabilities and P&L.")
    b_bal_p.add_argument("cnpj", type=str, help="Financial institution CNPJ or BACEN code")
    b_bal_p.add_argument("--json", action="store_true", help="Output in JSON format")

    b_rank_p = bank_sub.add_parser("ranking", help="Query ranking of financial institutions by Total Assets.")
    b_rank_p.add_argument("--segmento", type=str, default=None, help="Filter by prudential segment (S1 to S5)")
    b_rank_p.add_argument("--top", type=int, default=10, help="Number of institutions to display (default: 10)")
    b_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    b_sol_p = bank_sub.add_parser("solidity", help="Query Gold banking solidity score (0-100) and Basiléia ratio.")
    b_sol_p.add_argument("cnpj", type=str, help="Financial institution CNPJ or BACEN code")
    b_sol_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: ifdata
    ifdata_p = subparsers.add_parser("ifdata", help="Shortcut to query banking profile, balance, or solidity.")
    ifdata_p.add_argument("cnpj", type=str, help="Financial institution CNPJ or BACEN code")
    ifdata_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: treasury (STN Tesouro Transparente)
    treas_p = subparsers.add_parser(
        "treasury", help="Query municipal/state fiscal health, SADIPEM credit operations and FPM transfers (STN)."
    )
    treas_sub = treas_p.add_subparsers(dest="treasury_action", required=True)

    t_prof_p = treas_sub.add_parser("profile", help="Query municipality fiscal health profile, CAPAG and debt.")
    t_prof_p.add_argument("termo", type=str, help="IBGE 7-digit code or municipality name")
    t_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    t_pvl_p = treas_sub.add_parser("pvl", help="Query SADIPEM credit operations by municipality or UF.")
    t_pvl_p.add_argument("termo", type=str, help="IBGE 7-digit code, UF acronym (e.g. SP) or entity name")
    t_pvl_p.add_argument("--status", type=str, default=None, help="Filter by pleito status (e.g. Deferido)")
    t_pvl_p.add_argument("--json", action="store_true", help="Output in JSON format")

    t_fpm_p = treas_sub.add_parser("fpm", help="Query constitutional FPM transfers for a municipality.")
    t_fpm_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    t_fpm_p.add_argument("--ano", type=int, default=None, help="Exercise year (e.g. 2024)")
    t_fpm_p.add_argument("--json", action="store_true", help="Output in JSON format")

    t_cap_p = treas_sub.add_parser("capag", help="Query official CAPAG payment capacity grade and LRF ratios.")
    t_cap_p.add_argument("termo", type=str, help="IBGE 7-digit code or entity name")
    t_cap_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: sadipem
    sadipem_p = subparsers.add_parser("sadipem", help="Shortcut to query SADIPEM credit operations or fiscal health.")
    sadipem_p.add_argument("termo", type=str, help="IBGE 7-digit code, UF acronym or municipality name")
    sadipem_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: procurement (Compras.gov.br CATMAT/CATSER & Preços)
    proc_p = subparsers.add_parser(
        "procurement", help="Query Compras.gov.br CATMAT/CATSER catalogs and procurement price benchmarks."
    )
    proc_sub = proc_p.add_subparsers(dest="procurement_action", required=True)

    pr_mat_p = proc_sub.add_parser("materials", help="Search CATMAT materials by description or PDM.")
    pr_mat_p.add_argument("termo", type=str, help="Search query (e.g. CADEIRA, NOTEBOOK)")
    pr_mat_p.add_argument("--limit", type=int, default=10, help="Maximum number of items")
    pr_mat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pr_serv_p = proc_sub.add_parser("services", help="Search CATSER services by description or name.")
    pr_serv_p.add_argument("termo", type=str, help="Search query (e.g. SOFTWARE, VIGILANCIA)")
    pr_serv_p.add_argument("--limit", type=int, default=10, help="Maximum number of items")
    pr_serv_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pr_bench_p = proc_sub.add_parser("benchmark", help="Query price benchmark, dispersion, and risk rating for a PDM/Item.")
    pr_bench_p.add_argument("termo", type=str, help="PDM code (e.g. 00313), item code (e.g. 206504) or name")
    pr_bench_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pr_sup_p = proc_sub.add_parser("supplier", help="Query supplier sales volume and government procurement history.")
    pr_sup_p.add_argument("cnpj", type=str, help="Supplier 14-digit CNPJ")
    pr_sup_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: catmat
    catmat_p = subparsers.add_parser("catmat", help="Shortcut to search CATMAT materials or get price benchmarks.")
    catmat_p.add_argument("termo", type=str, help="Material keyword or PDM code")
    catmat_p.add_argument("--limit", type=int, default=10, help="Maximum number of items")
    catmat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: sidra (IBGE SIDRA Socioeconômico)
    sidra_p = subparsers.add_parser(
        "sidra", help="Query IBGE SIDRA indicators: municipal GDP, socioeconomic profile, IPCA inflation and Gini."
    )
    sidra_sub = sidra_p.add_subparsers(dest="sidra_action", required=True)

    s_gdp_p = sidra_sub.add_parser("gdp", help="Query official municipal GDP and sectoral VAB.")
    s_gdp_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    s_gdp_p.add_argument("--json", action="store_true", help="Output in JSON format")

    s_prof_p = sidra_sub.add_parser("profile", help="Query Gold socioeconomic profile, GDP per capita, and economic profile.")
    s_prof_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    s_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    s_ipca_p = sidra_sub.add_parser("ipca", help="Query IPCA inflation monthly time series.")
    s_ipca_p.add_argument("--limit", type=int, default=12, help="Number of months to retrieve")
    s_ipca_p.add_argument("--json", action="store_true", help="Output in JSON format")

    s_gini_p = sidra_sub.add_parser("gini", help="Query Gini inequality index and household income by UF.")
    s_gini_p.add_argument("sigla_uf", type=str, help="State 2-letter acronym (e.g. SP, RJ, DF)")
    s_gini_p.add_argument("--json", action="store_true", help="Output in JSON format")

    s_rank_p = sidra_sub.add_parser("ranking", help="List top municipalities ranked by GDP.")
    s_rank_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    s_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: pib
    pib_p = subparsers.add_parser("pib", help="Shortcut to query municipal GDP or socioeconomic profile.")
    pib_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    pib_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: pib-municipal (IBGE Produto Interno Bruto dos Municípios)
    pib_mun_p = subparsers.add_parser(
        "pib-municipal", help="Query official IBGE municipal GDP accounts, sectoral VAB and economic structure."
    )
    pib_mun_sub = pib_mun_p.add_subparsers(dest="pib_mun_action", required=True)

    pm_det_p = pib_mun_sub.add_parser("detail", help="Query municipal macro accounts and sectoral VAB.")
    pm_det_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    pm_det_p.add_argument("--year", type=int, default=2023, help="Reference year (default: 2023)")
    pm_det_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pm_feat_p = pib_mun_sub.add_parser("features", help="Query Gold features, economic weight and diversification score.")
    pm_feat_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    pm_feat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pm_top_p = pib_mun_sub.add_parser("top", help="List top Brazilian municipalities ranked by GDP.")
    pm_top_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    pm_top_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: censo (IBGE Censo Demográfico 2022)
    censo_p = subparsers.add_parser(
        "censo", help="Query official IBGE Censo Demográfico 2022 population, households, aging, and literacy."
    )
    censo_sub = censo_p.add_subparsers(dest="censo_action", required=True)

    c_sum_p = censo_sub.add_parser("summary", help="Query population, households, and density by municipality ID.")
    c_sum_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    c_sum_p.add_argument("--json", action="store_true", help="Output in JSON format")

    c_prof_p = censo_sub.add_parser("profile", help="Query Gold demographic profile, social maturity, and aging index.")
    c_prof_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    c_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    c_lit_p = censo_sub.add_parser("literacy", help="Query literacy rate and literate population counts.")
    c_lit_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    c_lit_p.add_argument("--json", action="store_true", help="Output in JSON format")

    c_age_p = censo_sub.add_parser("aging", help="List municipalities with highest aging index ratio.")
    c_age_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    c_age_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: censo2022
    censo_s_p = subparsers.add_parser("censo2022", help="Shortcut to query Censo 2022 demographic profile.")
    censo_s_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    censo_s_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: caged (MTE Novo CAGED)
    caged_p = subparsers.add_parser(
        "caged", help="Query official MTE Novo CAGED formal employment, admissions, separations, and wages."
    )
    caged_sub = caged_p.add_subparsers(dest="caged_action", required=True)

    cg_sum_p = caged_sub.add_parser("summary", help="Query monthly employment admissions, separations, and net balance.")
    cg_sum_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    cg_sum_p.add_argument("--competencia", type=str, default=None, help="Optional competence YYYY-MM")
    cg_sum_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cg_prof_p = caged_sub.add_parser("profile", help="Query Gold labor market profile, turnover, and heating score.")
    cg_prof_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    cg_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cg_sec_p = caged_sub.add_parser("sectors", help="Query employment movements by major CNAE sector.")
    cg_sec_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    cg_sec_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cg_rank_p = caged_sub.add_parser("ranking", help="List top municipalities by net job creation balance.")
    cg_rank_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    cg_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: novo-caged
    caged_s_p = subparsers.add_parser("novo-caged", help="Shortcut to query Novo CAGED labor market profile.")
    caged_s_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    caged_s_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: rais (MTE annual formal employment)
    rais_p = subparsers.add_parser(
        "rais", help="Query annual MTE RAIS formal employment and wages."
    )
    rais_sub = rais_p.add_subparsers(dest="rais_action", required=True)
    for action, help_text in (
        ("summary", "Municipal annual formal employment stock."),
        ("profile", "Municipal RAIS Gold formalization profile."),
        ("sectors", "Formal employment by CNAE sector."),
    ):
        action_p = rais_sub.add_parser(action, help=help_text)
        action_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
        if action in {"summary", "sectors"}:
            action_p.add_argument(
                "--ano", type=int, default=None, help="Optional RAIS reference year"
            )
        action_p.add_argument("--json", action="store_true", help="Output in JSON format")
    rais_rank_p = rais_sub.add_parser("ranking", help="Rank municipalities by formalization score.")
    rais_rank_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    rais_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Subcommand: pnad (IBGE PNAD Contínua)
    pnad_p = subparsers.add_parser(
        "pnad", help="Query official IBGE PNAD Contínua labor force, unemployment, earnings, and informality."
    )
    pnad_sub = pnad_p.add_subparsers(dest="pnad_action", required=True)

    pnad_ind_p = pnad_sub.add_parser("indicators", help="Query quarterly labor force and unemployment indicators.")
    pnad_ind_p.add_argument("uf", type=str, help="IBGE 2-digit UF code or 2-letter state abbreviation (e.g. SP, 35)")
    pnad_ind_p.add_argument("--trimestre", type=str, default=None, help="Optional quarter AAAA-TT (e.g. 2026-T2)")
    pnad_ind_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pnad_prof_p = pnad_sub.add_parser("profile", help="Query Gold labor resilience profile, earnings, and informality.")
    pnad_prof_p.add_argument("uf", type=str, help="IBGE 2-digit UF code or 2-letter state abbreviation (e.g. SP, 35)")
    pnad_prof_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pnad_earn_p = pnad_sub.add_parser("earnings", help="Query real habitual and effective monthly earnings by UF.")
    pnad_earn_p.add_argument("uf", type=str, help="IBGE 2-digit UF code or 2-letter state abbreviation (e.g. SP, 35)")
    pnad_earn_p.add_argument("--json", action="store_true", help="Output in JSON format")

    pnad_rank_p = pnad_sub.add_parser("ranking", help="List state ranking by unemployment or average earnings.")
    pnad_rank_p.add_argument(
        "--criterio",
        choices=["desocupacao", "renda"],
        default="desocupacao",
        help="Ranking criteria: desocupacao (lowest) or renda (highest)",
    )
    pnad_rank_p.add_argument("--limit", type=int, default=10, help="Max UFs to list")
    pnad_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # CNEFE (IBGE Cadastro Nacional de Endereços para Fins Estatísticos)
    cnefe_p = subparsers.add_parser(
        "cnefe",
        help="Query IBGE CNEFE address database, geocoding and building species (Censo 2022).",
    )
    cnefe_sub = cnefe_p.add_subparsers(dest="cnefe_action", required=True)

    cnefe_sum_p = cnefe_sub.add_parser("summary", help="Query address totals, density and classification by municipality.")
    cnefe_sum_p.add_argument("municipio", type=str, help="IBGE 7-digit code or municipality name")
    cnefe_sum_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnefe_cep_p = cnefe_sub.add_parser("cep", help="Query address counts and centroid for an 8-digit CEP.")
    cnefe_cep_p.add_argument("cep", type=str, help="8-digit Brazilian postal code (CEP)")
    cnefe_cep_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnefe_bld_p = cnefe_sub.add_parser("building-types", help="List building species breakdown for a municipality.")
    cnefe_bld_p.add_argument("municipio", type=str, help="IBGE 7-digit code or municipality name")
    cnefe_bld_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnefe_rank_p = cnefe_sub.add_parser("ranking", help="List top municipalities ranked by address density per km2.")
    cnefe_rank_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    cnefe_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Malhas Territoriais (IBGE Malhas Vetoriais)
    malhas_p = subparsers.add_parser(
        "malhas",
        help="Query official IBGE territorial boundary polygons, centroids and spatial features.",
    )
    malhas_sub = malhas_p.add_subparsers(dest="malhas_action", required=True)

    malhas_mun_p = malhas_sub.add_parser("municipio", help="Query official boundary GeoJSON and metrics for a municipality.")
    malhas_mun_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    malhas_mun_p.add_argument("--qualidade", type=str, default="minima", choices=["minima", "intermediaria", "maxima"], help="Mesh resolution quality")
    malhas_mun_p.add_argument("--json", action="store_true", help="Output in JSON format")

    malhas_uf_p = malhas_sub.add_parser("uf", help="Query official boundary GeoJSON and metrics for a Federative Unit.")
    malhas_uf_p.add_argument("sigla_uf", type=str, help="2-letter state abbreviation (e.g. SP, RJ)")
    malhas_uf_p.add_argument("--qualidade", type=str, default="minima", choices=["minima", "intermediaria", "maxima"], help="Mesh resolution quality")
    malhas_uf_p.add_argument("--json", action="store_true", help="Output in JSON format")

    malhas_feat_p = malhas_sub.add_parser("features", help="Query spatial morphology features and compactness score.")
    malhas_feat_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    malhas_feat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    malhas_rank_p = malhas_sub.add_parser("ranking", help="List top municipalities ranked by territorial compactness.")
    malhas_rank_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    malhas_rank_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    malhas_rank_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Telecomunicações e Conectividade (ANATEL Sistema Mosaico)
    anatel_p = subparsers.add_parser(
        "anatel",
        help="Query official ANATEL Mosaico telecom stations (ERBs), fiber broadband and 5G connectivity scores.",
    )
    anatel_sub = anatel_p.add_subparsers(dest="anatel_action", required=True)

    anatel_st_p = anatel_sub.add_parser("stations", help="Query licensed SMP stations (ERBs) and mobile coverage for a municipality.")
    anatel_st_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    anatel_st_p.add_argument("--json", action="store_true", help="Output in JSON format")

    anatel_bb_p = anatel_sub.add_parser("broadband", help="Query fixed broadband (SCM) and fiber FTTH profile for a municipality.")
    anatel_bb_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    anatel_bb_p.add_argument("--json", action="store_true", help="Output in JSON format")

    anatel_sp_p = anatel_sub.add_parser("spectrum", help="List radiofrequency spectrum allocations for a municipality.")
    anatel_sp_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    anatel_sp_p.add_argument("--faixa", type=str, help="Optional frequency band filter (e.g. 3500)")
    anatel_sp_p.add_argument("--json", action="store_true", help="Output in JSON format")

    anatel_ft_p = anatel_sub.add_parser("features", help="Query Gold digital connectivity features and 5G maturity score.")
    anatel_ft_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    anatel_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    anatel_rk_p = anatel_sub.add_parser("ranking", help="List top municipalities ranked by digital connectivity score.")
    anatel_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    anatel_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    anatel_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Transporte e Logística de Cargas (ANTT Malha Rodoviária, RNTRC e Ferroviária)
    antt_p = subparsers.add_parser(
        "antt",
        help="Query official ANTT highway corridors, RNTRC freight fleet, rail terminals and intermodal logistics scores.",
    )
    antt_sub = antt_p.add_subparsers(dest="antt_action", required=True)

    antt_hw_p = antt_sub.add_parser("highways", help="Query federal highway corridors, ANTT concessions and heavy vehicle traffic (VMD).")
    antt_hw_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    antt_hw_p.add_argument("--json", action="store_true", help="Output in JSON format")

    antt_rn_p = antt_sub.add_parser("rntrc", help="Query RNTRC registered freight carriers (ETC/TAC/CTC) and fleet capacity.")
    antt_rn_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    antt_rn_p.add_argument("--json", action="store_true", help="Output in JSON format")

    antt_rl_p = antt_sub.add_parser("rail", help="Query rail freight connectivity, concessions and intermodal terminals.")
    antt_rl_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    antt_rl_p.add_argument("--json", action="store_true", help="Output in JSON format")

    antt_md_p = antt_sub.add_parser("modals", help="List freight modal share, TKU cost and corridor reliability.")
    antt_md_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    antt_md_p.add_argument("--modal", type=str, help="Optional modal filter (e.g. Ferroviario, Rodoviario)")
    antt_md_p.add_argument("--json", action="store_true", help="Output in JSON format")

    antt_ft_p = antt_sub.add_parser("features", help="Query Gold logistics infrastructure and intermodality features.")
    antt_ft_p.add_argument("id_municipio", type=str, help="IBGE 7-digit municipality code")
    antt_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    antt_rk_p = antt_sub.add_parser("ranking", help="List top municipalities ranked by composite ANTT logistics score.")
    antt_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    antt_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    antt_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # DataSUS CNES (Cadastro Nacional de Estabelecimentos de Saúde) command group
    cnes_p = subparsers.add_parser("cnes", help="Query DataSUS CNES health establishments, beds/ICU, equipment, and workforce analytics.")
    cnes_sub = cnes_p.add_subparsers(dest="cnes_action", required=True)

    cnes_est_p = cnes_sub.add_parser("establishments", help="Get municipal CNES health establishments summary.")
    cnes_est_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    cnes_est_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnes_bed_p = cnes_sub.add_parser("beds", help="Get municipal CNES hospital and ICU bed capacity.")
    cnes_bed_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cnes_bed_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnes_eq_p = cnes_sub.add_parser("equipment", help="Get municipal CNES diagnostic imaging and dialysis equipment.")
    cnes_eq_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cnes_eq_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnes_wf_p = cnes_sub.add_parser("workforce", help="List municipal CNES health workforce and eSF teams.")
    cnes_wf_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cnes_wf_p.add_argument("--categoria", type=str, help="Optional category filter (e.g. Medicos)")
    cnes_wf_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnes_ft_p = cnes_sub.add_parser("features", help="Get Gold health capacity and ICU complexity features.")
    cnes_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cnes_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cnes_rk_p = cnes_sub.add_parser("ranking", help="List top municipalities ranked by composite CNES health capacity score.")
    cnes_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    cnes_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    cnes_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # DataSUS SIH (Sistema de Informações Hospitalares - SIH/SUS) command group
    sih_p = subparsers.add_parser("sih", help="Query DataSUS SIH/SUS hospital admissions (AIH), SUS costs, CID-10 morbidity, and outcomes.")
    sih_sub = sih_p.add_subparsers(dest="sih_action", required=True)

    sih_adm_p = sih_sub.add_parser("admissions", help="Get municipal SIH/SUS hospital admissions summary (AIH).")
    sih_adm_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    sih_adm_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sih_cost_p = sih_sub.add_parser("costs", help="Get municipal SIH/SUS approved hospital and ICU costs.")
    sih_cost_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sih_cost_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sih_cid_p = sih_sub.add_parser("cid10", help="List municipal SIH/SUS hospitalizations broken down by CID-10 chapters.")
    sih_cid_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sih_cid_p.add_argument("--capitulo", type=str, help="Optional CID-10 chapter filter (e.g. Circulatorio)")
    sih_cid_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sih_out_p = sih_sub.add_parser("outcomes", help="Get municipal SIH/SUS clinical outcomes, mortality, ICU utilization, and ICSAP.")
    sih_out_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sih_out_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sih_ft_p = sih_sub.add_parser("features", help="Get Gold SIH complexity and epidemiological pressure features.")
    sih_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sih_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sih_rk_p = sih_sub.add_parser("ranking", help="List top municipalities ranked by composite SIH hospital complexity score.")
    sih_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    sih_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    sih_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # DataSUS SIM (Sistema de Informações sobre Mortalidade) command group
    sim_p = subparsers.add_parser("sim", help="Query DataSUS SIM general mortality, CID-10 causes, maternal-infant rates, and preventable burden.")
    sim_sub = sim_p.add_subparsers(dest="sim_action", required=True)

    sim_gen_p = sim_sub.add_parser("general", help="Get municipal DataSUS SIM general mortality and ill-defined causes summary.")
    sim_gen_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    sim_gen_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sim_cid_p = sim_sub.add_parser("cid10", help="List municipal DataSUS SIM deaths broken down by major CID-10 chapters.")
    sim_cid_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sim_cid_p.add_argument("--capitulo", type=str, help="Optional CID-10 chapter filter (e.g. Circulatorio)")
    sim_cid_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sim_mat_p = sim_sub.add_parser("maternal-infant", help="Get municipal infant, neonatal, and maternal mortality rates and investigation share.")
    sim_mat_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sim_mat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sim_pre_p = sim_sub.add_parser("premature", help="Get premature NCD mortality (30-69y), external causes, and YPLL indicators.")
    sim_pre_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sim_pre_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sim_ft_p = sim_sub.add_parser("features", help="Get Gold DataSUS SIM surveillance quality and preventable mortality burden features.")
    sim_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sim_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sim_rk_p = sim_sub.add_parser("ranking", help="List top municipalities ranked by DataSUS SIM mortality surveillance score.")
    sim_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    sim_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    sim_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # DataSUS SINASC (Sistema de Informações sobre Nascidos Vivos) command group
    sn_p = subparsers.add_parser("sinasc", help="Query DataSUS SINASC live births, prenatal care, maternal profile, newborn vitality, and perinatal scores.")
    sn_sub = sn_p.add_subparsers(dest="sinasc_action", required=True)

    sn_bir_p = sn_sub.add_parser("births", help="Get municipal DataSUS SINASC live births summary, birth rate, and delivery route.")
    sn_bir_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    sn_bir_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sn_pre_p = sn_sub.add_parser("prenatal", help="List municipal DataSUS SINASC live births stratified by prenatal care bracket.")
    sn_pre_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sn_pre_p.add_argument("--faixa", type=str, help="Optional prenatal bracket filter (e.g. Adequado_Mais_7)")
    sn_pre_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sn_mat_p = sn_sub.add_parser("maternal", help="Get municipal maternal demographic and schooling profile (DataSUS SINASC).")
    sn_mat_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sn_mat_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sn_vit_p = sn_sub.add_parser("vitality", help="Get newborn low birth weight, prematurity, Apgar 5m, and anomalies indicators (SINASC).")
    sn_vit_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sn_vit_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sn_ft_p = sn_sub.add_parser("features", help="Get Gold DataSUS SINASC prenatal care quality and perinatal vitality features.")
    sn_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    sn_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    sn_rk_p = sn_sub.add_parser("ranking", help="List top municipalities ranked by DataSUS SINASC prenatal quality score.")
    sn_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    sn_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    sn_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # ANVISA CMED (Câmara de Regulação do Mercado de Medicamentos) command group
    cm_p = subparsers.add_parser("cmed", help="Query ANVISA CMED regulated drug prices (PF, PMC, PMVG), CAP, ICMS/PIS-COFINS, and efficiency hubs.")
    cm_sub = cm_p.add_subparsers(dest="cmed_action", required=True)

    cm_pres_p = cm_sub.add_parser("presentations", help="Get municipal ANVISA CMED active drug presentations inventory.")
    cm_pres_p.add_argument("id_municipio", help="7-digit IBGE municipality code, CNPJ-14, or municipality name")
    cm_pres_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cm_cls_p = cm_sub.add_parser("classes", help="List regulated PF, PMC and PMVG prices stratified by ATC therapeutic class.")
    cm_cls_p.add_argument("id_municipio", help="7-digit IBGE municipality code, CNPJ-14, or municipality name")
    cm_cls_p.add_argument("--classe", type=str, help="Optional ATC therapeutic class substring filter (e.g. Antineoplasicos)")
    cm_cls_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cm_pmvg_p = cm_sub.add_parser("pmvg", help="Get CAP coverage, PMVG ceiling, and public SUS procurement compliance.")
    cm_pmvg_p.add_argument("id_municipio", help="7-digit IBGE municipality code, CNPJ-14, or municipality name")
    cm_pmvg_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cm_tax_p = cm_sub.add_parser("taxes", help="Get state modal ICMS rate, PIS/COFINS lists, and active commercialization ratio.")
    cm_tax_p.add_argument("id_municipio", help="7-digit IBGE municipality code, CNPJ-14, or municipality name")
    cm_tax_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cm_ft_p = cm_sub.add_parser("features", help="Get Gold ANVISA CMED generics competitiveness and PMVG/CAP regulatory efficiency features.")
    cm_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, CNPJ-14, or municipality name")
    cm_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cm_rk_p = cm_sub.add_parser("ranking", help="List top pharmaceutical hubs ranked by ANVISA CMED regulatory efficiency score.")
    cm_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    cm_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    cm_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # OpenDataSUS Vigilância Epidemiológica & Imunização command group
    vig_p = subparsers.add_parser("vigilancia", help="Query OpenDataSUS SRAG, SINAN notifiable diseases, SI-PNI vaccination coverage, CIEVS/LACEN, and surveillance rankings.")
    vig_sub = vig_p.add_subparsers(dest="vigilancia_action", required=True)

    vig_srag_p = vig_sub.add_parser("srag", help="Get municipal Flu Syndrome (e-SUS) and SARI/SRAG (SIVEP-Gripe) surveillance indicators.")
    vig_srag_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name")
    vig_srag_p.add_argument("--json", action="store_true", help="Output in JSON format")

    vig_sinan_p = vig_sub.add_parser("sinan", help="List municipal SINAN compulsory notification diseases by epidemiological group.")
    vig_sinan_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name")
    vig_sinan_p.add_argument("--grupo", type=str, help="Optional disease group filter (e.g. Arboviroses)")
    vig_sinan_p.add_argument("--json", action="store_true", help="Output in JSON format")

    vig_vac_p = vig_sub.add_parser("vacinacao", help="Get municipal SI-PNI/RNDS vaccination coverage, doses, homogeneity, and dropout indicators.")
    vig_vac_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name")
    vig_vac_p.add_argument("--json", action="store_true", help="Output in JSON format")

    vig_cievs_p = vig_sub.add_parser("cievs", help="Get municipal CIEVS 24h/48h response readiness, GAL/LACEN turnaround, and RNDS integration indicators.")
    vig_cievs_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name")
    vig_cievs_p.add_argument("--json", action="store_true", help="Output in JSON format")

    vig_ft_p = vig_sub.add_parser("features", help="Get Gold OpenDataSUS epidemiological readiness and PNI immunization protection features.")
    vig_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit DataSUS prefix, CEP-8, or municipality name")
    vig_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    vig_rk_p = vig_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by OpenDataSUS epidemiological surveillance readiness score.")
    vig_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    vig_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    vig_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # INEP Censo Escolar (Educação Básica) command group
    ce_p = subparsers.add_parser("censo-escolar", help="Query INEP Censo Escolar schools, enrollments, teachers, flow, and education scores.")
    ce_sub = ce_p.add_subparsers(dest="censo_escolar_action", required=True)

    ce_sch_p = ce_sub.add_parser("schools", help="Get municipal active schools and infrastructure indicators (INEP).")
    ce_sch_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    ce_sch_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ce_enr_p = ce_sub.add_parser("enrollments", help="List municipal basic education enrollments by teaching stage (INEP).")
    ce_enr_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ce_enr_p.add_argument("--etapa", type=str, help="Optional educational stage filter (e.g. Ensino_Medio)")
    ce_enr_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ce_tch_p = ce_sub.add_parser("teachers", help="Get municipal teaching workforce qualification and INEP AFD indicator.")
    ce_tch_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ce_tch_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ce_flw_p = ce_sub.add_parser("flow", help="Get municipal school approval, abandonment, full-time share, and distortion rates.")
    ce_flw_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ce_flw_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ce_ft_p = ce_sub.add_parser("features", help="Get Gold INEP Censo Escolar infrastructure and flow quality features.")
    ce_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    ce_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ce_rk_p = ce_sub.add_parser("ranking", help="List top municipalities ranked by INEP Censo Escolar infrastructure score.")
    ce_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    ce_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    ce_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # INEP Censo da Educação Superior command group
    cs_p = subparsers.add_parser("censo-superior", help="Query INEP Censo da Educação Superior IES, CINE courses, enrollments, faculty, and hubs.")
    cs_sub = cs_p.add_subparsers(dest="censo_superior_action", required=True)

    cs_ins_p = cs_sub.add_parser("institutions", help="Get municipal higher education institutions (IES) inventory and IGC (INEP).")
    cs_ins_p.add_argument("id_municipio", help="7-digit IBGE municipality code (e.g. 3550308)")
    cs_ins_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cs_crs_p = cs_sub.add_parser("courses", help="List higher education courses, enrollments, and ENADE by CINE area (INEP).")
    cs_crs_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cs_crs_p.add_argument("--area", type=str, help="Optional CINE macroarea filter (e.g. STEM)")
    cs_crs_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cs_enr_p = cs_sub.add_parser("enrollments", help="Get municipal undergraduate enrollments, freshmen, graduates, TCG and TDA rates.")
    cs_enr_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cs_enr_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cs_fac_p = cs_sub.add_parser("faculty", help="Get municipal IES faculty PhD/Masters share and full-time DE regime profile.")
    cs_fac_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cs_fac_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cs_ft_p = cs_sub.add_parser("features", help="Get Gold INEP Censo Superior academic excellence and human capital features.")
    cs_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code")
    cs_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    cs_rk_p = cs_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by INEP Censo Superior excellence score.")
    cs_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    cs_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    cs_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # INEP IDEB command group
    ideb_p = subparsers.add_parser("ideb", help="Query INEP IDEB municipal education quality, SAEB proficiency, biennial evolution, and rankings.")
    ideb_sub = ideb_p.add_subparsers(dest="ideb_action", required=True)

    ideb_res_p = ideb_sub.add_parser("results", help="Get municipal IDEB biennial results by stage and goal attainment (INEP).")
    ideb_res_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    ideb_res_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ideb_stg_p = ideb_sub.add_parser("stages", help="List observed IDEB vs. INEP projected goal across educational stages and networks.")
    ideb_stg_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    ideb_stg_p.add_argument("--etapa", type=str, help="Optional stage/network filter (e.g. Anos_Iniciais)")
    ideb_stg_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ideb_saeb_p = ideb_sub.add_parser("saeb", help="Get municipal SAEB Portuguese/Math proficiency and adequate learning share (INEP).")
    ideb_saeb_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    ideb_saeb_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ideb_ev_p = ideb_sub.add_parser("evolution", help="Get municipal biennial IDEB evolution vs. 2021, goal gaps, and INSE equity ratio.")
    ideb_ev_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    ideb_ev_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ideb_ft_p = ideb_sub.add_parser("features", help="Get Gold INEP IDEB education performance and SAEB proficiency/equity features.")
    ideb_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    ideb_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    ideb_rk_p = ideb_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by composite INEP IDEB quality score.")
    ideb_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    ideb_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    ideb_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # INEP ENEM (Microdados do Exame Nacional do Ensino Médio) command group
    enem_p = subparsers.add_parser("enem", help="Query INEP ENEM area scores, school networks, essay competencies, equity QSE, and rankings.")
    enem_sub = enem_p.add_subparsers(dest="enem_action", required=True)

    enem_sc_p = enem_sub.add_parser("scores", help="Get municipal INEP ENEM mean TRI scores across all four objective areas.")
    enem_sc_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name")
    enem_sc_p.add_argument("--json", action="store_true", help="Output in JSON format")

    enem_nw_p = enem_sub.add_parser("networks", help="List municipal INEP ENEM scores disaggregated by school network dependency.")
    enem_nw_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name")
    enem_nw_p.add_argument("--rede", type=str, help="Optional school network filter (e.g. Privada)")
    enem_nw_p.add_argument("--json", action="store_true", help="Output in JSON format")

    enem_es_p = enem_sub.add_parser("essay", help="Get municipal INEP ENEM Essay score, Competencies I/III/V, and 900+ excellence share.")
    enem_es_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name")
    enem_es_p.add_argument("--json", action="store_true", help="Output in JSON format")

    enem_eq_p = enem_sub.add_parser("equity", help="Get municipal INEP ENEM QSE socioeconomic equity, Q025 internet, and SiSU/ProUni eligibility.")
    enem_eq_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name")
    enem_eq_p.add_argument("--json", action="store_true", help="Output in JSON format")

    enem_ft_p = enem_sub.add_parser("features", help="Get Gold INEP ENEM cognitive performance and higher-education equity features.")
    enem_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, 8-digit CEP, or municipality name")
    enem_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    enem_rk_p = enem_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by composite INEP ENEM score.")
    enem_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    enem_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    enem_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # CAPES Sucupira (Pós-Graduação Stricto Sensu) command group
    suc_p = subparsers.add_parser("sucupira", help="Query CAPES Sucupira postgraduate programs, students by field, faculty, production, and rankings.")
    suc_sub = suc_p.add_subparsers(dest="sucupira_action", required=True)

    suc_pr_p = suc_sub.add_parser("programs", help="Get municipal CAPES Sucupira stricto sensu programs count, excellence 6-7 share, and mean grade.")
    suc_pr_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP")
    suc_pr_p.add_argument("--json", action="store_true", help="Output in JSON format")

    suc_st_p = suc_sub.add_parser("students", help="List municipal CAPES Sucupira master's/doctoral students, graduates, and scholarships by field.")
    suc_st_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP")
    suc_st_p.add_argument("--area", type=str, help="Optional CAPES knowledge field filter (e.g. Engenharias)")
    suc_st_p.add_argument("--json", action="store_true", help="Output in JSON format")

    suc_fa_p = suc_sub.add_parser("faculty", help="Get municipal CAPES Sucupira permanent faculty, CNPq PQ fellows, and research lines.")
    suc_fa_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP")
    suc_fa_p.add_argument("--json", action="store_true", help="Output in JSON format")

    suc_pd_p = suc_sub.add_parser("production", help="Get municipal CAPES Sucupira Qualis A1/A2 papers, defended theses, patents, and grants.")
    suc_pd_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP")
    suc_pd_p.add_argument("--json", action="store_true", help="Output in JSON format")

    suc_ft_p = suc_sub.add_parser("features", help="Get Gold CAPES Sucupira postgraduate density, production scores, and 4-tier classification.")
    suc_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 6-digit prefix, or 8-digit CEP")
    suc_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    suc_rk_p = suc_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by composite CAPES Sucupira score.")
    suc_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    suc_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    suc_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # ONR Registro de Imóveis (SREI/SERP/CNM) command group
    onr_p = subparsers.add_parser("onr", help="Query ONR SREI real estate registries, transactions, credit guarantees, SERP, and rankings.")
    onr_sub = onr_p.add_subparsers(dest="onr_action", required=True)

    onr_reg_p = onr_sub.add_parser("registry", help="Get municipal CNM property registry stock and SREI digital certificates (ONR).")
    onr_reg_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    onr_reg_p.add_argument("--json", action="store_true", help="Output in JSON format")

    onr_tx_p = onr_sub.add_parser("transactions", help="List municipal real estate acts and declared financial volume by legal nature (ONR).")
    onr_tx_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    onr_tx_p.add_argument("--natureza", type=str, help="Optional act nature filter (e.g. Compra_Venda)")
    onr_tx_p.add_argument("--json", action="store_true", help="Output in JSON format")

    onr_cr_p = onr_sub.add_parser("credit", help="Get fiduciary lien real estate credit guarantees, Home Equity CGI, and defaults (ONR).")
    onr_cr_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    onr_cr_p.add_argument("--json", action="store_true", help="Output in JSON format")

    onr_srp_p = onr_sub.add_parser("serp", help="Get SERP/SAEC interoperability, nato-digital titles, SLA, and CNIB/Penhora orders (ONR).")
    onr_srp_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    onr_srp_p.add_argument("--json", action="store_true", help="Output in JSON format")

    onr_ft_p = onr_sub.add_parser("features", help="Get Gold ONR/SREI digital maturity and real estate liquidity features.")
    onr_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    onr_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    onr_rk_p = onr_sub.add_parser("ranking", help="List top Brazilian municipalities ranked by composite ONR SREI/SERP maturity score.")
    onr_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    onr_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    onr_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Capitais IPTU / ITBI (Cadastros Imobiliários Municipais) command group
    iptu_p = subparsers.add_parser("iptu-itbi", help="Query municipal IPTU property cadastre, ITBI transactions, prices, CTM governance, and rankings.")
    iptu_sub = iptu_p.add_subparsers(dest="iptu_action", required=True)

    iptu_cad_p = iptu_sub.add_parser("cadastre", help="Get municipal IPTU property cadastre, PGV venal value, and compliance.")
    iptu_cad_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    iptu_cad_p.add_argument("--json", action="store_true", help="Output in JSON format")

    iptu_tx_p = iptu_sub.add_parser("transactions", help="List municipal ITBI transactions by property segment.")
    iptu_tx_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    iptu_tx_p.add_argument("--segmento", type=str, help="Optional segment or category filter (e.g. Residencial, Comercial)")
    iptu_tx_p.add_argument("--json", action="store_true", help="Output in JSON format")

    iptu_pr_p = iptu_sub.add_parser("prices", help="Get ITBI declared price per m², annual growth, PGV-to-market ratio, and liquidity.")
    iptu_pr_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    iptu_pr_p.add_argument("--json", action="store_true", help="Output in JSON format")

    iptu_gov_p = iptu_sub.add_parser("governance", help="Get CTM georeferenced coverage, digital ITBI, and SINTER/CNO/ONR interoperability.")
    iptu_gov_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    iptu_gov_p.add_argument("--json", action="store_true", help="Output in JSON format")

    iptu_ft_p = iptu_sub.add_parser("features", help="Get Gold IPTU/ITBI market dynamism and fiscal governance features.")
    iptu_ft_p.add_argument("id_municipio", help="7-digit IBGE municipality code, 8-digit CEP, or municipality name")
    iptu_ft_p.add_argument("--json", action="store_true", help="Output in JSON format")

    iptu_rk_p = iptu_sub.add_parser("ranking", help="List top Brazilian capitals ranked by ITBI real estate dynamism and CTM governance.")
    iptu_rk_p.add_argument("--limit", type=int, default=10, help="Max municipalities to list")
    iptu_rk_p.add_argument("--uf", type=str, help="Optional 2-letter state filter")
    iptu_rk_p.add_argument("--json", action="store_true", help="Output in JSON format")

    # Direct shortcut: pnad-uf
    pnad_s_p = subparsers.add_parser("pnad-uf", help="Shortcut to query PNAD Contínua labor market profile.")
    pnad_s_p.add_argument("uf", type=str, help="IBGE 2-digit UF code or 2-letter state abbreviation (e.g. SP, 35)")
    pnad_s_p.add_argument("--json", action="store_true", help="Output in JSON format")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI execution entrypoint returning exit code 0 on success, 1 on failure."""
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = GatewaySettings()
    storage = HybridStorageEngine(settings)
    catalog = CatalogRegistry(settings)

    try:
        if args.command == "status":
            print("Brazil Data Gateway Status:")
            print(f"  Repo Root:      {settings.repo_root}")
            print(f"  Data Dir:       {settings.data_dir}")
            print(f"  Silver Layer:   {settings.silver_dir}")
            print(f"  Quarantine:     {settings.quarantine_dir}")
            print(f"  Cache TTL:      {settings.cache_ttl_seconds}s")
            print(f"  DuckDB Limit:   {settings.duckdb_memory_limit}")
            return 0

        if args.command == "catalog":
            sources = catalog.list_sources(
                wave=args.wave, segment_id=args.segment, status=args.status
            )
            if args.json:
                data = [s.model_dump() for s in sources]
                print(json.dumps(data, indent=2, ensure_ascii=False))
            else:
                print(f"Registered Sources ({len(sources)} found):")
                for s in sources:
                    print(
                        f"  [{s.roadmap_wave}] {s.source_id:<25} {s.name:<30} ({s.agency}) RICE:{s.rice_score:.1f}"
                    )
            return 0

        if args.command == "schema":
            contract_path = settings.repo_root / "catalog" / "contracts" / f"{args.source_id}.odcs.yaml"
            if not contract_path.is_file():
                print(f"Contract file not found at: {contract_path}", file=sys.stderr)
                return 1
            contract = load_contract(contract_path)
            print(f"Contract: {contract.contract_id} (Version {contract.version})")
            print(f"Source ID: {contract.source_id}")
            print(f"Primary Keys: {contract.primary_keys}")
            print("Columns:")
            for col in contract.columns:
                req = "mandatory" if col.required else "optional"
                print(f"  - {col.name:<25} {col.data_type:<10} ({req})")
            return 0

        if args.command == "dict":
            import yaml

            dbt_dict = catalog.get_dictionary(args.source_id)
            if not dbt_dict:
                print(f"Data dictionary not found for source: '{args.source_id}'", file=sys.stderr)
                return 1
            if args.json:
                print(json.dumps(dbt_dict, indent=2, ensure_ascii=False))
            else:
                print(yaml.dump(dbt_dict, sort_keys=False, allow_unicode=True))
            return 0

        if args.command == "profile":
            summary = storage.profile_parquet(args.parquet_path)
            if args.json:
                print(json.dumps(summary, indent=2))
            else:
                print(f"Dataset Profile: {args.parquet_path}")
                print(f"  Row Count:  {summary['row_count']}")
                print(f"  Columns:    {', '.join(summary['columns'])}")
                print(f"  Size Bytes: {summary['size_bytes']}")
                print(f"  SHA-256:    {summary['sha256']}")
            return 0

        if args.command == "enrich":
            enricher = EnrichmentEngine(settings=settings, storage_engine=storage)
            _, report = enricher.enrich(
                entity_type=args.entity,
                key_column=args.key_col,
                input_path=args.input,
                uf_column=args.uf_col,
                as_of_column=args.as_of,
                output_path=args.out_file,
            )
            if args.json:
                print(report.model_dump_json(indent=2))
            else:
                print("Enrichment Complete:")
                print(f"  Entity Type:  {report.entity_type}")
                print(f"  Total Rows:   {report.total_rows}")
                print(f"  Matched Rows: {report.matched_rows} ({report.match_rate:.1%})")
                print(f"  Output Path:  {report.output_path}")
                print(f"  SHA-256:      {report.sha256}")
            return 0

        if args.command == "sync":
            connector = get_connector(args.source_id, settings=settings)
            print(f"Starting sync for connector: {args.source_id}...")
            table = connector.extract()
            print(f"Sync complete! Materialized {table.num_rows} records into Bronze, Silver, and Gold.")
            return 0

        if args.command == "geo":
            from public_data_gateway.connectors.demografico.ibge_localidades import (
                IBGELocalidadesConnector,
            )

            conn = IBGELocalidadesConnector(settings=settings, storage=storage)
            if args.geo_action == "ufs":
                ufs = conn.list_ufs()
                for uf in ufs:
                    print(f"  [{uf['sigla_uf']}] {uf['nome_uf']:<20} (Região: {uf['regiao']})")
                return 0

            if args.geo_action == "municipios":
                items = conn.list_municipios(
                    uf=args.uf,
                    regiao=args.regiao,
                    capitais_only=args.capitais,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(items, indent=2, ensure_ascii=False))
                else:
                    print(f"Municipalities ({len(items)} shown):")
                    for it in items:
                        cap = " [Capital]" if it.get("is_capital") else ""
                        print(f"  {it['id_municipio']} - {it['nome_municipio']} ({it['sigla_uf']}){cap}")
                return 0

            if args.geo_action == "municipio":
                rec = conn.get_municipio(args.query, uf=args.uf)
                if not rec:
                    print(f"Municipality '{args.query}' not found.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(rec, indent=2, ensure_ascii=False))
                else:
                    print("Municipality Record:")
                    for k, v in rec.items():
                        print(f"  {k:<25}: {v}")
                return 0

        if args.command == "simples":
            from public_data_gateway.connectors.empresarial.rfb_simples_mei import (
                RFBSimplesMEIConnector,
            )

            conn = RFBSimplesMEIConnector(settings=settings)
            res = conn.get_simples_mei_status(args.cnpj)
            if args.json:
                print(json.dumps(res, indent=2, ensure_ascii=False))
            else:
                print(f"Simples Nacional / MEI Status for CNPJ: {res['cnpj_pesquisado']}")
                print(f"  CNPJ Básico:       {res['cnpj_basico']}")
                print(f"  Opção Simples:     {res['opcao_simples']} ({res['status_simples_atual']})")
                print(f"  Data Opção:        {res['data_opcao_simples'] or '-'}")
                print(f"  Data Exclusão:     {res['data_exclusao_simples'] or '-'}")
                print(f"  Enquadrado MEI:    {res['opcao_simei']} ({res['status_simei_atual']})")
                print(f"  Data Opção MEI:    {res['data_opcao_simei'] or '-'}")
                print(f"  Data Exclusão MEI: {res['data_exclusao_simei'] or '-'}")
            return 0

        if args.command == "pncp":
            from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
                PNCPContratacoesConnector,
            )

            conn = PNCPContratacoesConnector(settings=settings)
            if args.pncp_action == "contratacoes":
                rows = conn.list_contratacoes(
                    uf=args.uf,
                    modalidade_id=args.modalidade_id,
                    ano=args.ano,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(rows, indent=2, ensure_ascii=False))
                else:
                    print(f"PNCP Contratações ({len(rows)} found):")
                    for r in rows:
                        print(
                            f"  {r['numero_controle_pncp']} | {r['sigla_uf']} | {r['modalidade_nome']} | "
                            f"Est: R$ {r['valor_total_estimado']:,.2f} | {r['razao_social_orgao']}"
                        )
                return 0

            if args.pncp_action == "contratos":
                ct_rows = conn.list_contratos(
                    cnpj_orgao=args.cnpj,
                    ni_fornecedor=args.ni,
                    ano=args.ano,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(ct_rows, indent=2, ensure_ascii=False))
                else:
                    print(f"PNCP Contratos ({len(ct_rows)} found):")
                    for r in ct_rows:
                        print(
                            f"  {r['numero_controle_pncp']} | {r['sigla_uf']} | Forn: {r['nome_fornecedor']} ({r['ni_fornecedor']}) | "
                            f"R$ {r['valor_global']:,.2f} | {r['razao_social_orgao']}"
                        )
                return 0

        if args.command == "compliance":
            from public_data_gateway.connectors.compliance.cgu_ceis_cnep.connector import (
                CGUCEISCNEPConnector,
            )

            conn = CGUCEISCNEPConnector(settings=settings)
            if args.comp_action == "status":
                res = conn.get_compliance_status(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Compliance Status for {args.documento}:")
                    print(f"  Status Idoneidade:    {res['status_idoneidade']}")
                    print(f"  Sanção Ativa:         {res['has_active_sanction']}")
                    print(f"  Total Sanções CEIS:   {res['total_sancoes_ceis']} ({res['total_sancoes_ceis_ativas']} ativas)")
                    print(f"  Total Sanções CNEP:   {res['total_sancoes_cnep']} ({res['total_sancoes_cnep_ativas']} ativas)")
                    print(f"  Total Multas CNEP:    R$ {res['valor_total_multas_cnep']:,.2f}")
                return 0

            if args.comp_action == "sancoes":
                table_map = {
                    "ceis": ["cgu_ceis"],
                    "cnep": ["cgu_cnep"],
                    "cepim": ["cgu_cepim"],
                }
                tables_to_query = table_map.get(args.cadastro, ["cgu_ceis", "cgu_cnep"])
                all_rows = []
                for t in tables_to_query:
                    rows = conn.extract_table(t)
                    for r in rows:
                        if not args.todas and not r.get("is_sancao_ativa", r.get("is_ativo", False)):
                            continue
                        if args.uf and r.get("sigla_uf_orgao") != args.uf.upper():
                            continue
                        r["_fonte_tabela"] = t
                        all_rows.append(r)
                        if len(all_rows) >= args.limit:
                            break
                    if len(all_rows) >= args.limit:
                        break

                if args.json:
                    print(json.dumps(all_rows, indent=2, ensure_ascii=False))
                else:
                    print(f"CGU Sanções ({len(all_rows)} found):")
                    for r in all_rows:
                        doc = r.get("cpf_cnpj_sancionado") or r.get("cnpj_entidade")
                        nome = r.get("nome_sancionado") or r.get("nome_entidade")
                        tipo = r.get("tipo_sancao") or r.get("motivo_impedimento")
                        org = r.get("orgao_sancionador") or r.get("orgao_concedente")
                        print(f"  [{r['_fonte_tabela']}] {doc} | {nome} | {tipo} | Org: {org}")
                return 0

        if args.command == "pep":
            from public_data_gateway.connectors.compliance.cgu_pep.connector import (
                CGUPEPConnector,
            )

            conn = CGUPEPConnector(settings=settings)
            if args.pep_action == "status":
                res = conn.get_pep_status(args.termo)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"PEP Status for '{args.termo}':")
                    print(f"  Status Exposição:     {res['status_exposicao_politica']}")
                    print(f"  É PEP Ativo/Carência: {res['is_pep_ativo']}")
                    print(f"  Score Risco PLD:      {res['score_risco_pld']} ({res['categoria_risco_pld']})")
                    print(f"  Total Mandatos:       {res['total_mandatos_encontrados']}")
                    for m in res.get("mandatos", []):
                        print(f"    - {m['cpf_pep']} | {m['nome_pep']} | {m['descricao_funcao']} | Org: {m['nome_orgao']} | Ativo: {m['is_pep_ativo']}")
                return 0

            if args.pep_action == "list":
                rows = conn.list_pep(
                    uf=args.uf,
                    sigla_funcao=args.funcao,
                    ativos_only=not args.todos,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(rows, indent=2, ensure_ascii=False))
                else:
                    print(f"PEP Registros ({len(rows)} found):")
                    for r in rows:
                        print(f"  {r['cpf_pep']} | {r['nome_pep']} | {r['sigla_funcao']} ({r['descricao_funcao']}) | {r['nome_orgao']} ({r['sigla_uf_orgao']})")
                return 0

        if args.command == "judicial":
            from public_data_gateway.connectors.judicial.cnj_datajud.connector import (
                CNJDataJudConnector,
            )

            conn = CNJDataJudConnector(settings=settings)
            if args.jud_action == "risk":
                res = conn.get_litigation_risk(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Litigation Risk Profile for '{args.documento}':")
                    print(f"  Risco Judicial:       {res['categoria_risco_judicial']}")
                    print(f"  Score Litigiosidade:  {res['score_litigiosidade_juridica']}")
                    print(f"  Total Processos:      {res['total_processos']} ({res['total_processos_ativos']} ativos)")
                    print(f"  Execuções Fiscais:    {res['total_execucoes_fiscais']}")
                    print(f"  Ações Trabalhistas:   {res['total_acoes_trabalhistas']}")
                    print(f"  Passivo Contingente:  R$ {res['valor_total_causas_contingentes']:,.2f}")
                return 0

            if args.jud_action == "search":
                rows = conn.search_processos(
                    termo=args.termo,
                    tribunal=args.tribunal,
                    limit=args.limit,
                )
                if args.json:
                    print(json.dumps(rows, indent=2, ensure_ascii=False))
                else:
                    print(f"DataJud Processos ({len(rows)} found):")
                    for r in rows:
                        print(f"  {r['numero_processo']} | {r['tribunal_sigla']} ({r['grau_jurisdicao']}) | {r['classe_nome']} | R$ {r.get('valor_causa', 0.0):,.2f} | {r['polo_passivo_nome']}")
                return 0

            if args.jud_action == "cndt":
                from public_data_gateway.connectors.judicial.tst_cndt.connector import (
                    TstCndtConnector,
                )
                tst_conn = TstCndtConnector(settings=settings)
                res = tst_conn.get_cndt(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"CNDT Certificate for '{args.documento}':")
                    print(f"  Certidão Número:   {res.get('numero_certidao', 'N/A')}")
                    print(f"  Razão Social/Nome: {res.get('razao_social_ou_nome', 'N/A')}")
                    print(f"  Situação:          {res.get('status_certidao', 'N/A')}")
                    print(f"  Regular:           {res.get('is_regular', False)}")
                    print(f"  Validade:          {res.get('data_validade', 'N/A')}")
                    print(f"  Autenticidade:     {res.get('codigo_autenticidade', 'N/A')}")
                return 0

            if args.jud_action == "labor-risk":
                from public_data_gateway.connectors.judicial.tst_cndt.connector import (
                    TstCndtConnector,
                )
                tst_conn = TstCndtConnector(settings=settings)
                res = tst_conn.get_labor_risk(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Labor Compliance Risk Profile for '{args.documento}':")
                    print(f"  Razão Social/Nome: {res.get('razao_social_ou_nome', 'N/A')}")
                    print(f"  Status CNDT:       {res.get('ultimo_status_cndt', 'N/A')}")
                    print(f"  Certidão Válida:   {res.get('is_cndt_valida', False)}")
                    print(f"  Risco Trabalhista: {res.get('labor_risk_class', 'N/A')}")
                    print(f"  Labor Risk Score:  {res.get('labor_risk_score', 0.0)}")
                    print(f"  Processos BNDT:    {res.get('qtd_processos_bndt_ativos', 0)}")
                    print(f"  Passivo Total:     R$ {res.get('valor_total_passivo_trabalhista', 0.0):,.2f}")
                return 0

            if args.jud_action == "debitos":
                from public_data_gateway.connectors.judicial.tst_cndt.connector import (
                    TstCndtConnector,
                )
                tst_conn = TstCndtConnector(settings=settings)
                rows = tst_conn.search_debitos(args.termo)
                if args.json:
                    print(json.dumps(rows, indent=2, ensure_ascii=False))
                else:
                    print(f"Found {len(rows)} BNDT debtor entries matching '{args.termo}':")
                    for r in rows:
                        print(f"  - [{r.get('tribunal_origem')}] NUP: {r.get('numero_processo_nup')} | R$ {r.get('valor_debito', 0.0):,.2f} | {r.get('situacao_exigibilidade')}")
                return 0

        if args.command == "justica-aberta":
            from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
                CNJJusticaAbertaConnector,
            )

            ja_conn = CNJJusticaAbertaConnector(settings=settings)
            ja_action = getattr(args, "justica_aberta_action", None)
            if ja_action == "offices":
                res = ja_conn.get_notary_offices_registry(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"CNJ Justiça Aberta Serventias ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Serventias Ativas: {res.get('total_serventias_ativas')} ({res.get('taxa_provimento_titular_pct')}% providas titular)")
                    print(f"  Registro Imóveis:  {res.get('oficios_registro_imoveis')} | Notas/Protesto: {res.get('tabelionatos_notas_protesto')} | RCPN: {res.get('oficios_registro_civil_rcpn')}")
                return 0

            if ja_action == "productivity":
                rows = ja_conn.list_productivity_by_attribution(
                    args.id_municipio, atribuicao_cartorial=getattr(args, "atribuicao", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"CNJ Justiça Aberta Produtividade por Atribuição ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('atribuicao_cartorial')}: {r.get('atos_praticados_semestre')} atos | {r.get('proporcao_atos_eletronicos_e_notariado_pct')}% eletrônico | prazo {r.get('prazo_medio_emissao_dias')}d")
                return 0

            if ja_action == "emoluments":
                res = ja_conn.get_emoluments_revenue(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"CNJ Justiça Aberta Emolumentos ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Arrecadação Bruta: R$ {res.get('arrecadacao_bruta_emolumentos_brl', 0.0):,.2f}")
                    print(f"  Média/Serventia:   R$ {res.get('arrecadacao_media_por_serventia_brl', 0.0):,.2f} | Ticket/Ato: R$ {res.get('ticket_medio_emolumento_por_ato_brl', 0.0):,.2f}")
                return 0

            if ja_action == "digitalization":
                res = ja_conn.get_digitalization_compliance(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"CNJ Justiça Aberta Digitalização ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Acervo Digitaliz.: {res.get('acervo_historico_digitalizado_pct')}% | Integração SREI/ONR/CRC: {res.get('serventias_integradas_srei_onr_crc_pct')}%")
                    print(f"  Transparência CNJ: {res.get('indice_transparencia_prestacao_contas_cnj_pct')}% | Prepostos: {res.get('forca_trabalho_prepostos_escreventes')}")
                return 0

            if ja_action == "features":
                feat = ja_conn.get_justica_aberta_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"CNJ Justiça Aberta Features Cartoriais: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Eficiência:    {feat.get('score_eficiencia_modernizacao_cartorial')}/100.0")
                    print(f"  Score Dinamismo:     {feat.get('score_dinamismo_notarial_imobiliario')}/100.0")
                    print(f"  Classificação Polo:  {feat.get('classificacao_polo_notarial_cartorial')}")
                return 0

            if ja_action == "ranking":
                ranks = ja_conn.list_top_notary_efficiency_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Polos Notariais & Registrais (CNJ Justiça Aberta):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Eficiência {r.get('score_eficiencia_modernizacao_cartorial')} | Dinamismo: {r.get('score_dinamismo_notarial_imobiliario')} | {r.get('classificacao_polo_notarial_cartorial')}")
                return 0

        if args.command in {"company", "suframa", "tax-incentives"}:
            from public_data_gateway.connectors.empresarial.suframa_pim_zfm.connector import (
                SuframaPimZfmConnector,
            )
            suf_conn = SuframaPimZfmConnector(settings=settings)

            action = getattr(args, "company_action", None) or args.command
            if action in {"suframa"}:
                res = suf_conn.get_suframa_status(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"SUFRAMA Registration Status for '{args.documento}':")
                    print(f"  Razão Social:        {res.get('razao_social', 'N/A')}")
                    print(f"  CNPJ:                {res.get('cnpj', 'N/A')}")
                    print(f"  Inscrição SUFRAMA:   {res.get('numero_inscricao_suframa', 'N/A')}")
                    print(f"  Situação Cadastral:  {res.get('situacao_cadastral', 'N/A')}")
                    print(f"  Incentivo Aprovado:  {res.get('tem_incentivo_fiscal_aprovado', False)}")
                    print(f"  Área de Atuação:     {res.get('tipo_area_incentivada', 'N/A')}")
                    print(f"  Projetos Industriais:{res.get('qtd_projetos_industriais', 0)}")
                return 0

            if action in {"tax-incentives"}:
                res = suf_conn.get_tax_incentives_profile(args.cnpj)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Corporate Fiscal Incentive Profile for '{args.cnpj}':")
                    print(f"  Razão Social:       {res.get('razao_social', 'N/A')}")
                    print(f"  Score Incentivo:    {res.get('tax_incentive_score', 0.0)}/100.0")
                    print(f"  Classificação:      {res.get('tax_incentive_class', 'N/A')}")
                    print(f"  Cumpre PPB:         {res.get('cumpre_processo_produtivo_basico', False)}")
                    print(f"  Capacidade Fabril:  {res.get('capacidade_fabril_estimada', 'N/A')}")
                    print(f"  Benefícios Fiscais: {', '.join(res.get('beneficios_habilitados', [])) or 'Nenhum'}")
                return 0

            if action in {"pim-projects"}:
                projs = suf_conn.list_projetos(cnpj_or_inscricao=args.cnpj, status=args.status)
                if args.json:
                    print(json.dumps(projs, indent=2, ensure_ascii=False))
                else:
                    print(f"Found {len(projs)} SUFRAMA CAS industrial projects:")
                    for p in projs:
                        print(f"  - [{p.get('codigo_projeto')}] {p.get('titulo_projeto', p.get('nome_projeto', 'N/A'))} | {p.get('status_projeto')} | Resolução: {p.get('resolucao_aprovacao_cas', 'N/A')}")
                return 0

        if args.command in {"finance", "cvm", "ratios"}:
            from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr.connector import (
                CvmDfpItrConnector,
            )
            cvm_conn = CvmDfpItrConnector(settings=settings)

            action = getattr(args, "finance_action", None) or args.command
            if action in {"cvm"}:
                res = cvm_conn.get_company(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"CVM Public Company Profile for '{args.documento}':")
                    print(f"  Razão Social:        {res.get('razao_social', 'N/A')}")
                    print(f"  Código CVM:          {res.get('codigo_cvm', 'N/A')}")
                    print(f"  CNPJ:                {res.get('cnpj_14', 'N/A')}")
                    print(f"  Setor de Atividade:  {res.get('setor_atividade', 'N/A')}")
                    print(f"  Situação Registro:   {res.get('situacao_registro', 'N/A')}")
                    print(f"  Categoria Registro:  {res.get('categoria_registro', 'N/A')}")
                    print(f"  Segmento Mercado:    {res.get('tipo_mercado', 'N/A')}")
                return 0

            if action in {"ratios"}:
                res = cvm_conn.get_financial_ratios(args.documento)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Financial Ratios & Solvency for '{args.documento}':")
                    print(f"  Razão Social:       {res.get('razao_social', 'N/A')}")
                    print(f"  Liquidez Corrente:  {res.get('liquidez_corrente', 0.0)}")
                    print(f"  Liquidez Seca:      {res.get('liquidez_seca', 0.0)}")
                    print(f"  Margem Bruta:       {res.get('margem_bruta_pct', 0.0)}%")
                    print(f"  Margem Líquida:     {res.get('margem_liquida_pct', 0.0)}%")
                    print(f"  ROE:                {res.get('roe_pct', 0.0)}%")
                    print(f"  Endividamento Geral:{res.get('endividamento_geral_pct', 0.0)}%")
                    print(f"  Score Solvência:    {res.get('score_solvencia_fundamentalista', 0.0)}/100.0")
                    print(f"  Classificação:      {res.get('classificacao_solvencia', 'N/A')}")
                return 0

            if action in {"list-cvm"}:
                cias = cvm_conn.list_companies(setor=args.setor, situacao=args.situacao)
                if args.json:
                    print(json.dumps(cias, indent=2, ensure_ascii=False))
                else:
                    print(f"Found {len(cias)} CVM registered companies:")
                    for c in cias:
                        print(f"  - [{c.get('codigo_cvm')}] {c.get('razao_social')} | {c.get('setor_atividade')} | {c.get('situacao_registro')}")
                return 0

        elif args.command in {"trade", "comex"}:
            from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
                MDICComexStatConnector,
            )

            comex_conn = MDICComexStatConnector(settings=settings)

            if args.command == "comex":
                query_term = args.termo.strip().replace(".", "")
                if len(query_term) == 8:
                    res = comex_conn.get_ncm_trade(query_term)
                else:
                    res = comex_conn.get_municipality_trade(query_term)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Comex Stat Profile for '{args.termo}':")
                    for k, v in res.items():
                        print(f"  {k}: {v}")
                return 0

            trade_action = getattr(args, "trade_action", None)
            if trade_action == "municipality":
                res = comex_conn.get_municipality_trade(args.id_municipio)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"Municipality Foreign Trade for '{args.id_municipio}':")
                    for k, v in res.items():
                        print(f"  {k}: {v}")
                return 0

            if trade_action == "ncm":
                res = comex_conn.get_ncm_trade(args.codigo_ncm)
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"NCM Trade Profile for '{args.codigo_ncm}':")
                    for k, v in res.items():
                        print(f"  {k}: {v}")
                return 0

            if trade_action == "balance":
                res = comex_conn.get_trade_balance_profile()
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print("Brazilian Foreign Trade Balance Profile:")
                    for k, v in res.items():
                        print(f"  {k}: {v}")
                return 0

        elif args.command in {"funds", "cvm-fundos"}:
            from public_data_gateway.connectors.macrofinanceiro.cvm_fundos.connector import (
                CvmFundosConnector,
            )

            fund_conn = CvmFundosConnector(settings=settings)

            if args.command == "cvm-fundos":
                res = fund_conn.get_fund_by_cnpj(args.cnpj)
                if not res:
                    print(f"Investment fund not found for CNPJ '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"CVM Investment Fund: {res.get('razao_social')} [{res.get('cnpj_14')}]")
                    print(f"  Classe:        {res.get('classe_cvm')} ({res.get('classe_anbima')})")
                    print(f"  Situação:      {res.get('situacao_registro')}")
                    print(f"  Gestor:        {res.get('nome_gestor')}")
                    print(f"  Administrador: {res.get('nome_administrador')}")
                    print(f"  PL Referência: R$ {res.get('patrimonio_liquido', 0.0):,.2f}")
                return 0

            funds_action = getattr(args, "funds_action", None)
            if funds_action == "profile":
                res = fund_conn.get_fund_by_cnpj(args.cnpj)
                if not res:
                    print(f"Investment fund not found for CNPJ '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"CVM Investment Fund Profile: {res.get('razao_social')}")
                    for k, v in res.items():
                        print(f"  {k}: {v}")
                return 0

            if funds_action == "daily":
                reports = fund_conn.get_daily_reports(args.cnpj, start_date=args.start, end_date=args.end)
                if args.json:
                    print(json.dumps(reports, indent=2, ensure_ascii=False))
                else:
                    print(f"Daily Reports for Fund {args.cnpj} ({len(reports)} records):")
                    for rep in reports:
                        print(f"  {rep.get('data_competencia')}: Cota={rep.get('valor_cota')} | PL=R$ {rep.get('patrimonio_liquido', 0.0):,.2f} | Captação Liq=R$ {rep.get('captacao_liquida_dia', 0.0):,.2f} | Cotistas={rep.get('numero_cotistas')}")
                return 0

            if funds_action == "managers":
                managers = fund_conn.get_asset_managers(query=args.query, min_aum=args.min_aum)
                if args.json:
                    print(json.dumps(managers, indent=2, ensure_ascii=False))
                else:
                    print(f"Asset Managers Ranking ({len(managers)} firms):")
                    for idx, m in enumerate(managers, 1):
                        print(f"  {idx}. {m.get('nome_gestor')} | AuM: R$ {m.get('patrimonio_liquido_total', 0.0):,.2f} | Fundos: {m.get('total_fundos_geridos')}")
                return 0

            if funds_action == "features":
                feat = fund_conn.get_fund_features(args.cnpj)
                if not feat:
                    print(f"Feature profile not found for fund CNPJ '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(feat, indent=2, ensure_ascii=False))
                else:
                    print(f"Gold Features for {feat.get('razao_social')} [{feat.get('cnpj_14')}]:")
                    print(f"  Score Resiliência: {feat.get('score_resiliencia_fundo')}/100.0")
                    print(f"  Perfil Liquidez:   {feat.get('classificacao_liquidez')}")
                    print(f"  Rentabilidade 30d: {feat.get('rentabilidade_30d_pct')}%")
                    print(f"  Volatilidade:      {feat.get('volatilidade_diaria')}%")
                    print(f"  Captação Liq 30d:  R$ {feat.get('captacao_liquida_30d', 0.0):,.2f}")
                return 0

        elif args.command in {"banking", "ifdata"}:
            from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
                BcbIfdataConnector,
            )

            bank_conn = BcbIfdataConnector(settings=settings)

            if args.command == "ifdata":
                res = bank_conn.get_bank_by_cnpj(args.cnpj)
                if not res:
                    print(f"Financial institution not found for '{args.cnpj}'", file=sys.stderr)
                    return 1
                sol = bank_conn.get_bank_solidity(args.cnpj)
                if args.json:
                    output = {"instituicao": res, "solidez": sol}
                    print(json.dumps(output, indent=2, ensure_ascii=False))
                else:
                    uf_str = res.get("uf_sede") or res.get("uf") or "N/A"
                    mun_str = res.get("municipio_sede") or res.get("municipio") or "N/A"
                    print(f"BACEN IF.data Institution: {res.get('nome_instituicao')} [{res.get('cnpj_14_lider')}]")
                    print(f"  Segmento:       {res.get('segmento_prudencial')}")
                    print(f"  Tipo:           {res.get('tipo_instituicao')}")
                    print(f"  Localização:    {mun_str}/{uf_str}")
                    if sol:
                        bas_pct = sol.get("indice_basileia_pct") or sol.get("indice_basileia") or 0.0
                        print(f"  Score Solidez:  {sol.get('score_solidez_bancaria')}/100.0")
                        print(f"  Porte:          {sol.get('classificacao_porte_bancario')}")
                        print(f"  Ativo Total:    R$ {sol.get('ativo_total', 0.0):,.2f}")
                        print(f"  Basiléia:       {bas_pct:.1f}%")
                return 0

            banking_action = getattr(args, "banking_action", None)
            if banking_action == "profile":
                res = bank_conn.get_bank_by_cnpj(args.cnpj)
                if not res:
                    print(f"Financial institution not found for '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(res, indent=2, ensure_ascii=False))
                else:
                    print(f"BACEN Prudential Profile: {res.get('nome_instituicao')}")
                    for k, v in res.items():
                        print(f"  {k}: {v}")
                return 0

            if banking_action == "balance":
                sheet = bank_conn.get_bank_balance_sheet(args.cnpj)
                if not sheet:
                    print(f"Balance sheet not found for institution '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(sheet, indent=2, ensure_ascii=False))
                else:
                    inst = sheet.get("instituicao", {})
                    atv = sheet.get("ativo", {})
                    pas = sheet.get("passivo_e_pl", {})
                    dre = sheet.get("dre", {})
                    ind = sheet.get("indicadores_prudenciais", {})
                    print(f"Quarterly Balance Sheet for {inst.get('nome_instituicao')} [{inst.get('cnpj_14_lider')}]:")
                    print(f"  Ativo Total:                R$ {atv.get('ativo_total', 0.0):,.2f}")
                    print(f"  Carteira Crédito:           R$ {atv.get('carteira_credito_classificada', 0.0):,.2f}")
                    print(f"  Títulos e Val. Mobiliários: R$ {atv.get('titulos_valores_mobiliarios', 0.0):,.2f}")
                    print(f"  Depósitos Totais:           R$ {pas.get('depositos_totais', 0.0):,.2f}")
                    print(f"  Patrimônio Líquido:         R$ {pas.get('patrimonio_liquido', 0.0):,.2f}")
                    print(f"  Lucro Líquido Trimestre:    R$ {dre.get('lucro_liquido', 0.0):,.2f}")
                    print(f"  Índice de Basiléia:         {ind.get('indice_basileia')}%")
                    print(f"  Inadimplência > 90 dias:    {ind.get('inadimplencia_maior_90d')}%")
                return 0

            if banking_action == "ranking":
                ranking = bank_conn.get_banking_ranking(segmento=args.segmento, top=args.top)
                if args.json:
                    print(json.dumps(ranking, indent=2, ensure_ascii=False))
                else:
                    seg_label = f" [Segmento {args.segmento}]" if args.segmento else ""
                    print(f"Top {len(ranking)} Brazilian Financial Institutions by Total Assets{seg_label}:")
                    for idx, b in enumerate(ranking, 1):
                        bas_v = b.get("indice_basileia_pct") or b.get("indice_basileia") or 0.0
                        score_v = b.get("score_solidez_bancaria") or 0.0
                        print(f"  {idx:2d}. {b.get('nome_instituicao'):<30} | {b.get('segmento_prudencial')} | Ativo: R$ {b.get('ativo_total', 0.0):>18,.2f} | Basiléia: {bas_v:>5.1f}% | Score: {score_v:>5.1f}")
                return 0

            if banking_action == "solidity":
                sol = bank_conn.get_bank_solidity(args.cnpj)
                if not sol:
                    print(f"Solidity profile not found for institution '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(sol, indent=2, ensure_ascii=False))
                else:
                    bas_v = sol.get("indice_basileia_pct") or sol.get("indice_basileia") or 0.0
                    imob_v = sol.get("indice_imobilizacao_pct") or sol.get("indice_imobilizacao") or 0.0
                    roe_v = sol.get("roe_bancario_pct") or sol.get("roe_bancario_anualizado") or 0.0
                    inad_v = sol.get("taxa_inadimplencia_pct") or sol.get("inadimplencia_maior_90d") or 0.0
                    print(f"Gold Solidity & Resilience Features for {sol.get('nome_instituicao')} [{sol.get('cnpj_14_lider')}]:")
                    print(f"  Score Solidez Bancária:    {sol.get('score_solidez_bancaria')}/100.0")
                    print(f"  Classificação de Porte:    {sol.get('classificacao_porte_bancario')}")
                    print(f"  Índice de Basiléia:        {bas_v}% (Mínimo CMN: 10.5%)")
                    print(f"  Índice de Imobilização:    {imob_v}% (Máximo CMN: 50.0%)")
                    print(f"  ROE Bancário Anualizado:   {roe_v}%")
                    print(f"  Inadimplência Carteira:    {inad_v}%")
                return 0

        elif args.command in {"treasury", "sadipem"}:
            from public_data_gateway.connectors.macrofinanceiro.stn_tesouro_transparente.connector import (
                StnTesouroTransparenteConnector,
            )

            treas_conn = StnTesouroTransparenteConnector(settings=settings)

            if args.command == "sadipem":
                pvls = treas_conn.get_credit_operations_pvl(args.termo)
                prof = treas_conn.get_municipio_fiscal_profile(args.termo)
                if args.json:
                    out = {"perfil_fiscal": prof, "operacoes_credito": pvls}
                    print(json.dumps(out, indent=2, ensure_ascii=False))
                else:
                    if prof:
                        print(f"STN Fiscal Health Profile: {prof.get('nome_municipio')} [{prof.get('sigla_uf')} - {prof.get('id_municipio_ibge_7d')}]:")
                        print(f"  Classificação CAPAG:     Nota {prof.get('nota_capag_final')}")
                        print(f"  Score Saúde Fiscal:      {prof.get('score_saude_fiscal')}/100.0 ({prof.get('classificacao_saude_fiscal')})")
                        print(f"  Receita Corrente (RCL):  R$ {prof.get('receita_corrente_liquida_rcl', 0.0):,.2f}")
                        print(f"  Dívida Líquida (DCL):    R$ {prof.get('divida_consolidada_liquida_dcl', 0.0):,.2f} ({prof.get('razao_dcl_rcl_pct')}%)")
                        print(f"  FPM Anual Recebido:      R$ {prof.get('total_fpm_anual', 0.0):,.2f}")
                    if pvls:
                        print(f"\nSADIPEM Credit Operations for '{args.termo}' ({len(pvls)} pleitos):")
                        for idx, p in enumerate(pvls, 1):
                            print(f"  {idx}. {p.get('num_pvl')} | R$ {p.get('valor_operacao', 0.0):,.2f} | {p.get('status_pleito')} | Credor: {p.get('credor')} | Finalidade: {p.get('finalidade')}")
                    elif not prof:
                        print(f"No records found in STN SADIPEM for '{args.termo}'", file=sys.stderr)
                        return 1
                return 0

            treasury_action = getattr(args, "treasury_action", None)
            if treasury_action == "profile":
                prof = treas_conn.get_municipio_fiscal_profile(args.termo)
                if not prof:
                    print(f"Fiscal profile not found for '{args.termo}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"STN Fiscal Health Profile for {prof.get('nome_municipio')} [{prof.get('sigla_uf')}]:")
                    for k, v in prof.items():
                        print(f"  {k}: {v}")
                return 0

            if treasury_action == "pvl":
                pvls = treas_conn.get_credit_operations_pvl(args.termo, status=args.status)
                if args.json:
                    print(json.dumps(pvls, indent=2, ensure_ascii=False))
                else:
                    print(f"SADIPEM Credit Operations for '{args.termo}' ({len(pvls)} records):")
                    for idx, p in enumerate(pvls, 1):
                        print(f"  {idx}. {p.get('num_pvl')} | R$ {p.get('valor_operacao', 0.0):,.2f} | {p.get('status_pleito')} | Credor: {p.get('credor')}")
                return 0

            if treasury_action == "fpm":
                fpms = treas_conn.get_fpm_transfers(args.id_municipio, ano=args.ano)
                if args.json:
                    print(json.dumps(fpms, indent=2, ensure_ascii=False))
                else:
                    print(f"FPM Constitutional Transfers for '{args.id_municipio}' ({len(fpms)} records):")
                    for f in fpms[:6]:
                        print(f"  {f.get('ano_exercicio')}-{f.get('mes_competencia'):02d} D{f.get('decendio')}: Bruto=R$ {f.get('valor_bruto_fpm', 0.0):,.2f} | FUNDEB=-R$ {f.get('deducao_fundeb', 0.0):,.2f} | Liq=R$ {f.get('valor_liquido_fpm', 0.0):,.2f}")
                    if len(fpms) > 6:
                        print(f"  ... and {len(fpms) - 6} more decêndios")
                return 0

            if treasury_action == "capag":
                cap = treas_conn.get_capag_rating(args.termo)
                if not cap:
                    print(f"CAPAG rating not found for '{args.termo}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(cap, indent=2, ensure_ascii=False))
                else:
                    print(f"STN CAPAG Payment Capacity: {cap.get('nome_ente')} [{cap.get('sigla_uf')}]:")
                    print(f"  Nota Final CAPAG:        {cap.get('nota_capag_final')}")
                    print(f"  Ind 1 (Endividamento):   {cap.get('indicador_1_endividamento_pct')}% (Nota {cap.get('nota_indicador_1')})")
                    print(f"  Ind 2 (Poupança Corr):   {cap.get('indicador_2_poupanca_corrente_pct')}% (Nota {cap.get('nota_indicador_2')})")
                    print(f"  Ind 3 (Liquidez Rel):    {cap.get('indicador_3_liquidez_relativa_pct')}% (Nota {cap.get('nota_indicador_3')})")
                return 0

        elif args.command in {"procurement", "catmat"}:
            from public_data_gateway.connectors.compras_publicas.comprasgov_catmat import (
                ComprasGovCatmatConnector,
            )

            cat_conn = ComprasGovCatmatConnector(settings=settings)

            if args.command == "catmat":
                results = cat_conn.search_materials(args.termo, limit=args.limit)
                if not results:
                    bench = cat_conn.get_price_benchmark(args.termo)
                    if bench:
                        results = [bench]

                if args.json:
                    print(json.dumps(results, indent=2, ensure_ascii=False))
                else:
                    print(f"Compras.gov.br CATMAT results for '{args.termo}' ({len(results)} found):")
                    for idx, r in enumerate(results, 1):
                        if "codigo_item" in r:
                            print(f"  {idx}. Item {r.get('codigo_item')} | PDM: {r.get('nome_pdm')} | {r.get('descricao_item')[:60]}")
                        else:
                            print(f"  {idx}. PDM/Item: {r.get('codigo_item_ou_pdm')} | R$ {r.get('preco_medio_praticado', 0.0):,.2f} | Risco: {r.get('rating_risco_sobrepreco')}")
                return 0

            proc_action = getattr(args, "procurement_action", None)

            if proc_action == "materials":
                mats = cat_conn.search_materials(args.termo, limit=args.limit)
                if args.json:
                    print(json.dumps(mats, indent=2, ensure_ascii=False))
                else:
                    print(f"CATMAT Materials for '{args.termo}' ({len(mats)} records):")
                    for idx, m in enumerate(mats, 1):
                        sust = " [SUSTENTÁVEL]" if m.get("item_sustentavel") else ""
                        print(f"  {idx}. Item {m.get('codigo_item')} | PDM: {m.get('nome_pdm')} | {m.get('descricao_item')[:70]}{sust}")
                return 0

            if proc_action == "services":
                servs = cat_conn.search_services(args.termo, limit=args.limit)
                if args.json:
                    print(json.dumps(servs, indent=2, ensure_ascii=False))
                else:
                    print(f"CATSER Services for '{args.termo}' ({len(servs)} records):")
                    for idx, s in enumerate(servs, 1):
                        print(f"  {idx}. Serv {s.get('codigo_servico')} | {s.get('nome_servico')[:80]}")
                return 0

            if proc_action == "benchmark":
                bench = cat_conn.get_price_benchmark(args.termo)
                if not bench:
                    print(f"Price benchmark not found for '{args.termo}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(bench, indent=2, ensure_ascii=False))
                else:
                    print(f"Compras.gov.br Price Benchmark: {bench.get('descricao_padronizada')} ({bench.get('codigo_item_ou_pdm')}):")
                    print(f"  Preço Médio:             R$ {bench.get('preco_medio_praticado', 0.0):,.2f}")
                    print(f"  Faixa de Preço:          R$ {bench.get('preco_minimo', 0.0):,.2f} - R$ {bench.get('preco_maximo', 0.0):,.2f}")
                    print(f"  Coef. Variação (CV):     {bench.get('coeficiente_variacao_preco_pct')}%")
                    print(f"  Risco de Sobrepreço:     {bench.get('rating_risco_sobrepreco')}")
                    print(f"  Volume Financeiro Total: R$ {bench.get('volume_financeiro_total', 0.0):,.2f}")
                    print(f"  Total Contratações:      {bench.get('total_contratacoes')}")
                    print(f"  Fornecedores Distintos:  {bench.get('total_fornecedores_distintos')}")
                return 0

            if proc_action == "supplier":
                sup = cat_conn.get_supplier_procurement_history(args.cnpj)
                if not sup:
                    print(f"Supplier not found for CNPJ '{args.cnpj}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(sup, indent=2, ensure_ascii=False))
                else:
                    print(f"Government Procurement Supplier: {sup.get('nome_fornecedor')} ({sup.get('cnpj_fornecedor')}):")
                    print(f"  Total Itens Fornecidos:  {sup.get('total_itens_fornecidos')}")
                    print(f"  Valor Total Fornecido:   R$ {sup.get('valor_total_fornecido', 0.0):,.2f}")
                    print(f"  Ticket Médio Unitário:   R$ {sup.get('ticket_medio_unitario', 0.0):,.2f}")
                    print(f"  UASGs Atendidas:         {sup.get('total_uasgs_atendidas')}")
                    print(f"  UF Principal:            {sup.get('sigla_uf_principal')}")
                return 0

        elif args.command in ("sidra", "pib"):
            from public_data_gateway.connectors.demografico.ibge_sidra.connector import (
                IBGESidraConnector,
            )

            sidra_conn = IBGESidraConnector()

            if args.command == "pib":
                prof = sidra_conn.get_municipality_socioeconomic_profile(args.id_municipio)
                if not prof:
                    print(f"Municipality not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE SIDRA Socioeconomic Profile: {prof.get('nome_municipio')} ({prof.get('sigla_uf')} - {prof.get('id_municipio')}):")
                    print(f"  PIB Total:               R$ {prof.get('pib_mil_reais', 0.0):,.2f} mil")
                    print(f"  População Residente:     {prof.get('populacao_residente'):,} habitantes")
                    print(f"  PIB per Capita:          R$ {prof.get('pib_per_capita_reais', 0.0):,.2f}")
                    print(f"  Perfil Econômico:        {prof.get('perfil_economico_dominante')}")
                    print(f"  Porte do Município:      {prof.get('classificacao_porte_municipio')}")
                    print(f"  Participação PIB Brasil: {prof.get('participacao_pib_brasil_pct')}%")
                    print(f"  Score de Dinamismo:      {prof.get('score_dinamismo_economico')}/100")
                return 0

            sidra_action = getattr(args, "sidra_action", None)

            if sidra_action == "gdp":
                gdp_data = sidra_conn.get_municipality_gdp(args.id_municipio)
                if not gdp_data:
                    print(f"GDP data not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(gdp_data, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE Municipal GDP: {gdp_data.get('nome_municipio')} ({gdp_data.get('sigla_uf')} - {gdp_data.get('id_municipio')}):")
                    print(f"  Ano:                     {gdp_data.get('ano')}")
                    print(f"  PIB Total:               R$ {gdp_data.get('pib_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Agropecuária:        R$ {gdp_data.get('vab_agropecuaria_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Indústria:           R$ {gdp_data.get('vab_industria_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Serviços:            R$ {gdp_data.get('vab_servicos_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Administração Públ.: R$ {gdp_data.get('vab_administracao_publica_mil_reais', 0.0):,.2f} mil")
                    print(f"  Participação Brasil:     {gdp_data.get('participacao_pib_brasil_pct')}%")
                return 0

            if sidra_action == "profile":
                prof = sidra_conn.get_municipality_socioeconomic_profile(args.id_municipio)
                if not prof:
                    print(f"Municipality not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE SIDRA Socioeconomic Profile: {prof.get('nome_municipio')} ({prof.get('sigla_uf')} - {prof.get('id_municipio')}):")
                    print(f"  PIB Total:               R$ {prof.get('pib_mil_reais', 0.0):,.2f} mil")
                    print(f"  População Residente:     {prof.get('populacao_residente'):,} habitantes")
                    print(f"  PIB per Capita:          R$ {prof.get('pib_per_capita_reais', 0.0):,.2f}")
                    print(f"  Perfil Econômico:        {prof.get('perfil_economico_dominante')}")
                    print(f"  Porte do Município:      {prof.get('classificacao_porte_municipio')}")
                    print(f"  Participação PIB Brasil: {prof.get('participacao_pib_brasil_pct')}%")
                    print(f"  Score de Dinamismo:      {prof.get('score_dinamismo_economico')}/100")
                return 0

            if sidra_action == "ipca":
                ipca_data = sidra_conn.get_ipca_history(limit=args.limit)
                if args.json:
                    print(json.dumps(ipca_data, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE IPCA Inflation Series (Latest {len(ipca_data)} months):")
                    for row in ipca_data:
                        print(f"  {row.get('ano_mes')} ({row.get('mes_ano_extenso')}): Mensal {row.get('ipca_variacao_mensal_pct')}% | 12 Meses {row.get('ipca_acumulado_12m_pct')}% | Ano {row.get('ipca_acumulado_ano_pct')}%")
                return 0

            if sidra_action == "gini":
                gini_data = sidra_conn.get_gini_uf(args.sigla_uf)
                if not gini_data:
                    print(f"State not found for acronym '{args.sigla_uf}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(gini_data, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE PNADC Gini Index & Income: {gini_data.get('nome_uf')} ({gini_data.get('sigla_uf')}):")
                    print(f"  Índice de Gini:          {gini_data.get('indice_gini_rendimento')}")
                    print(f"  Rendimento Domiciliar:   R$ {gini_data.get('rendimento_medio_domiciliar_reais', 0.0):,.2f}")
                return 0

            if sidra_action == "ranking":
                ranks = sidra_conn.list_top_municipalities_by_gdp(limit=args.limit)
                if args.json:
                    print(json.dumps(ranks, indent=2, ensure_ascii=False))
                else:
                    print(f"Top {len(ranks)} Municipalities by GDP (IBGE SIDRA):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): R$ {r.get('pib_mil_reais', 0.0):,.2f} mil | Per Capita: R$ {r.get('pib_per_capita_reais', 0.0):,.2f}")
                return 0

        elif args.command in ("censo", "censo2022"):
            from public_data_gateway.connectors.demografico.ibge_censo2022.connector import (
                IBGECenso2022Connector,
            )

            censo_conn = IBGECenso2022Connector(settings=settings)

            if args.command == "censo2022":
                prof = censo_conn.get_demographic_profile(args.id_municipio)
                if not prof:
                    print(f"Demographic profile not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE Censo 2022 Profile: {prof.get('nome_municipio')} ({prof.get('sigla_uf')} - {prof.get('id_municipio')}):")
                    print(f"  População Residente:     {prof.get('populacao_residente'):,} habitantes")
                    print(f"  Domicílios Ocupados:     {prof.get('total_domicilios_particulares_ocupados'):,}")
                    print(f"  Idade Mediana:           {prof.get('idade_mediana_anos')} anos")
                    print(f"  Índice Envelhecimento:   {prof.get('indice_envelhecimento_razao')}")
                    print(f"  Taxa de Alfabetização:   {prof.get('taxa_alfabetizacao_pct')}%")
                    print(f"  Taxa de Urbanização:     {prof.get('taxa_urbanizacao_pct')}%")
                    print(f"  Score Maturidade Social: {prof.get('score_maturidade_social')}/100")
                return 0

            censo_action = getattr(args, "censo_action", None)

            if censo_action == "summary":
                data = censo_conn.get_census_summary(args.id_municipio)
                if not data:
                    print(f"Census summary not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE Censo 2022 Summary: {data.get('nome_municipio')} ({data.get('sigla_uf')} - {data.get('id_municipio')}):")
                    print(f"  População Residente:     {data.get('populacao_residente'):,} habitantes")
                    print(f"  Domicílios Ocupados:     {data.get('total_domicilios_particulares_ocupados'):,}")
                    print(f"  Média Moradores/Domic.:  {data.get('media_moradores_por_domicilio')}")
                    print(f"  Densidade Demográfica:   {data.get('densidade_demografica_hab_km2')} hab/km²")
                return 0

            if censo_action == "profile":
                prof = censo_conn.get_demographic_profile(args.id_municipio)
                if not prof:
                    print(f"Demographic profile not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE Censo 2022 Profile: {prof.get('nome_municipio')} ({prof.get('sigla_uf')} - {prof.get('id_municipio')}):")
                    print(f"  População Residente:     {prof.get('populacao_residente'):,} habitantes")
                    print(f"  Domicílios Ocupados:     {prof.get('total_domicilios_particulares_ocupados'):,}")
                    print(f"  Idade Mediana:           {prof.get('idade_mediana_anos')} anos")
                    print(f"  Índice Envelhecimento:   {prof.get('indice_envelhecimento_razao')}")
                    print(f"  Taxa de Alfabetização:   {prof.get('taxa_alfabetizacao_pct')}%")
                    print(f"  Taxa de Urbanização:     {prof.get('taxa_urbanizacao_pct')}%")
                    print(f"  Score Maturidade Social: {prof.get('score_maturidade_social')}/100")
                return 0

            if censo_action == "literacy":
                data = censo_conn.get_literacy_indicators(args.id_municipio)
                if not data:
                    print(f"Literacy indicators not found for ID '{args.id_municipio}'", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE Censo 2022 Literacy: {data.get('nome_municipio')} ({data.get('sigla_uf')} - {data.get('id_municipio')}):")
                    print(f"  População 15+ Anos:      {data.get('populacao_15_anos_ou_mais'):,}")
                    print(f"  População Alfabetizada:  {data.get('populacao_alfabetizada'):,}")
                    print(f"  Taxa de Alfabetização:   {data.get('taxa_alfabetizacao_pct')}%")
                    print(f"  Taxa de Analfabetismo:   {data.get('taxa_analfabetismo_pct')}%")
                return 0

            if censo_action == "aging":
                ranks = censo_conn.list_highest_aging_municipalities(limit=args.limit)
                if args.json:
                    print(json.dumps(ranks, indent=2, ensure_ascii=False))
                else:
                    print(f"Top {len(ranks)} Municipalities by Aging Index (Censo 2022):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Índice {r.get('indice_envelhecimento_razao')} | Idade Mediana: {r.get('idade_mediana_anos')} anos | {r.get('classificacao_etaria')}")
                return 0

        elif args.command in ("caged", "novo-caged"):
            from public_data_gateway.connectors.trabalho.mte_novo_caged.connector import (
                MTENovoCagedConnector,
            )

            caged_conn = MTENovoCagedConnector(settings=settings)

            if args.command == "novo-caged":
                prof = caged_conn.get_labor_market_profile(args.id_municipio)
                if not prof:
                    print(f"No labor market profile found for municipality {args.id_municipio}.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"Novo CAGED Labor Market Profile: {prof.get('nome_municipio')} ({prof.get('sigla_uf')} - {prof.get('id_municipio')}):")
                    print(f"  Competência Recente:     {prof.get('competencia_recente')}")
                    print(f"  Saldo Líquido Ano:       {prof.get('saldo_liquido_emprego_ano'):,} postos")
                    print(f"  Score Aquecimento:       {prof.get('score_aquecimento_mercado_trabalho')}/100 ({prof.get('classificacao_dinamica_emprego')})")
                    print(f"  Score Vulnerabilidade:   {prof.get('score_vulnerabilidade_emprego')}/100")
                    print(f"  Salário Médio Admissão:  R$ {prof.get('salario_medio_admissao_reais'):,.2f}")
                    print(f"  Taxa de Rotatividade:    {prof.get('taxa_rotatividade_turnover_pct')}%")
                    print(f"  Setor Líder:             {prof.get('setor_lider_geracao_emprego')}")
                return 0

            caged_action = getattr(args, "caged_action", None)

            if caged_action == "summary":
                data = caged_conn.get_employment_summary(args.id_municipio, competencia=args.competencia)
                if not data:
                    print(f"No CAGED employment movements found for municipality {args.id_municipio}.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(f"Novo CAGED Summary: {data.get('nome_municipio')} ({data.get('sigla_uf')} - {data.get('id_municipio')} - {data.get('competencia_movimentacao')}):")
                    print(f"  Total Admissões:         {data.get('total_admissoes'):,}")
                    print(f"  Total Desligamentos:     {data.get('total_desligamentos'):,}")
                    print(f"  Saldo Movimentação:      {data.get('saldo_movimentacao'):,} postos")
                    print(f"  Variação Relativa:       {data.get('variacao_relativa_pct')}%")
                    print(f"  Salário Médio Admissão:  R$ {data.get('salario_medio_admissao_reais'):,.2f}")
                return 0

            if caged_action == "profile":
                prof = caged_conn.get_labor_market_profile(args.id_municipio)
                if not prof:
                    print(f"No labor market profile found for municipality {args.id_municipio}.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"Novo CAGED Labor Market Profile: {prof.get('nome_municipio')} ({prof.get('sigla_uf')} - {prof.get('id_municipio')}):")
                    print(f"  Competência Recente:     {prof.get('competencia_recente')}")
                    print(f"  Saldo Líquido Ano:       {prof.get('saldo_liquido_emprego_ano'):,} postos")
                    print(f"  Score Aquecimento:       {prof.get('score_aquecimento_mercado_trabalho')}/100 ({prof.get('classificacao_dinamica_emprego')})")
                    print(f"  Score Vulnerabilidade:   {prof.get('score_vulnerabilidade_emprego')}/100")
                    print(f"  Salário Médio Admissão:  R$ {prof.get('salario_medio_admissao_reais'):,.2f}")
                    print(f"  Taxa de Rotatividade:    {prof.get('taxa_rotatividade_turnover_pct')}%")
                    print(f"  Setor Líder:             {prof.get('setor_lider_geracao_emprego')}")
                return 0

            if caged_action == "sectors":
                sectors = caged_conn.get_sectoral_employment(args.id_municipio)
                if args.json:
                    print(json.dumps(sectors, indent=2, ensure_ascii=False))
                else:
                    print(f"Novo CAGED Sectoral Dynamics ({args.id_municipio}):")
                    for s in sectors:
                        print(f"  - {s.get('grande_setor_cnae')}: Saldo {s.get('saldo_setorial'):,} | Admissões: {s.get('total_admissoes'):,} | Desligamentos: {s.get('total_desligamentos'):,} | Salário Médio: R$ {s.get('salario_medio_admissao_reais'):,.2f}")
                return 0

            if caged_action == "ranking":
                ranks = caged_conn.list_top_job_creating_municipalities(limit=args.limit)
                if args.json:
                    print(json.dumps(ranks, indent=2, ensure_ascii=False))
                else:
                    print(f"Top {len(ranks)} Municipalities by Net Job Creation (Novo CAGED):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Saldo {r.get('saldo_movimentacao'):,} | Admissões: {r.get('total_admissoes'):,} | Salário Médio: R$ {r.get('salario_medio_admissao_reais'):,.2f}")
                return 0

        elif args.command == "rais":
            from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

            conn = MTERaisConnector(settings=settings)
            action = args.rais_action
            if action == "summary":
                result = conn.get_formal_employment_summary(args.id_municipio, ano=args.ano)
            elif action == "profile":
                result = conn.get_rais_market_profile(args.id_municipio)
            elif action == "sectors":
                result = conn.get_sectoral_employment(args.id_municipio, ano=args.ano)
            else:
                result = conn.list_top_formalized_municipalities(limit=args.limit)
            if not result:
                print("No RAIS data found for the requested query.", file=sys.stderr)
                return 1
            if args.json:
                print(json.dumps(result, indent=2, ensure_ascii=False))
            elif isinstance(result, list):
                print(f"RAIS {action.title()}: {len(result)} municipalities/records")
                for row in result:
                    print(f"  {row.get('nome_municipio', row.get('id_municipio'))}: {row}")
            else:
                print(f"RAIS {action.title()}: {result.get('nome_municipio', args.id_municipio)}")
                for key, value in result.items():
                    print(f"  {key}: {value}")
            return 0

        elif args.command in ("pnad", "pnad-uf"):
            from public_data_gateway.connectors.trabalho.ibge_pnad_continua.connector import (
                IBGEPnadContinuaConnector,
            )

            pnad_conn = IBGEPnadContinuaConnector(settings=settings)

            if args.command == "pnad-uf":
                prof = pnad_conn.get_state_labor_profile(args.uf)
                if not prof:
                    print(f"No PNAD labor profile found for state '{args.uf}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE PNAD Contínua State Profile: {prof.get('nome_uf')} ({prof.get('sigla_uf')} - {prof.get('ano_trimestre_recente')}):")
                    print(f"  Taxa de Desocupação:     {prof.get('taxa_desocupacao_pct')}%")
                    print(f"  Rendimento Médio Real:   R$ {prof.get('rendimento_medio_real_habitual_reais'):,.2f}")
                    print(f"  Massa Salarial Habitual: R$ {prof.get('massa_rendimento_real_habitual_milhoes_reais'):,.2f} milhões")
                    print(f"  Taxa de Informalidade:   {prof.get('taxa_informalidade_pct')}%")
                    print(f"  Taxa de Subutilização:   {prof.get('taxa_composta_subutilizacao_pct')}%")
                    print(f"  Taxa de Desalento:       {prof.get('taxa_desalento_pct')}%")
                    print(f"  Razão Renda Mulher/Homem:{prof.get('razao_rendimento_mulher_homem')}")
                    print(f"  Score Resiliência:       {prof.get('score_resiliencia_mercado_trabalho')}/100 ({prof.get('classificacao_mercado_trabalho_uf')})")
                return 0

            pnad_action = getattr(args, "pnad_action", None)

            if pnad_action == "indicators":
                data = pnad_conn.get_labor_force_indicators(args.uf, ano_trimestre=args.trimestre)
                if not data:
                    print(f"No labor force indicators found for state '{args.uf}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE PNAD Labor Force: {data.get('nome_uf')} ({data.get('sigla_uf')} - {data.get('ano_trimestre')}):")
                    print(f"  População Idade Trabalhar:{data.get('populacao_idade_trabalhar_mil'):,} mil pessoas")
                    print(f"  Força de Trabalho Total:  {data.get('forca_trabalho_total_mil'):,} mil pessoas")
                    print(f"  População Ocupada:        {data.get('populacao_ocupada_mil'):,} mil pessoas")
                    print(f"  População Desocupada:     {data.get('populacao_desocupada_mil'):,} mil pessoas")
                    print(f"  Taxa de Desocupação:      {data.get('taxa_desocupacao_pct')}%")
                    print(f"  Taxa de Participação:     {data.get('taxa_participacao_forca_pct')}%")
                    print(f"  Taxa de Nível Ocupação:   {data.get('taxa_nivel_ocupacao_pct')}%")
                return 0

            if pnad_action == "profile":
                prof = pnad_conn.get_state_labor_profile(args.uf)
                if not prof:
                    print(f"No PNAD labor profile found for state '{args.uf}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(prof, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE PNAD Contínua State Profile: {prof.get('nome_uf')} ({prof.get('sigla_uf')} - {prof.get('ano_trimestre_recente')}):")
                    print(f"  Taxa de Desocupação:     {prof.get('taxa_desocupacao_pct')}%")
                    print(f"  Rendimento Médio Real:   R$ {prof.get('rendimento_medio_real_habitual_reais'):,.2f}")
                    print(f"  Massa Salarial Habitual: R$ {prof.get('massa_rendimento_real_habitual_milhoes_reais'):,.2f} milhões")
                    print(f"  Taxa de Informalidade:   {prof.get('taxa_informalidade_pct')}%")
                    print(f"  Taxa de Subutilização:   {prof.get('taxa_composta_subutilizacao_pct')}%")
                    print(f"  Taxa de Desalento:       {prof.get('taxa_desalento_pct')}%")
                    print(f"  Razão Renda Mulher/Homem:{prof.get('razao_rendimento_mulher_homem')}")
                    print(f"  Score Resiliência:       {prof.get('score_resiliencia_mercado_trabalho')}/100 ({prof.get('classificacao_mercado_trabalho_uf')})")
                return 0

            if pnad_action == "earnings":
                earn = pnad_conn.get_earnings_summary(args.uf)
                if not earn:
                    print(f"No earnings data found for state '{args.uf}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(earn, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE PNAD Real Earnings: {earn.get('nome_uf')} ({earn.get('sigla_uf')} - {earn.get('ano_trimestre')}):")
                    print(f"  Rendimento Médio Habitual: R$ {earn.get('rendimento_medio_real_habitual_reais'):,.2f}")
                    print(f"  Rendimento Médio Efetivo:  R$ {earn.get('rendimento_medio_real_efetivo_reais'):,.2f}")
                    print(f"  Massa de Rendimento:       R$ {earn.get('massa_rendimento_real_habitual_milhoes_reais'):,.2f} milhões")
                    print(f"  Variação Ano Anterior:     {earn.get('variacao_rendimento_ano_anterior_pct')}%")
                return 0

            if pnad_action == "ranking":
                if args.criterio == "renda":
                    ranks = pnad_conn.list_highest_earnings_states(limit=args.limit)
                    if args.json:
                        print(json.dumps(ranks, indent=2, ensure_ascii=False))
                    else:
                        print(f"Top {len(ranks)} States by Real Monthly Earnings (PNAD Contínua):")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_uf')} ({r.get('sigla_uf')}): R$ {r.get('rendimento_medio_real_habitual_reais'):,.2f} | Massa: R$ {r.get('massa_rendimento_real_habitual_milhoes_reais'):,.2f}M")
                else:
                    ranks = pnad_conn.list_lowest_unemployment_states(limit=args.limit)
                    if args.json:
                        print(json.dumps(ranks, indent=2, ensure_ascii=False))
                    else:
                        print(f"Top {len(ranks)} States by Lowest Unemployment Rate (PNAD Contínua):")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_uf')} ({r.get('sigla_uf')}): {r.get('taxa_desocupacao_pct')}% desocupação | Nível Ocupação: {r.get('taxa_nivel_ocupacao_pct')}%")
                return 0

        elif args.command in ("cnefe",):
            from public_data_gateway.connectors.geografico.ibge_cnefe.connector import (
                IBGECnefeConnector,
            )

            cnefe_conn = IBGECnefeConnector(settings=settings)
            cnefe_action = getattr(args, "cnefe_action", None)

            if cnefe_action == "summary":
                summ = cnefe_conn.get_municipality_summary(args.municipio)
                if not summ:
                    print(f"No CNEFE data found for municipality '{args.municipio}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(summ, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE CNEFE Address Profile: {summ.get('nome_municipio')} ({summ.get('sigla_uf')} - {summ.get('id_municipio')}):")
                    print(f"  Total de Endereços:       {summ.get('total_enderecos'):,}")
                    print(f"  Domicílios Particulares:  {summ.get('total_domicilios_particulares'):,}")
                    print(f"  Comércio e Serviços:      {summ.get('total_estabelecimentos_comerciais_servicos'):,}")
                    print(f"  Estabelecimentos Saúde:   {summ.get('total_estabelecimentos_saude'):,}")
                    print(f"  Estabelecimentos Ensino:  {summ.get('total_estabelecimentos_ensino'):,}")
                    print(f"  Densidade de Endereços:   {summ.get('densidade_enderecos_km2'):,.2f} end/km²")
                    print(f"  Score Densidade Com.:     {summ.get('score_densidade_comercial')}/100.0")
                    print(f"  Score Infra Social:       {summ.get('score_infraestrutura_social')}/100.0")
                    print(f"  Taxa Georreferenciamento: {summ.get('taxa_cobertura_georreferenciada_pct')}%")
                return 0

            if cnefe_action == "cep":
                profile = cnefe_conn.get_cep_profile(args.cep)
                if not profile:
                    print(f"No CNEFE data found for CEP '{args.cep}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(profile, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE CNEFE CEP Profile: {profile.get('cep_8')} - {profile.get('nome_municipio')} ({profile.get('sigla_uf')}):")
                    print(f"  Total de Endereços:      {profile.get('total_enderecos'):,}")
                    print(f"  Total de Domicílios:     {profile.get('total_domicilios'):,}")
                    print(f"  Estabelecimentos:        {profile.get('total_estabelecimentos'):,}")
                    print(f"  Centroide (Lat, Lon):    {profile.get('latitude_centroide')}, {profile.get('longitude_centroide')}")
                    print(f"  Índice Uso Misto:        {profile.get('indice_misto_residencial_comercial')}")
                return 0

            if cnefe_action == "building-types":
                types_list = cnefe_conn.list_building_types(args.municipio)
                if not types_list:
                    print(f"No building species found for municipality '{args.municipio}'.", file=sys.stderr)
                    return 1
                if args.json:
                    print(json.dumps(types_list, indent=2, ensure_ascii=False))
                else:
                    print(f"IBGE CNEFE Building Species Breakdown: {types_list[0].get('nome_municipio')} ({types_list[0].get('sigla_uf')}):")
                    for b in types_list:
                        print(f"  - {b.get('tipo_edificacao')}: {b.get('total_edificacoes'):,} ({b.get('participacao_pct')}%) | {b.get('densidade_edificacoes_km2'):,.2f} /km²")
                return 0

            if cnefe_action == "ranking":
                ranks = cnefe_conn.list_top_address_density(limit=args.limit)
                if args.json:
                    print(json.dumps(ranks, indent=2, ensure_ascii=False))
                else:
                    print(f"Top {len(ranks)} Municipalities by Address Density (CNEFE Censo 2022):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): {r.get('densidade_enderecos_km2'):,.2f} end/km² | Total: {r.get('total_enderecos'):,}")
                return 0

        elif args.command == "malhas":
            from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
                IBGEMalhasConnector,
            )

            malhas_conn = IBGEMalhasConnector(settings=settings)
            malhas_action = getattr(args, "malhas_action", None)

            if malhas_action == "municipio":
                res = malhas_conn.get_municipio_boundary(
                    args.id_municipio, qualidade=getattr(args, "qualidade", "minima")
                )
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"IBGE Malha Territorial Municipal ({args.id_municipio}):")
                    print(f"  Tipo Geometria:      {res.get('tipo_geometria')}")
                    print(f"  Centroide (Lon, Lat): {res.get('centroide_lon')}, {res.get('centroide_lat')}")
                    print(f"  Bounding Box:        {res.get('bbox')}")
                    print(f"  Área Aprox. km²:     {res.get('area_aproximada_km2')}")
                    print(f"  Compacidade:         {res.get('score_compacidade')}/100.0")
                return 0

            if malhas_action == "uf":
                res = malhas_conn.get_uf_boundary(
                    args.sigla_uf, qualidade=getattr(args, "qualidade", "minima")
                )
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"IBGE Malha Territorial Estadual ({args.sigla_uf}):")
                    print(f"  Tipo Geometria:      {res.get('tipo_geometria')}")
                    print(f"  Centroide (Lon, Lat): {res.get('centroide_lon')}, {res.get('centroide_lat')}")
                    print(f"  Bounding Box:        {res.get('bbox')}")
                    print(f"  Área Aprox. km²:     {res.get('area_aproximada_km2')}")
                return 0

            if malhas_action == "features":
                feat = malhas_conn.get_territorial_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"IBGE Features Territoriais: {feat.get('nome_municipio', '')} ({feat.get('sigla_uf', '')} - {feat.get('id_municipio')}):")
                    print(f"  Centroide:           ({feat.get('centroide_longitude')}, {feat.get('centroide_latitude')})")
                    print(f"  Extensão Lon / Lat:  {feat.get('extensao_longitudinal_graus')}° / {feat.get('extensao_latitudinal_graus')}°")
                    print(f"  Área Estimada:       {feat.get('area_aproximada_km2'):,.2f} km²")
                    print(f"  Compacidade Polsby:  {feat.get('score_compacidade_territorial')}/100.0")
                    print(f"  Isolamento Capital:  {feat.get('score_isolamento_geografico')}/100.0")
                return 0

            if malhas_action == "ranking":
                ranks = malhas_conn.list_top_compact_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Municípios por Compacidade Territorial (IBGE Malhas):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Score {r.get('score_compacidade_territorial')} | {r.get('area_aproximada_km2'):,.2f} km²")
                return 0

        elif args.command == "anatel":
            from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
                AnatelMosaicoConnector,
            )

            anatel_conn = AnatelMosaicoConnector(settings=settings)
            anatel_action = getattr(args, "anatel_action", None)

            if anatel_action == "stations":
                res = anatel_conn.get_station_coverage_summary(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANATEL Mosaico Estações SMP ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Total ERBs:          {res.get('total_erbs_licenciadas')} (5G: {res.get('erbs_5g_nr')}, 4G: {res.get('erbs_4g_lte')})")
                    print(f"  Operadora Líder:     {res.get('operadora_lider_erbs')}")
                    print(f"  Densidade 10k hab:   {res.get('densidade_erbs_por_10k_hab')}")
                    print(f"  Cobertura Urbana 5G: {res.get('cobertura_urbana_5g_pct')}%")
                return 0

            if anatel_action == "broadband":
                res = anatel_conn.get_broadband_scm_profile(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANATEL Mosaico Banda Larga SCM ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Backhaul Fibra:      {res.get('possui_backhaul_fibra')}")
                    print(f"  Total Acessos SCM:   {res.get('total_acessos_scm')} ({res.get('participacao_fibra_pct')}% fibra FTTH)")
                    print(f"  Velocidade Mediana:  {res.get('velocidade_mediana_mbps')} Mbps")
                    print(f"  HHI Concentração:    {res.get('indice_hhi_concentracao_scm')}")
                return 0

            if anatel_action == "spectrum":
                rows = anatel_conn.list_spectrum_allocations(
                    args.id_municipio, faixa_frequencia_mhz=getattr(args, "faixa", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANATEL Alocações de Espectro ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('faixa_frequencia_mhz')} ({r.get('geracao_tecnologica')}): {r.get('estacoes_licenciadas_faixa')} estações | {r.get('largura_banda_agregada_mhz')} MHz")
                return 0

            if anatel_action == "features":
                feat = anatel_conn.get_telecom_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANATEL Features Conectividade: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Conectividade: {feat.get('score_conectividade_digital')}/100.0")
                    print(f"  Score Maturidade 5G: {feat.get('score_maturidade_5g')}/100.0")
                    print(f"  Classificação:       {feat.get('classificacao_infraestrutura_telecom')}")
                return 0

            if anatel_action == "ranking":
                ranks = anatel_conn.list_top_connected_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Municípios por Conectividade Digital (ANATEL Mosaico):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Score {r.get('score_conectividade_digital')} | 5G: {r.get('score_maturidade_5g')} | {r.get('classificacao_infraestrutura_telecom')}")
                return 0

        elif args.command == "antt":
            from public_data_gateway.connectors.geografico.antt_malha_transporte.connector import (
                AnttMalhaTransporteConnector,
            )

            antt_conn = AnttMalhaTransporteConnector(settings=settings)
            antt_action = getattr(args, "antt_action", None)

            if antt_action == "highways":
                res = antt_conn.get_highway_corridors(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANTT Corredores Rodoviários ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Rodovias BRs:        {res.get('rodovias_federais_principais')}")
                    print(f"  Malha / Concedida:   {res.get('extensao_malha_federal_km')} km ({res.get('proporcao_malha_concedida_pct')}% concedida)")
                    print(f"  Praças de Pedágio:   {res.get('pracas_pedagio_ativas')}")
                    print(f"  VMD Veículos Pesados:{res.get('volume_medio_diario_vmd_pesados')}/dia")
                return 0

            if antt_action == "rntrc":
                res = antt_conn.get_rntrc_fleet_profile(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANTT Cadastro RNTRC ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Transportadores:     {res.get('total_transportadores_rntrc')} (ETC: {res.get('transportadores_etc_empresas')}, TAC: {res.get('transportadores_tac_autonomos')}, CTC: {res.get('transportadores_ctc_cooperativas')})")
                    print(f"  Frota Habilitada:    {res.get('frota_veiculos_habilitados')} veículos")
                    print(f"  Capacidade Estática: {res.get('capacidade_estatica_total_ton')} ton (Idade média: {res.get('idade_media_frota_anos')} anos)")
                return 0

            if antt_action == "rail":
                res = antt_conn.get_rail_intermodal_terminals(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANTT Malha Ferroviária & Terminais ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Conexão Ferroviária: {res.get('possui_conexao_ferroviaria')} ({res.get('concessionarias_ferroviarias')})")
                    print(f"  Extensão / Terminais:{res.get('extensao_ferroviaria_km')} km | {res.get('terminais_carga_intermodal')} terminais intermodais")
                    print(f"  Movimentação Anual:  {res.get('movimentacao_ferroviaria_anual_tu')} mil TU")
                return 0

            if antt_action == "modals":
                rows = antt_conn.list_modal_flows(
                    args.id_municipio, modal_transporte=getattr(args, "modal", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANTT Matriz Modal de Cargas ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('modal_transporte')}: {r.get('participacao_modal_cargas_pct')}% | R$ {r.get('custo_tonelada_km_brl')}/TKU | {r.get('velocidade_media_comercial_kmh')} km/h | Confiabilidade {r.get('indice_confiabilidade_corredor')}")
                return 0

            if antt_action == "features":
                feat = antt_conn.get_logistics_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ANTT Features Logísticas: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Infra Logística:{feat.get('score_infraestrutura_logistica')}/100.0")
                    print(f"  Score Intermodalidade:{feat.get('score_intermodalidade_cargas')}/100.0")
                    print(f"  Classificação Nó:     {feat.get('classificacao_nodo_logistico')}")
                return 0

            if antt_action == "ranking":
                ranks = antt_conn.list_top_logistics_hubs(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Municípios por Infraestrutura Logística (ANTT):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Score {r.get('score_infraestrutura_logistica')} | Intermodal: {r.get('score_intermodalidade_cargas')} | {r.get('classificacao_nodo_logistico')}")
                return 0

        elif args.command == "cnes":
            from public_data_gateway.connectors.saude.datasus_cnes.connector import (
                DataSUSCnesConnector,
            )

            cnes_conn = DataSUSCnesConnector()
            cnes_action = getattr(args, "cnes_action", None)
            if cnes_action == "establishments":
                res = cnes_conn.get_establishments_summary(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS CNES Estabelecimentos ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Ativos / SUS:        {res.get('total_estabelecimentos_ativos')} ({res.get('proporcao_rede_sus_pct')}% SUS)")
                    print(f"  Hospitais / UBS / UPA: {res.get('hospitais_gerais_especializados')} / {res.get('unidades_basicas_saude_ubs')} / {res.get('unidades_pronto_atendimento_upa')}")
                return 0

            if cnes_action == "beds":
                res = cnes_conn.get_hospital_beds_icu(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS CNES Leitos & UTI ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Leitos Totais / SUS: {res.get('total_leitos_hospitalares')} / {res.get('leitos_sus')} ({res.get('leitos_por_1000_hab')} por 1.000 hab)")
                    print(f"  UTI Total (Ad/Ped):  {res.get('total_leitos_uti')} ({res.get('leitos_uti_adulto')}/{res.get('leitos_uti_pediatrico_neonatal')}) | {res.get('leitos_uti_por_10k_hab')} por 10k hab")
                return 0

            if cnes_action == "equipment":
                res = cnes_conn.get_diagnostic_equipment(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS CNES Equipamentos Diagnósticos ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  TC / RM / Mamógrafos:{res.get('tomografos_computadorizados')} / {res.get('ressonancia_magnetica')} / {res.get('mamografos_em_uso')}")
                    print(f"  Hemodiálise / Índice:{res.get('aparelhos_hemodialise')} | Score Diag {res.get('indice_disponibilidade_diagnostica')}/100.0 ({res.get('proporcao_equipamentos_sus_pct')}% SUS)")
                return 0

            if cnes_action == "workforce":
                rows = cnes_conn.list_health_workforce(
                    args.id_municipio, categoria_profissional=getattr(args, "categoria", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS CNES Força de Trabalho ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('categoria_profissional')}: {r.get('total_vinculos_ativos')} vínculos ({r.get('vinculos_atendimento_sus')} SUS) | {r.get('densidade_por_10k_hab')}/10k hab | {r.get('carga_horaria_semanal_media')}h/sem")
                return 0

            if cnes_action == "features":
                feat = cnes_conn.get_health_capacity_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS CNES Features Assistenciais: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Capacidade Hosp: {feat.get('score_capacidade_hospitalar')}/100.0")
                    print(f"  Score Complexidade UTI:{feat.get('score_alta_complexidade_uti')}/100.0")
                    print(f"  Classificação Rede:    {feat.get('classificacao_rede_assistencial')}")
                return 0

            if cnes_action == "ranking":
                ranks = cnes_conn.list_top_health_hubs(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Polos de Capacidade Assistencial (DataSUS CNES):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Hosp {r.get('score_capacidade_hospitalar')} | UTI: {r.get('score_alta_complexidade_uti')} | {r.get('classificacao_rede_assistencial')}")
                return 0

        elif args.command == "sih":
            from public_data_gateway.connectors.saude.datasus_sih.connector import (
                DataSUSSihConnector,
            )

            sih_conn = DataSUSSihConnector()
            sih_action = getattr(args, "sih_action", None)
            if sih_action == "admissions":
                res = sih_conn.get_admissions_summary(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIH Internações AIH ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Total AIH / Urgência: {res.get('total_internacoes_aih')} ({res.get('proporcao_urgencia_pct')}% urgência)")
                    print(f"  Diárias / Permanência:{res.get('diarias_hospitalares_totais')} diárias | {res.get('tempo_medio_permanencia_dias')} dias ({res.get('internacoes_por_1000_hab_ano')}/1k hab ano)")
                return 0

            if sih_action == "costs":
                res = sih_conn.get_hospital_costs(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIH Custos Hospitalares SUS ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Valor Total AIH:      R$ {res.get('valor_total_aih_brl'):,.2f} (Ticket médio R$ {res.get('ticket_medio_aih_brl'):,.2f})")
                    print(f"  UTI Alta Complexidade:R$ {res.get('valor_uti_alta_complexidade_brl'):,.2f} ({res.get('proporcao_custo_uti_pct')}% do total | Diária R$ {res.get('custo_medio_diaria_brl'):,.2f})")
                return 0

            if sih_action == "cid10":
                rows = sih_conn.list_cid10_morbidity(
                    args.id_municipio, capitulo_cid10=getattr(args, "capitulo", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIH Morbidade CID-10 ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('capitulo_cid10')}: {r.get('internacoes_capitulo_aih')} AIHs ({r.get('participacao_capitulo_pct')}%) | R$ {r.get('valor_total_capitulo_brl'):,.2f} | Letalidade {r.get('taxa_letalidade_capitulo_pct')}%")
                return 0

            if sih_action == "outcomes":
                res = sih_conn.get_mortality_outcomes(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIH Desfechos Hospitalares ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Altas / Óbitos:       {res.get('altas_hospitalares')} / {res.get('obitos_hospitalares')} (Mortalidade {res.get('taxa_mortalidade_hospitalar_pct')}%)")
                    print(f"  Acionamento UTI/ICSAP:{res.get('internacoes_com_uti')} ({res.get('taxa_utilizacao_uti_pct')}% UTI) | {res.get('internacoes_icsap_evitaveis')} ICSAP ({res.get('proporcao_icsap_pct')}%)")
                return 0

            if sih_action == "features":
                feat = sih_conn.get_sih_morbidity_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIH Features Assistenciais: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Complexidade SIH: {feat.get('score_complexidade_assistencial_sih')}/100.0")
                    print(f"  Score Pressão Morbidade:{feat.get('score_pressao_morbidade_hospitalar')}/100.0")
                    print(f"  Perfil Epidemiológico:  {feat.get('perfil_epidemiologico_hospitalar')}")
                return 0

            if sih_action == "ranking":
                ranks = sih_conn.list_top_sih_complexity_hubs(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Polos de Complexidade Hospitalar (DataSUS SIH/SUS):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Complexidade {r.get('score_complexidade_assistencial_sih')} | Pressão: {r.get('score_pressao_morbidade_hospitalar')} | {r.get('perfil_epidemiologico_hospitalar')}")
                return 0

        elif args.command == "sim":
            from public_data_gateway.connectors.saude.datasus_sim.connector import (
                DataSUSSimConnector,
            )

            sim_conn = DataSUSSimConnector()
            sim_action = getattr(args, "sim_action", None)
            if sim_action == "general":
                res = sim_conn.get_general_mortality(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIM Mortalidade Geral ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Total Óbitos / TBM:   {res.get('total_obitos_registrados')} ({res.get('taxa_bruta_mortalidade_1000_hab')}/1k hab)")
                    print(f"  Hosp / Mal Definidas: {res.get('proporcao_obitos_hospitalares_pct')}% hosp | {res.get('proporcao_causas_mal_definidas_pct')}% mal def.")
                return 0

            if sim_action == "cid10":
                rows = sim_conn.list_cid10_mortality_causes(
                    args.id_municipio, capitulo_cid10=getattr(args, "capitulo", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIM Causas de Óbito CID-10 ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('capitulo_cid10')}: {r.get('obitos_capitulo')} óbitos ({r.get('participacao_capitulo_pct')}%) | Taxa {r.get('taxa_mortalidade_especifica_100k_hab')}/100k | Idade média {r.get('idade_media_obito_capitulo_anos')} anos")
                return 0

            if sim_action == "maternal-infant":
                res = sim_conn.get_maternal_infant_mortality(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIM Mortalidade Materno-Infantil ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Óbitos <1a / TMI:     {res.get('obitos_infantis_menores_1_ano')} (TMI {res.get('taxa_mortalidade_infantil_1000_nv')}/1k NV)")
                    print(f"  RMM / Investigação:   {res.get('razao_mortalidade_materna_100k_nv')}/100k NV | {res.get('proporcao_obitos_infantis_investigados_pct')}% investigados")
                return 0

            if sim_action == "premature":
                res = sim_conn.get_premature_dcnt_external_mortality(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIM Carga Prematura & Causas Externas ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  DCNT 30-69 anos:      {res.get('obitos_prematuros_dcnt_30_69_anos')} ({res.get('proporcao_mortalidade_prematura_dcnt_pct')}%)")
                    print(f"  Externas / APVP:      {res.get('taxa_mortalidade_causas_externas_100k_hab')}/100k hab | APVP {res.get('anos_potenciais_vida_perdidos_1000_hab')}/1k hab")
                return 0

            if sim_action == "features":
                feat = sim_conn.get_sim_mortality_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SIM Features Vigilância Epidemiológica: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Vigilância SIM: {feat.get('score_qualidade_vigilancia_obitos_sim')}/100.0")
                    print(f"  Score Carga Evitável: {feat.get('score_carga_mortalidade_evitavel')}/100.0")
                    print(f"  Perfil Transição:     {feat.get('perfil_transicao_epidemiologica_mortalidade')}")
                return 0

            if sim_action == "ranking":
                ranks = sim_conn.list_top_sim_surveillance_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Redes Municipais em Vigilância SIM (DataSUS):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Vigilância {r.get('score_qualidade_vigilancia_obitos_sim')} | Carga: {r.get('score_carga_mortalidade_evitavel')} | {r.get('perfil_transicao_epidemiologica_mortalidade')}")
                return 0

        elif args.command == "sinasc":
            from public_data_gateway.connectors.saude.datasus_sinasc.connector import (
                DataSUSSinascConnector,
            )

            sn_conn = DataSUSSinascConnector()
            sn_action = getattr(args, "sinasc_action", None)
            if sn_action == "births":
                res = sn_conn.get_live_births_summary(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SINASC Nascidos Vivos ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Nascidos Vivos / TBN: {res.get('total_nascidos_vivos')} (TBN {res.get('taxa_bruta_natalidade_1000_hab')}/1k hab)")
                    print(f"  Hospitalar / Cesáreo: {res.get('proporcao_partos_hospitalares_pct')}% hosp | {res.get('proporcao_partos_cesareos_pct')}% cesáreo")
                return 0

            if sn_action == "prenatal":
                rows = sn_conn.list_prenatal_care_breakdown(
                    args.id_municipio, faixa_assistencia_prenatal=getattr(args, "faixa", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SINASC Assistência Pré-Natal ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('faixa_assistencia_prenatal')}: {r.get('nascidos_vivos_faixa')} NV ({r.get('participacao_faixa_prenatal_pct')}%) | 1º Trim {r.get('inicio_prenatal_primeiro_trimestre_pct')}% | Cesárea {r.get('proporcao_cesarea_faixa_pct')}%")
                return 0

            if sn_action == "maternal":
                res = sn_conn.get_maternal_demographic_profile(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SINASC Perfil Materno ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Idade Média / <20a:   {res.get('idade_media_materna_anos')} anos | {res.get('proporcao_gravidez_adolescencia_pct')}% <20 anos")
                    print(f"  >=35a / >=12a Estudo: {res.get('proporcao_gestacao_tardia_35_mais_pct')}% >=35a | {res.get('proporcao_escolaridade_materna_12_anos_mais_pct')}% >=12 anos estudo")
                return 0

            if sn_action == "vitality":
                res = sn_conn.get_newborn_vitality_conditions(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SINASC Vitalidade Neonatal ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Baixo Peso / Premat.: {res.get('proporcao_baixo_peso_nascer_pct')}% <2500g | {res.get('proporcao_prematuridade_pct')}% <37 sem")
                    print(f"  Apgar 7-10 / Anomal.: {res.get('proporcao_apgar_5min_adequado_7_10_pct')}% Apgar | {res.get('taxa_anomalias_congenitas_10k_nv')}/10k NV")
                return 0

            if sn_action == "features":
                feat = sn_conn.get_sinasc_perinatal_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"DataSUS SINASC Features Perinatais: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Pré-Natal:      {feat.get('score_qualidade_assistencia_prenatal_sinasc')}/100.0")
                    print(f"  Score Vitalidade:     {feat.get('score_vitalidade_protecao_perinatal')}/100.0")
                    print(f"  Classificação Rede:   {feat.get('classificacao_rede_materno_perinatal')}")
                return 0

            if sn_action == "ranking":
                ranks = sn_conn.list_top_sinasc_prenatal_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Redes Municipais Materno-Perinatais (DataSUS SINASC):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Pré-Natal {r.get('score_qualidade_assistencia_prenatal_sinasc')} | Vitalidade: {r.get('score_vitalidade_protecao_perinatal')} | {r.get('classificacao_rede_materno_perinatal')}")
                return 0

        elif args.command == "cmed":
            from public_data_gateway.connectors.saude.anvisa_cmed.connector import (
                ANVISACmedConnector,
            )

            cm_conn = ANVISACmedConnector()
            cm_action = getattr(args, "cmed_action", None)
            try:
                if cm_action == "presentations":
                    res = cm_conn.get_presentations_summary(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"ANVISA CMED Apresentações ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Laboratório Âncora: {res.get('laboratorio_detentor_referencia')} ({res.get('cnpj_14')})")
                        print(f"  Ativas / Gen+Sim:   {res.get('total_apresentacoes_ativas_cmed')} ({res.get('participacao_genericos_similares_pct')}% genéricos+similares)")
                    return 0

                if cm_action == "classes":
                    rows = cm_conn.list_therapeutic_class_prices(
                        args.id_municipio, classe_terapeutica_atc=getattr(args, "classe", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"ANVISA CMED Preços por Classe Terapêutica ({args.id_municipio}):")
                        for r in rows:
                            print(f"  - {r.get('classe_terapeutica_atc')}: PF R$ {r.get('preco_fabrica_pf_medio_brl')} | PMC R$ {r.get('preco_maximo_consumidor_pmc_medio_brl')} | PMVG R$ {r.get('preco_maximo_venda_governo_pmvg_medio_brl')}")
                    return 0

                if cm_action == "pmvg":
                    res = cm_conn.get_public_procurement_pmvg(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"ANVISA CMED Compras Públicas CAP & PMVG ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  PF / PMC / PMVG:    R$ {res.get('preco_fabrica_medio_ponderado_brl')} / R$ {res.get('pmc_medio_ponderado_aliquota_uf_brl')} / R$ {res.get('pmvg_medio_ponderado_brl')}")
                        print(f"  CAP / Conform. SUS: {res.get('proporcao_apresentacoes_cap_pct')}% CAP | {res.get('conformidade_teto_pmvg_compras_sus_pct')}% conformidade PMVG")
                    return 0

                if cm_action == "taxes":
                    res = cm_conn.get_tax_regime_piscofins(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"ANVISA CMED Regime Tributário ICMS & PIS/COFINS ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  ICMS Modal / Lista+: {res.get('aliquota_icms_modal_uf_pct')}% ICMS | {res.get('participacao_lista_positiva_piscofins_pct')}% Lista Positiva")
                        print(f"  Comerc. 2024 / Spr:  {res.get('apresentacoes_comercializacao_2024_ativa_pct')}% ativa | {res.get('spread_medio_pmc_sobre_pf_pct')}% spread PMC/PF")
                    return 0

                if cm_action == "features":
                    feat = cm_conn.get_cmed_drug_pricing_features(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"ANVISA CMED Features Regulatórias: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                        print(f"  Score Competitividade: {feat.get('score_competitividade_genericos_cmed')}/100.0")
                        print(f"  Score Eficiência PMVG: {feat.get('score_eficiencia_regulatoria_pmvg_cap')}/100.0")
                        print(f"  Classificação Polo:    {feat.get('classificacao_polo_farmaceutico_cmed')}")
                    return 0

                if cm_action == "ranking":
                    ranks = cm_conn.list_top_cmed_regulatory_efficiency_hubs(
                        limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Top {len(ranks)} Polos Farmacêuticos CMED (ANVISA):")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Eficiência {r.get('score_eficiencia_regulatoria_pmvg_cap')} | Competitividade: {r.get('score_competitividade_genericos_cmed')} | {r.get('classificacao_polo_farmaceutico_cmed')}")
                    return 0
            except ValueError as exc:
                print(f"Erro ANVISA CMED: {exc}", file=sys.stderr)
                return 1

        elif args.command == "vigilancia":
            from public_data_gateway.connectors.saude.opendatasus_vigilancia.connector import (
                OpenDataSUSVigilanciaConnector,
            )

            vig_conn = OpenDataSUSVigilanciaConnector(settings=settings)
            vig_action = getattr(args, "vigilancia_action", None)
            try:
                if vig_action == "srag":
                    res = vig_conn.get_vigilancia_srag_respiratory_surveillance(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"OpenDataSUS Vigilância SG & SRAG ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  SG / SRAG ({res.get('ano_epidemiologico')}): {res.get('notificacoes_sindrome_gripal_esus_ano'):,} SG e-SUS | {res.get('internacoes_srag_sivep_ano'):,} internações SRAG ({res.get('taxa_incidencia_srag_100k_hab')}/100k)")
                        print(f"  UTI / Letalidade:  {res.get('proporcao_srag_uti_pct')}% UTI | {res.get('letalidade_hospitalar_srag_pct')}% letalidade | {res.get('confirmacao_laboratorial_rt_pcr_srag_pct')}% RT-PCR")
                    return 0

                if vig_action == "sinan":
                    rows = vig_conn.list_vigilancia_sinan_notifiable_diseases(
                        args.id_municipio, grupo_agravo=getattr(args, "grupo", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"OpenDataSUS SINAN Agravos Compulsórios ({args.id_municipio}) - {len(rows)} grupo(s):")
                        for r in rows:
                            print(f"  - {r.get('grupo_agravo_sinan')} ({r.get('categoria_transmissao')}): {r.get('casos_confirmados_ano'):,}/{r.get('casos_notificados_ano'):,} conf. | Inc.: {r.get('taxa_incidencia_confirmada_100k_hab')}/100k | Enc.: {r.get('proporcao_encerramento_oportuno_sinan_pct')}%")
                    return 0

                if vig_action == "vacinacao":
                    res = vig_conn.get_vigilancia_sipni_vaccination_coverage(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"OpenDataSUS SI-PNI Cobertura Vacinal ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Doses / Salas:    {res.get('doses_aplicadas_calendario_pni_ano'):,} doses | {res.get('salas_vacina_ativas_cnes')} salas ativas")
                        print(f"  Penta / VIP / TV: {res.get('cobertura_pentavalente_d3_pct')}% Penta | {res.get('cobertura_poliomielite_vip_pct')}% VIP | {res.get('cobertura_triplice_viral_d1_pct')}% Tríplice Viral")
                        print(f"  Homog. / Aband.:  {res.get('homogeneidade_cobertura_vacinal_pni_pct')}% homogeneidade | {res.get('taxa_abandono_esquema_multidose_pct')}% abandono")
                    return 0

                if vig_action == "cievs":
                    res = vig_conn.get_vigilancia_cievs_response_capacity(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"OpenDataSUS CIEVS/GAL-LACEN Prontidão ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  24h / 48h / GAL:  {res.get('notificacoes_imediatas_24h_cievs_pct')}% 24h | {res.get('investigacoes_epidemiologicas_48h_pct')}% 48h | {res.get('tempo_medio_liberacao_gal_lacen_horas')}h LACEN")
                        print(f"  Bloq. / RNDS:     {res.get('indice_bloqueio_vetorial_arboviroses_pct')}% bloqueio | {res.get('integracao_rnds_esus_sinan_pct')}% integração RNDS")
                    return 0

                if vig_action == "features":
                    feat = vig_conn.get_vigilancia_epidemiological_features(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"OpenDataSUS Vigilância Features: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                        print(f"  Score Prontidão:   {feat.get('score_prontidao_vigilancia_epidemiologica')}/100.0")
                        print(f"  Score Imunização:  {feat.get('score_protecao_imunizacao_coletiva_pni')}/100.0")
                        print(f"  Classificação:     {feat.get('classificacao_vigilancia_epidemiologica_municipal')}")
                    return 0

                if vig_action == "ranking":
                    ranks = vig_conn.list_top_vigilancia_surveillance_municipalities(
                        limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Top {len(ranks)} Municípios em Prontidão Epidemiológica & Imunização (OpenDataSUS):")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Prontidão {r.get('score_prontidao_vigilancia_epidemiologica')} | Imunização: {r.get('score_protecao_imunizacao_coletiva_pni')} | {r.get('classificacao_vigilancia_epidemiologica_municipal')}")
                    return 0
            except ValueError as exc:
                print(f"Erro OpenDataSUS Vigilância: {exc}", file=sys.stderr)
                return 1

        elif args.command == "censo-escolar":
            from public_data_gateway.connectors.educacao.inep_censo_escolar.connector import (
                INEPCensoEscolarConnector,
            )

            ce_conn = INEPCensoEscolarConnector()
            ce_action = getattr(args, "censo_escolar_action", None)
            if ce_action == "schools":
                res = ce_conn.get_school_infrastructure(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Escolar Escolas & Infraestrutura ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Escolas Ativas:     {res.get('total_escolas_ativas')} ({res.get('proporcao_rede_publica_pct')}% pública)")
                    print(f"  Banda Larga / Lab:  {res.get('escolas_com_internet_banda_larga_pct')}% internet | {res.get('escolas_com_laboratorio_ciencias_pct')}% lab ciências")
                return 0

            if ce_action == "enrollments":
                rows = ce_conn.list_enrollments_by_stage(
                    args.id_municipio, etapa_ensino=getattr(args, "etapa", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Escolar Matrículas por Etapa ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('etapa_ensino')}: {r.get('matriculas_totais_etapa')} ({r.get('proporcao_tempo_integral_pct')}% integral | {r.get('participacao_etapa_total_pct')}%)")
                return 0

            if ce_action == "teachers":
                res = ce_conn.get_teacher_qualifications(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Escolar Docentes & AFD ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Docentes Exercício: {res.get('total_docentes_exercicio')} ({res.get('proporcao_docentes_superior_pct')}% superior)")
                    print(f"  Indicador AFD:      {res.get('indicador_adequacao_formacao_docente_afd_pct')}% | Alunos/Docente: {res.get('razao_alunos_por_docente')}")
                return 0

            if ce_action == "flow":
                res = ce_conn.get_yield_flow_indicators(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Escolar Rendimento & Fluxo ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Matrículas / Integ: {res.get('total_matriculas_educacao_basica')} ({res.get('proporcao_tempo_integral_total_pct')}% integral)")
                    print(f"  Aprovação / Aband:  {res.get('taxa_aprovacao_media_pct')}% aprovação | {res.get('taxa_abandono_escolar_pct')}% abandono")
                return 0

            if ce_action == "features":
                feat = ce_conn.get_censo_escolar_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Escolar Features Educação Básica: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Infraestrutura: {feat.get('score_infraestrutura_escolar_inep')}/100.0")
                    print(f"  Score Qualidade Fluxo:{feat.get('score_qualidade_fluxo_educacional')}/100.0")
                    print(f"  Classificação Rede:   {feat.get('classificacao_rede_educacional_basica')}")
                return 0

            if ce_action == "ranking":
                ranks = ce_conn.list_top_censo_escolar_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Redes Municipais de Educação Básica (INEP Censo Escolar):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Infra {r.get('score_infraestrutura_escolar_inep')} | Fluxo: {r.get('score_qualidade_fluxo_educacional')} | {r.get('classificacao_rede_educacional_basica')}")
                return 0

        elif args.command == "censo-superior":
            from public_data_gateway.connectors.educacao.inep_censo_superior.connector import (
                INEPCensoSuperiorConnector,
            )

            cs_conn = INEPCensoSuperiorConnector()
            cs_action = getattr(args, "censo_superior_action", None)
            if cs_action == "institutions":
                res = cs_conn.get_institutions_summary(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Superior Instituições IES ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  IES Ativas / Univ:  {res.get('total_ies_ativas')} IES ({res.get('universidades_e_centros_universitarios')} Univ/Centros | {res.get('proporcao_ies_publicas_pct')}% públicas)")
                    print(f"  IGC Contínuo Médio: {res.get('igc_medio_continuo_municipio')}")
                return 0

            if cs_action == "courses":
                rows = cs_conn.list_cine_area_courses(
                    args.id_municipio, area_cine_rotulo=getattr(args, "area", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Superior Cursos por Área CINE ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('area_cine_rotulo')}: {r.get('cursos_graduacao_ativos')} cursos | {r.get('matriculas_ativas_area')} matrículas ({r.get('participacao_matriculas_area_pct')}%) | ENADE: {r.get('conceito_enade_medio_area')}")
                return 0

            if cs_action == "enrollments":
                res = cs_conn.get_enrollments_and_graduates(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Superior Matrículas & Fluxo ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Matrículas / EaD:   {res.get('total_matriculas_graduacao')} ({res.get('proporcao_matriculas_ead_pct')}% EaD)")
                    print(f"  Conclusão / Desist: {res.get('taxa_conclusao_acumulada_tcg_pct')}% TCG | {res.get('taxa_desistencia_anual_tda_pct')}% TDA")
                return 0

            if cs_action == "faculty":
                res = cs_conn.get_faculty_profile(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Superior Corpo Docente ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Docentes / Doutores:{res.get('total_docentes_exercicio_ies')} ({res.get('proporcao_doutores_pct')}% doutores)")
                    print(f"  Regime DE / Razão:  {res.get('proporcao_regime_tempo_integral_de_pct')}% DE | {res.get('razao_alunos_por_docente_equivalente')} alunos/docente")
                return 0

            if cs_action == "features":
                feat = cs_conn.get_censo_superior_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP Censo Superior Features Polos Universitários: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Excelência IES:{feat.get('score_excelencia_academica_pesquisa_ies')}/100.0")
                    print(f"  Score Cap. Humano:   {feat.get('score_formacao_capital_humano_superior')}/100.0")
                    print(f"  Classificação Polo:  {feat.get('classificacao_polo_educacao_superior')}")
                return 0

            if cs_action == "ranking":
                ranks = cs_conn.list_top_censo_superior_hubs(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Polos Universitários & Educação Superior (INEP Censo Superior):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Excelência {r.get('score_excelencia_academica_pesquisa_ies')} | Cap. Humano: {r.get('score_formacao_capital_humano_superior')} | {r.get('classificacao_polo_educacao_superior')}")
                return 0

        elif args.command == "ideb":
            from public_data_gateway.connectors.educacao.inep_ideb.connector import (
                INEPIdebConnector,
            )

            ideb_conn = INEPIdebConnector()
            ideb_action = getattr(args, "ideb_action", None)
            if ideb_action == "results":
                res = ideb_conn.get_ideb_municipal_results(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP IDEB Resultados ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Anos Iniciais / Finais: {res.get('ideb_anos_iniciais_rede_publica')} / {res.get('ideb_anos_finais_rede_publica')}")
                    print(f"  Ensino Médio Púb / Priv:{res.get('ideb_ensino_medio_rede_publica')} / {res.get('ideb_rede_privada_ensino_medio')}")
                    print(f"  Metas Cumpridas:        {res.get('proporcao_escolas_cumpriram_meta_pct')}% ({res.get('escolas_avaliadas_ideb_municipio')} escolas)")
                return 0

            if ideb_action == "stages":
                rows = ideb_conn.list_ideb_stage_network_scores(
                    args.id_municipio, etapa_ciclo=getattr(args, "etapa", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP IDEB Etapas e Redes ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('etapa_ciclo_ideb')}: observado {r.get('ideb_observado')} | meta {r.get('meta_projetada_inep')} | saldo {r.get('saldo_sobre_meta_ideb')}")
                return 0

            if ideb_action == "saeb":
                res = ideb_conn.get_ideb_saeb_proficiency(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP IDEB/SAEB Proficiências ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  LP / MAT 5º ano:     {res.get('proficiencia_saeb_lp_5ano')} / {res.get('proficiencia_saeb_mat_5ano')}")
                    print(f"  LP / MAT 9º ano:     {res.get('proficiencia_saeb_lp_9ano')} / {res.get('proficiencia_saeb_mat_9ano')}")
                    print(f"  Aprendizado Adequado:{res.get('aprendizado_adequado_anos_iniciais_pct')}% AI | {res.get('aprendizado_adequado_anos_finais_pct')}% AF")
                return 0

            if ideb_action == "evolution":
                res = ideb_conn.get_ideb_biennial_goals_evolution(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP IDEB Evolução & Metas ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  2021 -> 2023 Var:   {res.get('ideb_edicao_anterior_2021')} ({res.get('variacao_bienal_ideb_pontos'):+.2f} pts)")
                    print(f"  Equidade INSE Ratio:{res.get('equidade_aprendizagem_inse_ratio')}")
                return 0

            if ideb_action == "features":
                feat = ideb_conn.get_ideb_education_quality_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"INEP IDEB Features: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Desempenho IDEB: {feat.get('score_desempenho_ideb_municipal')}/100.0")
                    print(f"  Score SAEB/Equidade:   {feat.get('score_proficiencia_equidade_saeb')}/100.0")
                    print(f"  Classificação IDEB:    {feat.get('classificacao_qualidade_ideb_municipio')}")
                return 0

            if ideb_action == "ranking":
                ranks = ideb_conn.list_top_ideb_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Municípios no IDEB & Equidade SAEB (INEP IDEB):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): IDEB Score {r.get('score_desempenho_ideb_municipal')} | SAEB/INSE: {r.get('score_proficiencia_equidade_saeb')} | {r.get('classificacao_qualidade_ideb_municipio')}")
                return 0

        elif args.command == "enem":
            from public_data_gateway.connectors.educacao.inep_enem.connector import (
                INEPEnemConnector,
            )

            enem_conn = INEPEnemConnector(settings=settings)
            enem_action = getattr(args, "enem_action", None)
            try:
                if enem_action == "scores":
                    res = enem_conn.get_enem_municipal_area_scores(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"INEP ENEM Notas TRI ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  LC / CH:           {res.get('nota_media_linguagens_codigos_lc')} / {res.get('nota_media_ciencias_humanas_ch')}")
                        print(f"  CN / MT / Geral:   {res.get('nota_media_ciencias_natureza_cn')} / {res.get('nota_media_matematica_mt')} / {res.get('nota_media_objetivas_tri_geral')}")
                        print(f"  Presentes 2 dias:  {res.get('participantes_presentes_2dias_ano'):,}/{res.get('inscritos_confirmados_enem_ano'):,} ({res.get('taxa_presenca_efetiva_2dias_pct')}%)")
                    return 0

                if enem_action == "networks":
                    rows = enem_conn.list_enem_scores_by_school_network(
                        args.id_municipio, dependencia_rede=getattr(args, "rede", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"INEP ENEM Redes de Ensino ({args.id_municipio}) - {len(rows)} rede(s):")
                        for r in rows:
                            print(f"  - {r.get('dependencia_administrativa_escola')}: Objetivas {r.get('nota_media_objetivas_rede')} | MT {r.get('nota_media_matematica_rede')} | Redação {r.get('nota_media_redacao_rede')}")
                    return 0

                if enem_action == "essay":
                    res = enem_conn.get_enem_essay_competencies(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"INEP ENEM Redação & Competências ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Redação Média:     {res.get('nota_media_redacao_enem')} pts (Comp1 {res.get('nota_media_comp1_norma_padrao')} | Comp3 {res.get('nota_media_comp3_argumentacao')} | Comp5 {res.get('nota_media_comp5_proposta_intervencao')})")
                        print(f"  Notas 900+ / Zero: {res.get('redacoes_nota_900_mais_ano'):,} ({res.get('proporcao_redacao_900_mais_pct')}%) | Zero: {res.get('taxa_redacao_zero_ou_anulada_pct')}%")
                    return 0

                if enem_action == "equity":
                    res = enem_conn.get_enem_socioeconomic_equity(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"INEP ENEM Equidade QSE & Acesso ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Isenção / Pública: {res.get('proporcao_isencao_taxa_inscricao_pct')}% isenção | {res.get('proporcao_concluintes_escola_publica_pct')}% pública | {res.get('acesso_internet_domiciliar_q025_pct')}% internet")
                        print(f"  Equidade / SiSU:   {res.get('razao_equidade_publica_privada_enem')} razão púb/priv | {res.get('aptos_sisu_prouni_fies_nota_450_pct')}% aptos SiSU/ProUni")
                    return 0

                if enem_action == "features":
                    feat = enem_conn.get_enem_performance_equity_features(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"INEP ENEM Features: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                        print(f"  Score Cognitivo:   {feat.get('score_desempenho_cognitivo_enem')}/100.0")
                        print(f"  Score Equidade:    {feat.get('score_equidade_acesso_ensino_superior_enem')}/100.0")
                        print(f"  Classificação:     {feat.get('classificacao_desempenho_enem_municipal')}")
                    return 0

                if enem_action == "ranking":
                    ranks = enem_conn.list_top_enem_municipalities(
                        limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Top {len(ranks)} Municípios no Desempenho & Equidade ENEM (INEP):")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Cognitivo {r.get('score_desempenho_cognitivo_enem')} | Equidade: {r.get('score_equidade_acesso_ensino_superior_enem')} | {r.get('classificacao_desempenho_enem_municipal')}")
                    return 0
            except ValueError as exc:
                print(f"Erro INEP ENEM: {exc}", file=sys.stderr)
                return 1

        elif args.command == "sucupira":
            from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
                CapesSucupiraConnector,
            )

            suc_conn = CapesSucupiraConnector(settings=settings)
            suc_action = getattr(args, "sucupira_action", None)
            try:
                if suc_action == "programs":
                    res = suc_conn.get_sucupira_program_evaluation(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"CAPES Sucupira Programas ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Programas / Nota 6-7: {res.get('total_programas_stricto_sensu_ano')} ({res.get('programas_nota_6_7_excelencia_ano')} excelência)")
                        print(f"  Nota Média CAPES:     {res.get('nota_media_capes_programas')}")
                    return 0

                if suc_action == "students":
                    rows = suc_conn.list_sucupira_students_by_area(
                        args.id_municipio, grande_area_conhecimento=getattr(args, "area", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"CAPES Sucupira Discentes ({args.id_municipio}) - {len(rows)} área(s):")
                        for r in rows:
                            print(f"  - {r.get('grande_area_conhecimento')}: Mestrado {r.get('matriculados_mestrado_area')} | Doutorado {r.get('matriculados_doutorado_area')} | Bolsas {r.get('bolsistas_capes_area')}")
                    return 0

                if suc_action == "faculty":
                    res = suc_conn.get_sucupira_faculty_research(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"CAPES Sucupira Docentes ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Docentes Permanentes: {res.get('total_docentes_permanentes_ano')} | PQ CNPq: {res.get('docentes_bolsistas_pq_cnpq_ano')}")
                    return 0

                if suc_action == "production":
                    res = suc_conn.get_sucupira_scientific_production(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"CAPES Sucupira Produção Científica ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Qualis A1/A2: {res.get('artigos_qualis_a1_a2_ano')} | Teses/Diss.: {res.get('teses_dissertacoes_defendidas_ano')} | Fomento: R$ {res.get('fomento_bolsas_estimado_milhoes_brl')}M")
                    return 0

                if suc_action == "features":
                    res = suc_conn.get_sucupira_postgrad_features(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"CAPES Sucupira Gold ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Score Composto: {res.get('score_composto_ecossistema_pos_graduacao')} | Perfil: {res.get('perfil_pos_graduacao_sucupira')}")
                    return 0

                if suc_action == "ranking":
                    ranks = suc_conn.list_top_sucupira_municipalities(
                        limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Top {len(ranks)} Municípios CAPES Sucupira:")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): {r.get('score_composto_ecossistema_pos_graduacao')} | {r.get('perfil_pos_graduacao_sucupira')}")
                    return 0
            except ValueError as exc:
                print(f"Erro CAPES Sucupira: {exc}", file=sys.stderr)
                return 1

        elif args.command == "onr":
            from public_data_gateway.connectors.imobiliario.onr_registro_imoveis.connector import (
                ONRRegistroImoveisConnector,
            )

            onr_conn = ONRRegistroImoveisConnector()
            onr_action = getattr(args, "onr_action", None)
            if onr_action == "registry":
                res = onr_conn.get_onr_property_registry(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ONR SREI Acervo de Matrículas ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Serventias / CNM:     {res.get('serventias_ri_integradas_srei')} RI | {res.get('total_matriculas_ativas_cnm'):,} matrículas")
                    print(f"  Conversão CNM Digital:{res.get('taxa_conversao_cnm_digital_pct')}% | Novas/ano: {res.get('novas_matriculas_abertas_ano'):,}")
                    print(f"  Certidões Digitais:   {res.get('certidoes_digitais_srei_ano'):,}")
                return 0

            if onr_action == "transactions":
                rows = onr_conn.list_onr_transactions_by_nature(
                    args.id_municipio, natureza_ato=getattr(args, "natureza", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ONR SREI Transações e Atos Registrais ({args.id_municipio}):")
                    for r in rows:
                        print(f"  - {r.get('natureza_ato_registral')}: {r.get('quantidade_atos_registrados_ano'):,} atos | R$ {r.get('volume_financeiro_declarado_milhoes_brl')}M | e-Prot: {r.get('proporcao_protocolo_eletronico_srei_pct')}%")
                return 0

            if onr_action == "credit":
                res = onr_conn.get_onr_real_estate_credit_guarantees(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ONR Garantias e Crédito Imobiliário ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Alienação Fiduciária: {res.get('contratos_alienacao_fiduciaria_ano'):,} contratos | R$ {res.get('volume_credito_imobiliario_garantido_milhoes_brl')}M")
                    print(f"  SFI/SBPE / Home Eq.:  {res.get('participacao_sfi_sbpe_pmcmv_pct')}% | {res.get('contratos_home_equity_cgi_ano'):,} CGI")
                    print(f"  Execução Garantia:    {res.get('taxa_execucao_garantia_fiduciaria_pct')}% ({res.get('consolidacoes_propriedade_inadimplencia_ano')} consolidações)")
                return 0

            if onr_action == "serp":
                res = onr_conn.get_onr_serp_interoperability(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ONR Interoperabilidade SERP/SAEC ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                    print(f"  Integração / NatoDig: {res.get('indice_integracao_serp_saec_pct')}% SERP | {res.get('proporcao_titulos_nato_digitais_extrato_pct')}% nato-digitais")
                    print(f"  SLA Qualificação/Cert:{res.get('prazo_medio_qualificacao_registral_dias')} dias | {res.get('prazo_medio_certidao_eletronica_horas')} horas")
                    print(f"  Ordens CNIB / Penhora:{res.get('ordens_indisponibilidade_cnib_processadas_ano'):,} CNIB | {res.get('penhoras_online_arisp_srei_ano'):,} ARISP")
                return 0

            if onr_action == "features":
                feat = onr_conn.get_onr_real_estate_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"ONR/SREI Features Registrais Imobiliárias: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                    print(f"  Score Maturidade SERP: {feat.get('score_maturidade_digital_srei_serp')}/100.0")
                    print(f"  Score Liquidez/Seg.:   {feat.get('score_liquidez_seguranca_imobiliaria')}/100.0")
                    print(f"  Classificação Mercado: {feat.get('classificacao_mercado_registral_imobiliario')}")
                return 0

            if onr_action == "ranking":
                ranks = onr_conn.list_top_onr_real_estate_municipalities(
                    limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                )
                if getattr(args, "json", False):
                    print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(ranks)} Municípios em Maturidade Digital SERP & Liquidez Imobiliária (ONR):")
                    for idx, r in enumerate(ranks, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): SERP Score {r.get('score_maturidade_digital_srei_serp')} | Liquidez: {r.get('score_liquidez_seguranca_imobiliaria')} | {r.get('classificacao_mercado_registral_imobiliario')}")
                return 0

        elif args.command == "iptu-itbi":
            from public_data_gateway.connectors.imobiliario.capitais_iptu_itbi.connector import (
                CapitaisIptuItbiConnector,
            )

            iptu_conn = CapitaisIptuItbiConnector(settings=settings)
            iptu_action = getattr(args, "iptu_action", None)
            try:
                if iptu_action == "cadastre":
                    res = iptu_conn.get_iptu_itbi_property_cadastre(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Capitais IPTU Cadastro Imobiliário ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Inscrições / Área:   {res.get('inscricoes_imobiliarias_ativas_iptu'):,} | {res.get('area_construida_tributada_mil_m2'):,.1f} mil m²")
                        print(f"  Venal / Arrecad.:    R$ {res.get('valor_venal_medio_m2_brl')}/m² | IPTU R$ {res.get('arrecadacao_iptu_milhoes_brl')}M ({res.get('taxa_adimplencia_iptu_pct')}% adimplência)")
                    return 0

                if iptu_action == "transactions":
                    rows = iptu_conn.list_itbi_transactions_by_segment(
                        args.id_municipio, segmento_imovel=getattr(args, "segmento", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Capitais ITBI Transações por Segmento ({args.id_municipio}) - {len(rows)} segmento(s):")
                        for r in rows:
                            print(f"  - {r.get('segmento_imovel_itbi')}: {r.get('quantidade_transacoes_itbi_ano'):,} transações | R$ {r.get('valor_transacionado_itbi_milhoes_brl')}M (Ticket R$ {r.get('valor_medio_transacao_mil_brl')}k)")
                    return 0

                if iptu_action == "prices":
                    res = iptu_conn.get_iptu_itbi_price_dynamics(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Capitais IPTU/ITBI Dinâmica de Preços ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  Preço ITBI / Var.:   R$ {res.get('preco_medio_m2_itbi_declarado_brl')}/m² ({res.get('variacao_anual_preco_m2_itbi_pct'):+.2f}% a.a.)")
                        print(f"  PGV/Mercado & Liq.:  {res.get('razao_venal_pgv_sobre_mercado_itbi')}x | Liquidez {res.get('indice_liquidez_imobiliaria_itbi_pct')}%")
                    return 0

                if iptu_action == "governance":
                    res = iptu_conn.get_iptu_itbi_fiscal_governance(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Capitais IPTU/ITBI Governança CTM ({res.get('nome_municipio')} - {res.get('id_municipio')}):")
                        print(f"  CTM Geo / SINTER:    {res.get('cobertura_cadastral_georreferenciada_ctm_pct')}% CTM | {res.get('integracao_iptu_sinter_cno_onr_pct')}% SINTER/ONR")
                        print(f"  ITBI Digital / Rec.: {res.get('transacoes_itbi_emissao_digital_pct')}% digital | R$ {res.get('arrecadacao_itbi_milhoes_brl')}M ITBI")
                    return 0

                if iptu_action == "features":
                    feat = iptu_conn.get_iptu_itbi_real_estate_features(args.id_municipio)
                    if getattr(args, "json", False):
                        print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Capitais IPTU/ITBI Features: {feat.get('nome_municipio')} ({feat.get('sigla_uf')} - {feat.get('id_municipio')}):")
                        print(f"  Score Dinamismo ITBI:{feat.get('score_dinamismo_mercado_imobiliario_itbi')}/100.0")
                        print(f"  Score Governança CTM:{feat.get('score_governanca_cadastral_fiscal_iptu')}/100.0")
                        print(f"  Classificação:       {feat.get('classificacao_mercado_imobiliario_capital')}")
                    return 0

                if iptu_action == "ranking":
                    ranks = iptu_conn.list_top_iptu_itbi_capitals(
                        limit=getattr(args, "limit", 10), sigla_uf=getattr(args, "uf", None)
                    )
                    if getattr(args, "json", False):
                        print(json.dumps(ranks, indent=2, ensure_ascii=False, default=str))
                    else:
                        print(f"Top {len(ranks)} Capitais em Dinamismo Imobiliário ITBI & Governança CTM (IPTU/ITBI):")
                        for idx, r in enumerate(ranks, 1):
                            print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): Dinamismo {r.get('score_dinamismo_mercado_imobiliario_itbi')} | Governança: {r.get('score_governanca_cadastral_fiscal_iptu')} | {r.get('classificacao_mercado_imobiliario_capital')}")
                    return 0
            except ValueError as exc:
                print(f"Erro Capitais IPTU/ITBI: {exc}", file=sys.stderr)
                return 1

        elif args.command == "pib-municipal":
            from public_data_gateway.connectors.demografico.ibge_pib_municipal.connector import (
                IBGEPibMunicipalConnector,
            )

            pib_conn = IBGEPibMunicipalConnector()
            if args.pib_mun_action == "detail":
                res = pib_conn.get_municipal_gdp_detail(args.id_municipio, year=getattr(args, "year", 2023))
                if getattr(args, "json", False):
                    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
                else:
                    if not res:
                        print(f"No PIB municipal data found for '{args.id_municipio}'.", file=sys.stderr)
                        return 1
                    print(f"IBGE PIB Municipal Detail: {res.get('nome_municipio', '')} ({res.get('sigla_uf', '')} - {res.get('id_municipio')}):")
                    print(f"  Ano:                     {res.get('ano')}")
                    print(f"  PIB Total:               R$ {res.get('valor_pib_mil_reais', 0.0):,.2f} mil")
                    print(f"  Impostos Líquidos:       R$ {res.get('valor_impostos_liquidos_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Total:               R$ {res.get('vab_total_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Agropecuária:        R$ {res.get('vab_agropecuaria_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Indústria:           R$ {res.get('vab_industria_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Serviços:            R$ {res.get('vab_servicos_mil_reais', 0.0):,.2f} mil")
                    print(f"  VAB Adm Pública:         R$ {res.get('vab_administracao_publica_mil_reais', 0.0):,.2f} mil")
                    print(f"  Participação Brasil:     {res.get('participacao_pib_brasil_pct')}%")
                    print(f"  Participação UF:         {res.get('participacao_pib_uf_pct')}%")
                return 0

            if args.pib_mun_action == "features":
                feat = pib_conn.get_municipal_gdp_features(args.id_municipio)
                if getattr(args, "json", False):
                    print(json.dumps(feat, indent=2, ensure_ascii=False, default=str))
                else:
                    if not feat:
                        print(f"No Gold PIB features found for '{args.id_municipio}'.", file=sys.stderr)
                        return 1
                    print(f"IBGE Municipal Economic Features: {feat.get('nome_municipio', '')} ({feat.get('sigla_uf', '')} - {feat.get('id_municipio')}):")
                    print(f"  PIB Total:               R$ {feat.get('valor_pib_mil_reais', 0.0):,.2f} mil")
                    print(f"  Vocação Econômica:       {feat.get('vocacao_economica')}")
                    print(f"  Score Peso Econômico:    {feat.get('score_peso_economico_nacional')} / 100")
                    print(f"  Score Diversificação:    {feat.get('score_diversificacao_produtiva')} / 100")
                    print(f"  Share Agropecuária:      {feat.get('share_vab_agropecuaria_pct')}%")
                    print(f"  Share Indústria:         {feat.get('share_vab_industria_pct')}%")
                    print(f"  Share Serviços:          {feat.get('share_vab_servicos_pct')}%")
                    print(f"  Share Adm Pública:       {feat.get('share_vab_administracao_publica_pct')}%")
                return 0

            if args.pib_mun_action == "top":
                top = pib_conn.list_top_municipalities(limit=args.limit)
                if getattr(args, "json", False):
                    print(json.dumps(top, indent=2, ensure_ascii=False, default=str))
                else:
                    print(f"Top {len(top)} Brazilian Municipalities by GDP (IBGE Contas Regionais):")
                    for idx, r in enumerate(top, 1):
                        print(f"  {idx}. {r.get('nome_municipio')} ({r.get('sigla_uf')}): R$ {r.get('valor_pib_mil_reais', 0.0):,.2f} mil | Part. Brasil: {r.get('participacao_pib_brasil_pct')}%")
                return 0

    except Exception as exc:
        print(f"Error executing command '{args.command}': {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
