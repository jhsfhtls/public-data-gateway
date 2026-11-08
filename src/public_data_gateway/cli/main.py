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

    except Exception as exc:
        print(f"Error executing command '{args.command}': {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
