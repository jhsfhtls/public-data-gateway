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

    except Exception as exc:
        print(f"Error executing command '{args.command}': {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
