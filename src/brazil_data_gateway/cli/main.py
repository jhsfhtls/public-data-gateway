"""Command-Line Interface (CLI) entrypoint for Brazil Data Gateway (bdg)."""

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from brazil_data_gateway.core.catalog import CatalogRegistry
from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.connector import get_connector
from brazil_data_gateway.core.contracts import load_contract
from brazil_data_gateway.core.storage import HybridStorageEngine
from brazil_data_gateway.enrichment.engine import EnrichmentEngine


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
            from brazil_data_gateway.connectors.demografico.ibge_localidades import (
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

    except Exception as exc:
        print(f"Error executing command '{args.command}': {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
