# Changelog

All notable changes to `public-data-gateway` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **114 Homologated Medallion Connectors (Phases 00–11.5 — 11 Gates):**
  - **85 Official Brazilian Public Data Connectors (`source_origin: GOV_BR`)** across 20 macrosegments (Geography, Macroeconomics, Public Procurement, Compliance/Sanctions, Capital Markets, Demographics, Labor, Public Finance, Education, Health, Real Estate, Infrastructure, Environment/ESG, Agriculture, Energy, Mobility/Road Safety, Disasters, Elections, Intellectual Property, and Social Benefits).
  - **29 Public & Global Analytics/ML Connectors (`source_origin: PUBLIC_GLOBAL`)** across Blocks A–E (KYC/KYB, Financial Markets, Geospatial/Climate, Research/Global News, and Open ML enrichment).
- **Analytics Engineering & Cross-Domain Gold Warehouse (`dbt/` + DuckDB):**
  - **698 dbt models**, **396 dbt sources**, **9,390 documented columns** (`100%` coverage enforced by `scripts/lint_dbt_docs.py`), and **8,964 automated dbt tests**.
  - Multi-disciplinary Gold marts (`mart_cross_domain_territorio_b2b_macro_brasil`, `mart_cross_domain_esg_agro_infra_brasil`, `mart_cross_domain_capital_humano_saude_brasil`, `feature_store_cross_domain_municipio` with DuckDB `ASOF` point-in-time joins) and 9 GTM Lifecycle marts (`S01..S09` x `P01..P10`).
- **Open-Core Cloud Lakehouse Mirror (Phase 11):**
  - `FreshnessSlaAuditor`, `CdcDeltaTracker` (`SHA-256` & row-count delta detection), `CloudLakehouseMirror` (S3, Cloudflare R2, local backend), and read-only `PublicWarehouseAdapter` (DuckDB, MotherDuck, BigQuery).
- **Hexagonal Medallion Core, Governance & Multi-Interface Parity:**
  - Single master declarative catalog (`catalog/sources_registry.yaml` + `CATALOG.md`) covering all 114 sources, living implementation tracker (`JORNADA_IMPLEMENTACAO.md` + `state/project_progress.json`), and Architecture Decision Records (`ADR-001`–`ADR-004`).
  - Hybrid `EnrichmentEngine` (Mod-11 check-digit validation, municipality entity resolution, Client Path Shield, CWE-1236 formula sanitization, Zip Bomb protection, and OWASP LLM01 `<untrusted_gov_data>` prompt-injection wrappers).
  - Full 4-interface parity across Python SDK (`PublicData` / `BrazilData`), FastAPI (`/api/v1`), CLI (`pdg` / `bdg`), and MCP Server (`pdg-mcp` / `bdg-mcp`).

### Changed
- Unified `catalog/sources_registry.yaml` and `catalog/public_apis_registry.yaml` into a single master catalog (`114` sources: `85 GOV_BR` + `29 PUBLIC_GLOBAL`) with `source_origin` and accent-insensitive keyword search (`query`/`q`) exposed across SDK, FastAPI, CLI, and MCP.

### Removed
- Removed split catalog file `catalog/public_apis_registry.yaml` after consolidating all 114 sources into `catalog/sources_registry.yaml`.
