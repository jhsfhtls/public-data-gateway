# Changelog

All notable changes to `brazil-data-gateway` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial repository bootstrap, C.O.R.E. satellite governance shield, GitHub Actions CI/CD (`ci.yml`, `pr-lint.yml`), and PR templates.
- Hexagonal Medallion Core (`Bronze -> Quarantine/Silver -> Gold`), ODCS v3.1 data contracts, 5-Pillar Data Observability gates, and DevSecOps hardening (`POP_T_175`).
- Master declarative catalog (`catalog/sources_registry.yaml` + `CATALOG.md`) covering 44+ official Brazilian public data sources across all 13 macrosegments (`POP_T_176` & `POP_T_177`), including ANA Hidroweb and Suframa PIM/ZFM.
- Living implementation tracker (`JORNADA_IMPLEMENTACAO.md` + `state/project_progress.json`) and Architecture Decision Records (`docs/adr/`).
- Hybrid `EnrichmentEngine` (Mod-11 check-digit gate, fuzzy municipality entity resolution, DuckDB `ASOF` point-in-time joins, cross-repo client folder isolation).
- Unified consumption interfaces: Python SDK (`BrazilData`), FastAPI (`/api/v1`), CLI (`bdg`), and MCP Server (`bdg-mcp`).
