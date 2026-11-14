# ADR-004: Paridade Obrigatoria Medallion, dbt (Staging/Intradominio/Cross-Domain) e 11-Gates para Todas as APIs

* **Status:** Accepted
* **Data:** 2026-11-14
* **Contexto:** Na Fase 10, 29 APIs publicas e globais foram integradas inicialmente em um Hub sincrono (`PublicApisHubService`) para acelerar a cobertura de Analytics, KYC/KYB, Espacial e ML. Entretanto, para manter a consistencia de governanca, observabilidade, Feature Store e reuso em modelos dbt/DuckDB, o projeto exige que **toda API** possua exatamente o mesmo rigor estrutural dos 49 conectores das Waves 1 a 6.

## Decisao
Fica estabelecido que **nenhuma API** deve permanecer apenas como cliente HTTP sincrono isolado. Toda API homologada deve possuir:

1. **Catalogo:** Entrada validada no catalogo mestre (`sources_registry.yaml` / `public_apis_registry.yaml`) e documentacao sincronizada (`CATALOG.md` / `CATALOG_PUBLIC_APIS.md`).
2. **Conector & ODCS v3.1:** Implementacao via `BaseConnector` + `contract.yaml` + isolamento `data/<connector_id>/{bronze,silver,gold,quarantine}/`.
3. **dbt Completo:**
   - `staging`: `source.yml` + `stg_*` com 100% de cobertura no `lint_dbt_docs.py`.
   - `intradominio`: `dim_*` / `fct_*` / `mart_*` especificos do dominio.
   - `cross_domain`: propagacao de features para `feature_store_cross_domain_municipio` / marts cross-domain.
4. **Testes & Gates:** `GATE_01..GATE_11` completos, testes offline deterministicos e testes `live` (`tests/live/`) de 1 linha com fail-hard.
5. **Paridade Multi-Interface:** Metodos dedicados em **SDK**, **FastAPI**, **CLI** e **MCP** (`wrap_untrusted_gov_data`).

## Estrategia para APIs Parametrizadas (Borda / Spatial / KYC)
Para APIs que dependem de parametro de entrada (CEP, CNPJ, coordenada lat/lon, dominio, ticker), a ingestao em lote (`ingest`) utiliza um **conjunto canonico de referencia** (ex.: 27 capitais/UFs ou entidades-semente publicas) para materializar as tabelas Bronze/Silver/Gold e alimentar os modelos dbt, mantendo tambem a consulta sob demanda sincronizada com cache WAL.

## Consequencias
* A **Fase 10.5** executara a migracao e elevacao de todas as APIs da Fase 10 para este padrao em 4 waves dedicadas antes da Fase 11 (Cloud Lakehouse Mirror).
