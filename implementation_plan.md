# Plano de Implementação: ibge_malhas (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `ibge_malhas` (IBGE: Malhas Territoriais Vetoriais - Municípios e UFs)
- **Agência:** Instituto Brasileiro de Geografia e Estatística (IBGE)
- **Legislação:** Lei nº 5.534/1968, Lei nº 5.878/1973, Decreto nº 73.177/1973, Portaria IBGE nº 123/2021.
- **Segmento:** `05_geografico_logistica`
- **Protocolo:** `OGC_WFS_SHP` / GeoJSON API (`https://servicodados.ibge.gov.br/api/v3/malhas/`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-IBGE-MALHAS-001`, PKs: `id_municipio`, `sigla_uf`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` com extração de 4 tabelas Silver (`malhas_municipios`, `malhas_ufs`, `malhas_regioes`, `malhas_brasil`), Gold Feature Store (`features_malhas_territoriais_municipios_brasil.parquet`), scores de compacidade territorial e isolamento geográfico, e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/ibge_malhas/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_ibge_malhas_*`) com 100% de documentação e testes em todas as colunas.
- [x] **Gate 5: dbt Core:** `fct_malhas_geometria_municipio.sql` e `dim_malha_territorial_uf.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_inteligencia_territorial_fronteiras_brasil.sql` documentado em `_marts_models.yml`.
- [x] **Gate 7: dbt Build & Tests:** 243 modelos dbt, 2.430 testes dbt 100% PASS, `lint_dbt_docs.py` aprovado com 100% de cobertura (2.982 colunas auditadas).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.geo.malhas_*`, `get_municipio_boundary`, `get_uf_boundary`, `get_territorial_features`, `list_top_compact_municipalities`.
  - CLI: `bdg malhas municipio`, `bdg malhas uf`, `bdg malhas features`, `bdg malhas ranking`.
  - REST API: `/api/v1/geo/malhas/*` (top-compact, features, uf, municipio) e atalho `/api/v1/malhas/{id_municipio}`.
  - MCP: `bdg_get_municipio_boundary`, `bdg_get_uf_boundary`, `bdg_get_territorial_features`, `bdg_list_top_compact_municipalities` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 9 testes unitários/integração + 1 teste live fail-hard 100% PASS. Suíte offline global 100% PASS.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (26/49 fontes homologadas — 53.1%), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` perfeitamente sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes, squash-merge na `main`.
