# Plano de Implementação: antt_malha_transporte (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `antt_malha_transporte` (ANTT: Malha de Transporte Rodoviário e Ferroviário de Cargas)
- **Agência:** Agência Nacional de Transportes Terrestres (ANTT)
- **Legislação:** Lei nº 10.233/2001, Lei nº 11.442/2007, Resolução ANTT nº 5.982/2022, Decreto nº 8.777/2016.
- **Segmento:** `05_geografico_logistica`
- **Protocolo:** `PORTAL_CKAN_JSON` / Portais Públicos ANTT (`https://dados.antt.gov.br/` e `https://dados.antt.gov.br/api/3/action/site_read`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-ANTT-MALHA-001`, PKs: `id_municipio`, `sigla_uf`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` com extração de 4 tabelas Silver (`antt_corredores_rodoviarios`, `antt_frota_rntrc_municipio`, `antt_malha_ferroviaria_terminais`, `antt_fluxo_logistico_modal`), Gold Feature Store (`features_malha_transporte_logistica_municipios_brasil.parquet`), scores normalizados (0-100) de infraestrutura logística e intermodalidade, registro dinâmico `register_connector` e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/antt_malha_transporte/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_antt_corredores_rodoviarios`, `stg_antt_frota_rntrc_municipio`, `stg_antt_malha_ferroviaria_terminais`, `stg_antt_fluxo_logistico_modal`) com 100% de documentação e testes `not_null` (e `unique` nas chaves municipais).
- [x] **Gate 5: dbt Core:** `fct_antt_infraestrutura_logistica_municipio.sql` e `fct_antt_matriz_modal_cargas_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_logistica_corredores_transporte_brasil.sql` documentado em `_marts_models.yml` com paridade exata frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_logistica_corredores_transporte_brasil +fct_antt_matriz_modal_cargas_municipio`) com 175/175 PASS e 0 erros; `lint_dbt_docs.py` aprovado com 100% de cobertura (257 modelos, 3.258 colunas auditadas).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.geo.antt_connector`, `get_highway_corridors`, `get_rntrc_fleet_profile`, `get_rail_intermodal_terminals`, `list_modal_flows`, `get_logistics_features`, `list_top_logistics_hubs`.
  - CLI: `bdg antt highways`, `bdg antt rntrc`, `bdg antt rail`, `bdg antt modals`, `bdg antt features`, `bdg antt ranking`.
  - REST API: `/api/v1/geo/antt/*` (`top-hubs`, `features`, `rntrc`, `rail`, `modals`, `highways`) e atalho `/api/v1/antt/{id_municipio}`.
  - MCP: `bdg_get_antt_highway_corridors`, `bdg_get_antt_rntrc_fleet`, `bdg_get_antt_logistics_features`, `bdg_list_top_logistics_hubs` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline (contrato, scores, baseline Parquets, métodos/registro do conector, SDK, CLI, REST API e MCP) + 1 teste live fail-hard 100% PASS.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (28/49 fontes homologadas — 57.1%, Wave 4 100% concluída), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes, squash-merge na `main`.
