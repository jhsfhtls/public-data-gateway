# Plano de Implementação: datasus_sim (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `datasus_sim` (DataSUS: Sistema de Informações sobre Mortalidade - SIM)
- **Agência:** Ministério da Saúde / DataSUS
- **Legislação:** Lei nº 6.015/1973 (Lei de Registros Públicos - DO), Lei nº 8.080/1990 (Lei Orgânica da Saúde), Portaria SVS/MS nº 116/2009 (Regulamentação do fluxo SIM/SINASC) e Decreto nº 8.777/2016 (Política de Dados Abertos).
- **Segmento:** `06_saude_publica` (Wave 5)
- **Protocolo:** `FTP_DBC` / Portais Públicos DataSUS SIM (`https://datasus.saude.gov.br/transferencia-de-arquivos/` e `https://datasus.saude.gov.br/mortalidade-desde-1996-pela-cid-10/`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-DATASUS-SIM-001`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` (`DataSUSSimConnector`) com extração de 4 tabelas Silver (`sim_obitos_gerais_municipio`, `sim_causas_capitulos_cid10`, `sim_mortalidade_materno_infantil`, `sim_mortalidade_prematura_dcnt_externas`), Gold Feature Store (`features_sim_mortalidade_causas_cid10_municipios_brasil.parquet`), scores normalizados (0-100) de vigilância de óbitos e carga de mortalidade evitável, registro dinâmico `register_connector` e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/datasus_sim/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_sim_obitos_gerais_municipio`, `stg_sim_causas_capitulos_cid10`, `stg_sim_mortalidade_materno_infantil`, `stg_sim_mortalidade_prematura_dcnt_externas`) com 100% de documentação e testes `not_null` / `unique`.
- [x] **Gate 5: dbt Core:** `fct_sim_mortalidade_vigilancia_municipio.sql` e `fct_sim_mortalidade_capitulos_cid10_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_mortalidade_epidemiologia_sim_brasil.sql` documentado em `dbt/models/marts/saude/_marts_models.yml` com paridade exata frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_mortalidade_epidemiologia_sim_brasil +fct_sim_mortalidade_capitulos_cid10_municipio`) com `PASS=161 TOTAL=161` e `lint_dbt_docs.py` aprovado com 100% de cobertura (285 modelos, 3.802 colunas, 154 fontes).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.health.sim_connector`, `get_sim_general_mortality`, `list_sim_cid10_causes`, `get_sim_maternal_infant_mortality`, `get_sim_premature_dcnt_external`, `get_sim_mortality_features`, `list_top_sim_surveillance_municipalities`.
  - CLI: `bdg sim general`, `cid10`, `maternal-infant`, `premature`, `features`, `ranking`.
  - REST API: `/api/v1/health-public/sim/*` e atalho `/api/v1/sim/{id_municipio}`.
  - MCP: `bdg_get_sim_general_mortality`, `bdg_get_sim_maternal_infant_mortality`, `bdg_get_sim_mortality_features`, `bdg_list_top_sim_surveillance_municipalities` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline + 1 teste live fail-hard contra portais oficiais do DataSUS SIM.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (32/49 fontes homologadas — 65.3%, quarto conector da Wave 5), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes e squash-merge em `main`.
