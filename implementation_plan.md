# Plano de Implementação: datasus_cnes (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `datasus_cnes` (DataSUS: Cadastro Nacional de Estabelecimentos de Saúde — Estabelecimentos, Leitos/UTI, Equipamentos Diagnósticos e Força de Trabalho)
- **Agência:** Ministério da Saúde / DataSUS
- **Legislação:** Lei nº 8.080/1990 (SUS), Portaria de Consolidação GM/MS nº 1/2017, Portaria SAES/MS nº 1.646/2015, Decreto nº 8.777/2016.
- **Segmento:** `06_saude_publica` (Wave 5)
- **Protocolo:** `PORTAL_WEB` / `REST_JSON` (`https://cnes.datasus.gov.br/` e `https://apidadosabertos.saude.gov.br/cnes/estabelecimentos?limit=1`)
- **Chaves Canônicas:** `cnpj_14`, `id_municipio`, `sigla_uf`, `cep_8`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-DATASUS-CNES-001`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` com extração de 4 tabelas Silver (`cnes_estabelecimentos_saude`, `cnes_leitos_hospitalares_uti`, `cnes_equipamentos_diagnostico`, `cnes_profissionais_equipes_saude`), Gold Feature Store (`features_cnes_capacidade_assistencial_municipios_brasil.parquet`), scores normalizados (0-100) de capacidade hospitalar e alta complexidade UTI, registro dinâmico `register_connector` e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/datasus_cnes/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_cnes_estabelecimentos_saude`, `stg_cnes_leitos_hospitalares_uti`, `stg_cnes_equipamentos_diagnostico`, `stg_cnes_profissionais_equipes_saude`) com 100% de documentação e testes `not_null` (e `unique` nas chaves municipais).
- [x] **Gate 5: dbt Core:** `fct_cnes_infraestrutura_hospitalar_municipio.sql` e `fct_cnes_forca_trabalho_saude_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_capacidade_assistencial_saude_brasil.sql` documentado em `dbt/models/marts/saude/_marts_models.yml` com paridade exata frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_capacidade_assistencial_saude_brasil +fct_cnes_forca_trabalho_saude_municipio`) com 192/192 PASS e 0 erros; `lint_dbt_docs.py` aprovado com 100% de cobertura (264 modelos, 3.408 colunas, 142 fontes auditadas).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.health.cnes_connector`, `get_cnes_establishments`, `get_cnes_beds_icu`, `get_cnes_equipment`, `list_cnes_workforce`, `get_cnes_health_features`, `list_top_health_hubs`.
  - CLI: `bdg cnes establishments`, `bdg cnes beds`, `bdg cnes equipment`, `bdg cnes workforce`, `bdg cnes features`, `bdg cnes ranking`.
  - REST API: `/api/v1/health-public/cnes/*` (`top-hubs`, `features`, `beds`, `equipment`, `workforce`, `establishments`) e atalho `/api/v1/cnes/{id_municipio}`.
  - MCP: `bdg_get_cnes_establishments`, `bdg_get_cnes_beds_icu`, `bdg_get_cnes_health_features`, `bdg_list_top_health_hubs` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline (contrato, scores, baseline Parquets, métodos/registro do conector, SDK, CLI, REST API e MCP) + 1 teste live fail-hard 100% PASS (9/9).
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (29/49 fontes homologadas — 59.2%, primeiro conector da Wave 5), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes, squash-merge na `main`.
