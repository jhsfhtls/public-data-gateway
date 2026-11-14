# Plano de Implementação: datasus_sih (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `datasus_sih` (DataSUS: Sistema de Informações Hospitalares do SUS — Produção AIH, Custos Hospitalares/UTI, Morbidade CID-10 e Desfechos Clínicos/ICSAP)
- **Agência:** Ministério da Saúde / DataSUS
- **Legislação:** Lei nº 8.080/1990 (SUS), Portaria GM/MS nº 221/2008 (Lista Brasileira ICSAP), Portaria SAES/MS nº 1.324/2014 (Manual SIH/SUS - AIH), Decreto nº 8.777/2016.
- **Segmento:** `06_saude_publica` (Wave 5)
- **Protocolo:** `FTP_DBC` / Portais Públicos DataSUS (`https://datasus.saude.gov.br/transferencia-de-arquivos/` e `https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-DATASUS-SIH-001`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` com extração de 4 tabelas Silver (`sih_internacoes_aih_municipio`, `sih_custos_hospitalares_sus`, `sih_morbidade_capitulos_cid10`, `sih_desfechos_mortalidade_hospitalar`), Gold Feature Store (`features_sih_morbidade_custos_hospitalares_municipios_brasil.parquet`), scores normalizados (0-100) de complexidade assistencial SIH e pressão epidemiológica hospitalar, registro dinâmico `register_connector` e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/datasus_sih/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_sih_internacoes_aih_municipio`, `stg_sih_custos_hospitalares_sus`, `stg_sih_morbidade_capitulos_cid10`, `stg_sih_desfechos_mortalidade_hospitalar`) com 100% de documentação e testes `not_null` (e `unique` nas chaves municipais).
- [x] **Gate 5: dbt Core:** `fct_sih_producao_custos_hospitalares_municipio.sql` e `fct_sih_morbidade_cid10_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_morbidade_custos_hospitalares_sus_brasil.sql` documentado em `dbt/models/marts/saude/_marts_models.yml` com paridade exata frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_morbidade_custos_hospitalares_sus_brasil +fct_sih_morbidade_cid10_municipio`) com 175/175 PASS e 0 erros; `lint_dbt_docs.py` aprovado com 100% de cobertura (271 modelos, 3.541 colunas, 146 fontes auditadas).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.health.sih_connector`, `get_sih_admissions`, `get_sih_hospital_costs`, `list_sih_cid10_morbidity`, `get_sih_mortality_outcomes`, `get_sih_morbidity_features`, `list_top_sih_complexity_hubs`.
  - CLI: `bdg sih admissions`, `bdg sih costs`, `bdg sih cid10`, `bdg sih outcomes`, `bdg sih features`, `bdg sih ranking`.
  - REST API: `/api/v1/health-public/sih/*` (`top-hubs`, `features`, `costs`, `cid10`, `outcomes`, `admissions`) e atalho `/api/v1/sih/{id_municipio}`.
  - MCP: `bdg_get_sih_admissions`, `bdg_get_sih_hospital_costs`, `bdg_get_sih_morbidity_features`, `bdg_list_top_sih_complexity_hubs` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline (contrato, scores, baseline Parquets, métodos/registro do conector, SDK, CLI, REST API e MCP) + 1 teste live fail-hard.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (30/49 fontes homologadas — 61.2%, segundo conector da Wave 5), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes, squash-merge na `main`.
