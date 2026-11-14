# Plano de Implementação: inep_censo_escolar (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `inep_censo_escolar` (INEP: Microdados do Censo Escolar da Educação Básica)
- **Agência:** Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)
- **Legislação:** Lei nº 9.394/1996 (LDB), Lei nº 13.005/2014 (PNE), Portaria MEC nº 316/2007 (Educacenso/INEP), Decreto nº 8.777/2016 (Política de Dados Abertos).
- **Segmento:** `07_educacao_capital_humano` (Wave 5)
- **Protocolo:** `HTTP_BULK_ZIP` / Portais Públicos INEP (`https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar` e `https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/sinopses-estatisticas/educacao-basica`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`, `cep_8`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-INEP-CENSO-ESCOLAR-001`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` (`INEPCensoEscolarConnector`) com extração de 4 tabelas Silver (`censo_escolar_escolas_infraestrutura`, `censo_escolar_matriculas_etapas`, `censo_escolar_docentes_qualificacao`, `censo_escolar_rendimento_fluxo`), Gold Feature Store (`features_censo_escolar_educacao_basica_municipios_brasil.parquet`), scores normalizados (0-100) de infraestrutura escolar e qualidade do fluxo educacional, registro dinâmico `register_connector` e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/inep_censo_escolar/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_censo_escolar_escolas_infraestrutura`, `stg_censo_escolar_matriculas_etapas`, `stg_censo_escolar_docentes_qualificacao`, `stg_censo_escolar_rendimento_fluxo`) com 100% de documentação e testes `not_null` / `unique`.
- [x] **Gate 5: dbt Core:** `fct_censo_escolar_infraestrutura_docentes_municipio.sql` e `fct_censo_escolar_matriculas_etapas_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_infraestrutura_educacao_basica_brasil.sql` documentado em `dbt/models/marts/educacao/_marts_models.yml` com paridade exata frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_infraestrutura_educacao_basica_brasil +fct_censo_escolar_matriculas_etapas_municipio`) com `PASS=167 TOTAL=167` e `lint_dbt_docs.py` aprovado com 100% de cobertura (278 modelos, 3.683 colunas, 150 fontes).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.education.censo_escolar_connector`, `get_censo_escolar_infrastructure`, `list_censo_escolar_enrollments`, `get_censo_escolar_teachers`, `get_censo_escolar_yield_flow`, `get_censo_escolar_features`, `list_top_censo_escolar_municipalities`.
  - CLI: `bdg censo-escolar schools`, `enrollments`, `teachers`, `flow`, `features`, `ranking`.
  - REST API: `/api/v1/education/censo-escolar/*` e atalho `/api/v1/censo-escolar/{id_municipio}`.
  - MCP: `bdg_get_censo_escolar_schools`, `bdg_get_censo_escolar_teachers`, `bdg_get_censo_escolar_features`, `bdg_list_top_censo_escolar_municipalities` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline + 1 teste live fail-hard contra portais oficiais do INEP.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (31/49 fontes homologadas — 63.3%, terceiro conector da Wave 5), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes e squash-merge em `main`.
