# Plano de Implementação: inep_censo_superior (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `inep_censo_superior` (INEP: Microdados do Censo da Educação Superior)
- **Agência:** Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)
- **Legislação:** Lei nº 9.394/1996 (LDB), Lei nº 10.861/2004 (SINAES/ENADE/IGC), Lei nº 13.005/2014 (PNE), Decreto nº 9.235/2017 e Decreto nº 8.777/2016 (Dados Abertos).
- **Segmento:** `07_educacao_capital_humano` (Wave 5)
- **Protocolo:** `HTTP_BULK_ZIP` / Portais Oficiais INEP Censo da Educação Superior (`https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior` e `https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-da-educacao-superior`)
- **Chaves Canônicas:** `cnpj_14`, `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-INEP-CENSO-SUPERIOR-001`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` (`INEPCensoSuperiorConnector`) com extração de 4 tabelas Silver (`censo_superior_instituicoes_ies`, `censo_superior_cursos_areas_cine`, `censo_superior_matriculas_ingressantes_concluintes`, `censo_superior_docentes_titulacao_regime`), Gold Feature Store (`features_censo_superior_universidades_cursos_municipios_brasil.parquet`), scores normalizados (0-100) de excelência acadêmica/pesquisa IES e formação de capital humano superior, registro dinâmico `register_connector` e `verify_live_connectivity()` / `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/inep_censo_superior/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_censo_superior_instituicoes_ies`, `stg_censo_superior_cursos_areas_cine`, `stg_censo_superior_matriculas_ingressantes_concluintes`, `stg_censo_superior_docentes_titulacao_regime`) com 100% de documentação e testes `not_null` / `unique`.
- [x] **Gate 5: dbt Core:** `fct_censo_superior_ies_docentes_fluxo_municipio.sql` e `fct_censo_superior_cursos_areas_cine_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_ecossistema_educacao_superior_brasil.sql` documentado em `dbt/models/marts/educacao/_marts_models.yml` com paridade numérica frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_ecossistema_educacao_superior_brasil +fct_censo_superior_cursos_areas_cine_municipio`) com `PASS=166 TOTAL=166` e `lint_dbt_docs.py` aprovado com 100% de cobertura (292 modelos, 3.943 colunas, 158 fontes).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.education.censo_superior_connector`, `get_censo_superior_institutions`, `list_censo_superior_cine_courses`, `get_censo_superior_enrollments_graduates`, `get_censo_superior_faculty_profile`, `get_censo_superior_features`, `list_top_censo_superior_hubs`.
  - CLI: `bdg censo-superior institutions`, `courses`, `enrollments`, `faculty`, `features`, `ranking`.
  - REST API: `/api/v1/education/censo-superior/*` e atalho `/api/v1/censo-superior/{id_municipio}`.
  - MCP: `bdg_get_censo_superior_institutions`, `bdg_get_censo_superior_faculty`, `bdg_get_censo_superior_features`, `bdg_list_top_censo_superior_hubs` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline + 1 teste live fail-hard contra portais oficiais do INEP Censo da Educação Superior.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (33/49 fontes homologadas — 67.3%, quinto conector da Wave 5), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes e squash-merge em `main`.
