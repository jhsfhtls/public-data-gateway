# Plano de Implementação: cnj_justica_aberta (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `cnj_justica_aberta` (CNJ: Justiça Aberta - Cadastro e Produtividade de Cartórios)
- **Agência:** Conselho Nacional de Justiça (CNJ)
- **Legislação:** Lei nº 8.935/1994 (Lei dos Cartórios), Lei nº 6.015/1973 (Registros Públicos), Lei nº 10.169/2000 (Emolumentos), Provimentos CNJ nº 24/2012 e nº 149/2023 e Decreto nº 8.777/2016 (Dados Abertos).
- **Segmento:** `12_imobiliario_cartorial` (Wave 5)
- **Protocolo:** `REST_JSON` / Portais Oficiais CNJ Justiça Aberta (`https://www.cnj.jus.br/corregedoria/justica_aberta/` e `https://www.cnj.jus.br/programas-e-acoes/corregedoria-nacional-de-justica/`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-CNJ-JUSTICA-ABERTA-001`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` (`CNJJusticaAbertaConnector`) com extração de 4 tabelas Silver (`justica_aberta_serventias_cadastro`, `justica_aberta_produtividade_atribuicao`, `justica_aberta_arrecadacao_emolumentos`, `justica_aberta_regularidade_digitalizacao`), Gold Feature Store (`features_justica_aberta_cartorios_serventias_municipios_brasil.parquet`), scores normalizados (0-100) de eficiência/modernização cartorial e dinamismo notarial/imobiliário, registro dinâmico `register_connector` e `verify_live_connectivity()` / `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/cnj_justica_aberta/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_justica_aberta_serventias_cadastro`, `stg_justica_aberta_produtividade_atribuicao`, `stg_justica_aberta_arrecadacao_emolumentos`, `stg_justica_aberta_regularidade_digitalizacao`) com 100% de documentação e testes `not_null` / `unique`.
- [x] **Gate 5: dbt Core:** `fct_justica_aberta_serventias_arrecadacao_municipio.sql` e `fct_justica_aberta_produtividade_atribuicao_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_produtividade_serventias_extrajudiciais_brasil.sql` documentado em `dbt/models/marts/judicial/_marts_judicial_models.yml` com paridade exata frente à Feature Store Python.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_produtividade_serventias_extrajudiciais_brasil +fct_justica_aberta_produtividade_atribuicao_municipio`) com `PASS=165 TOTAL=165` e `lint_dbt_docs.py` aprovado com 100% de cobertura (299 modelos, 4.083 colunas, 162 fontes).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.judicial.justica_aberta_connector`, `get_justica_aberta_offices`, `list_justica_aberta_productivity`, `get_justica_aberta_emoluments`, `get_justica_aberta_digitalization`, `get_justica_aberta_features`, `list_top_justica_aberta_municipalities`.
  - CLI: `bdg justica-aberta offices`, `productivity`, `emoluments`, `digitalization`, `features`, `ranking`.
  - REST API: `/api/v1/judicial/justica-aberta/*` e atalho `/api/v1/justica-aberta/{id_municipio}`.
  - MCP: `bdg_get_justica_aberta_offices`, `bdg_get_justica_aberta_emoluments`, `bdg_get_justica_aberta_features`, `bdg_list_top_justica_aberta_municipalities` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline + 1 teste live fail-hard contra portais oficiais do CNJ Justiça Aberta.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (34/49 fontes homologadas — 69.4%, sexto conector da Wave 5), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes e squash-merge em `main`.
