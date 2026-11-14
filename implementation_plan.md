# Plano de Implementação: anatel_mosaico (11-Gate Protocol)

## 1. Contexto do Conector
- **Fonte:** `anatel_mosaico` (ANATEL: Sistema Mosaico de Estações e Cobertura de Telecomunicações)
- **Agência:** Agência Nacional de Telecomunicações (ANATEL)
- **Legislação:** Lei nº 9.472/1997 (LGT), Lei nº 13.116/2015 (Lei Geral das Antenas), Resolução ANATEL nº 719/2020, Decreto nº 8.777/2016.
- **Segmento:** `05_geografico_logistica`
- **Protocolo:** `REST_JSON` / Portais Públicos ANATEL (`https://sistemas.anatel.gov.br/mosaico/` e `https://sistemas.anatel.gov.br/se/public/view/b/licenciamento.php`)
- **Chaves Canônicas:** `id_municipio`, `sigla_uf`

## 2. Status dos 11 Gates
- [x] **Gate 1: Contrato ODCS v3.1:** `contract.yaml` (`ODCS-BR-ANATEL-MOSAICO-001`, PKs: `id_municipio`, `sigla_uf`, 4 tabelas Silver e 1 Gold Feature Store).
- [x] **Gate 2: Ingestão Medallion:** `connector.py` com extração de 4 tabelas Silver (`mosaico_estacoes_smp`, `mosaico_banda_larga_scm`, `mosaico_espectro_radiofrequencia`, `mosaico_cobertura_setores`), Gold Feature Store (`features_telecom_conectividade_municipios_brasil.parquet`), scores normalizados (0-100) de conectividade digital e maturidade 5G, registro dinâmico `register_connector` e `verify_live()`.
- [x] **Gate 3: Baseline Parquet:** Materializado em `data/anatel_mosaico/{silver,gold}/`.
- [x] **Gate 4: dbt Staging:** 4 views staging (`stg_anatel_estacoes_smp`, `stg_anatel_banda_larga_scm`, `stg_anatel_espectro_radiofrequencia`, `stg_anatel_cobertura_setores`) com 100% de documentação e testes `not_null` em todas as colunas.
- [x] **Gate 5: dbt Core:** `fct_anatel_infraestrutura_telecom_municipio.sql` e `fct_anatel_alocacao_espectro_municipio.sql` documentados em `_core_models.yml`.
- [x] **Gate 6: dbt Data Mart:** `mart_conectividade_digital_telecom_brasil.sql` documentado em `_marts_models.yml`.
- [x] **Gate 7: dbt Build & Tests:** Build seletivo (`+mart_conectividade_digital_telecom_brasil +fct_anatel_alocacao_espectro_municipio`) com 182/182 PASS e 0 erros; `lint_dbt_docs.py` aprovado com 100% de cobertura (250 modelos, 3.125 colunas auditadas).
- [x] **Gate 8: Paridade Multi-Interface:**
  - SDK: `BrazilData.geo.anatel_connector`, `get_telecom_stations`, `get_broadband_profile`, `list_spectrum_allocations`, `get_telecom_features`, `list_top_connected_municipalities`.
  - CLI: `bdg anatel stations`, `bdg anatel broadband`, `bdg anatel spectrum`, `bdg anatel features`, `bdg anatel ranking`.
  - REST API: `/api/v1/geo/anatel/*` (`top-connected`, `features`, `broadband`, `spectrum`, `stations`) e atalho `/api/v1/anatel/{id_municipio}`.
  - MCP: `bdg_get_anatel_stations`, `bdg_get_anatel_broadband`, `bdg_get_anatel_telecom_features`, `bdg_list_top_connected_municipalities` com `<untrusted_gov_data>`.
- [x] **Gate 9: Testes Pytest:** 8 testes offline (contrato, scores, baseline Parquets, métodos/registro do conector, SDK, CLI, REST API e MCP) + 1 teste live fail-hard 100% PASS.
- [x] **Gate 10: Catálogo & Progresso:** `catalog/sources_registry.yaml` atualizado para `HOMOLOGATED` (27/49 fontes homologadas — 55.1%), `project_progress.json`, `CATALOG.md` e `JORNADA_IMPLEMENTACAO.md` sincronizados.
- [ ] **Gate 11: CI/CD & PR:** PR aberto via GitHub CLI, monitoramento de 100% dos checks verdes, squash-merge na `main`.
