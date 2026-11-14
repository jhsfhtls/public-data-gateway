# Implementation Plan — `datasus_sinasc` (`ODCS-BR-DATASUS-SINASC-001`)

> **Fase 07 — Ingestão Wave 5 (`06_saude_publica` / `saude`)**  
> **Conector #35 de 49 (`71.4%` do catálogo homologado)**  
> **Branch / Worktree:** `feat/conn-datasus-sinasc` em `../worktrees/feat-conn-datasus-sinasc`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-datasus-sinasc` a partir de `main` (`64f3194`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` sincronizado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/saude/datasus_sinasc/contract.yaml` (`ODCS-BR-DATASUS-SINASC-001`).
  - `DataSUSSinascConnector` (`connector.py`) registrado em `datasus_sinasc`, materializando 4 tabelas Silver (`sinasc_nascidos_vivos_municipio`, `sinasc_assistencia_prenatal_parto`, `sinasc_perfil_materno_demografico`, `sinasc_condicoes_vitalidade_recem_nascido`) e 1 Gold Feature Store (`features_sinasc_natalidade_saude_perinatal_municipios_brasil.parquet`) em `data/datasus_sinasc/{silver,gold}/`.
  - Probe fail-hard `verify_live()` nos endpoints OpenDataSUS e API Dados Abertos Saúde.
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_sinasc_*`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_sinasc_nascidos_vivos_vitalidade_municipio`, `fct_sinasc_assistencia_prenatal_parto_municipio`) e 1 mart (`mart_natalidade_saude_perinatal_sinasc_brasil`).
  - Execução de `dbt build`, `dbt docs generate` e `scripts/lint_dbt_docs.py` (`APPROVED`), além de validação de paridade estrita SQL–Python (tolerância `1e-6`).
- **Gate 8 — Paridade Multi-Interface:**
  - `HealthFacade` e `BrazilData` (`sdk.py`), subcomando CLI `sinasc` (`cli/main.py`), endpoints REST `/api/v1/health-public/sinasc/*` e `/api/v1/sinasc/{id_municipio}` (`api/app.py`) e tools MCP (`bdg_*` com `wrap_untrusted_gov_data` em `mcp/server.py`).
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_datasus_sinasc.py` (8 testes offline) e `tests/connectors/test_datasus_sinasc_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(health): add datasus_sinasc connector`, PR com 4/4 checks verdes, squash-merge em `main`, remoção segura do worktree/junctions, reinstalação editável canônica e revalidação end-to-end em `public-data-gateway`.
