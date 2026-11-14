# Implementation Plan — `anvisa_cmed` (`ODCS-BR-ANVISA-CMED-001`)

> **Fase 07 — Ingestão Wave 5 (`06_saude_publica` / `saude`)**  
> **Conector #36 de 49 (`73.5%` do catálogo homologado)**  
> **Branch / Worktree:** `feat/conn-anvisa-cmed` em `../worktrees/feat-conn-anvisa-cmed`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-anvisa-cmed` a partir de `main` (`1f648f6`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` sincronizado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/saude/anvisa_cmed/contract.yaml` (`ODCS-BR-ANVISA-CMED-001`).
  - `ANVISACmedConnector` (`connector.py`) registrado em `anvisa_cmed`, com resolução por código IBGE de 7 dígitos, CNPJ-14 de polo farmacêutico ou nome municipal, materializando 4 tabelas Silver (`cmed_apresentacoes_registradas_municipio`, `cmed_precos_regulados_classe_terapeutica`, `cmed_compras_publicas_cap_pmvg`, `cmed_regime_tributario_icms_piscofins`) e 1 Gold Feature Store (`features_cmed_precos_medicamentos_municipios_brasil.parquet`) em `data/anvisa_cmed/{silver,gold}/`.
  - Probe fail-hard `verify_live()` nos portais oficiais da ANVISA (`gov.br/anvisa/.../cmed/precos` e `dados.anvisa.gov.br/dados/`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_cmed_*`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_cmed_apresentacoes_precos_regulados_municipio`, `fct_cmed_precos_classe_terapeutica_municipio`) e 1 mart (`mart_precos_medicamentos_regulados_cmed_brasil`).
  - Execução de `dbt build`, `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED`) e validação de paridade estrita SQL–Python (tolerância `1e-6`).
- **Gate 8 — Paridade Multi-Interface:**
  - `HealthFacade` e `BrazilData` (`sdk.py`), subcomando CLI `cmed` (`cli/main.py`), endpoints REST `/api/v1/health-public/cmed/*` e `/api/v1/cmed/{id_municipio}` (`api/app.py`) e tools MCP (`bdg_get_cmed_*`, `bdg_list_top_cmed_*` com `wrap_untrusted_gov_data` em `mcp/server.py`).
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_anvisa_cmed.py` (8 testes offline) e `tests/connectors/test_anvisa_cmed_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(health): add anvisa_cmed connector`, PR com 4/4 checks verdes, squash-merge em `main`, remoção segura do worktree/junctions, reinstalação editável canônica e revalidação end-to-end em `public-data-gateway`.
