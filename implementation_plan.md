# Implementation Plan — `opendatasus_vigilancia` (`ODCS-BR-OPENDATASUS-VIGILANCIA-001`)

> **Fase 07 — Ingestão Wave 5 (`06_saude_publica` / `saude`)**  
> **Conector #39 de 49 (`79.6%` do catálogo homologado) — Próximo: `inep_enem`**  
> **Branch / Worktree:** `feat/conn-opendatasus-vigilancia` em `../worktrees/feat-conn-opendatasus-vigilancia`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-opendatasus-vigilancia` a partir de `main` (`0636340`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` copiado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/saude/opendatasus_vigilancia/contract.yaml` (`ODCS-BR-OPENDATASUS-VIGILANCIA-001`).
  - `OpenDataSUSVigilanciaConnector` (`connector.py`) registrado em `opendatasus_vigilancia`, materializando 4 tabelas Silver (`vigilancia_sindromes_respiratorias_srag_municipio`, `vigilancia_agravos_notificacao_sinan`, `vigilancia_cobertura_vacinal_sipni_municipio`, `vigilancia_capacidade_resposta_cievs_lacen`) e 1 Gold Feature Store (`features_opendatasus_vigilancia_epidemiologica_municipios_brasil`) em `data/opendatasus_vigilancia/{silver,gold}/`.
  - Probe fail-hard `probe_live()` nos portais oficiais (`https://opendatasus.saude.gov.br/` e `https://www.gov.br/saude/pt-br/composicao/svsa`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_vigilancia_sindromes_respiratorias_srag_municipio`, `stg_vigilancia_agravos_notificacao_sinan`, `stg_vigilancia_cobertura_vacinal_sipni_municipio`, `stg_vigilancia_capacidade_resposta_cievs_lacen`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_vigilancia_srag_imunizacao_cievs_municipio`, `fct_vigilancia_agravos_sinan_municipio`) e 1 mart (`mart_vigilancia_epidemiologica_imunizacao_opendatasus_brasil`).
  - Execução de `dbt build`, `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED` — 334 modelos / 4.803 colunas / 182 fontes) e validação de paridade estrita SQL–Python (23 colunas compartilhadas, zero divergências).
- **Gate 8 — Paridade Multi-Interface:**
  - `HealthFacade.vigilancia_connector` e `BrazilData` (`sdk.py`), grupo CLI `bdg vigilancia` (`srag`, `sinan`, `vacinacao`, `cievs`, `features`, `ranking` em `cli/main.py`), 6 rotas REST `/api/v1/health/vigilancia/*` + `/api/v1/vigilancia/{id_municipio}` (`api/app.py`) e 4 tools MCP `bdg_*_vigilancia*` com `wrap_untrusted_gov_data(result, source_id="opendatasus_vigilancia")` em `mcp/server.py`.
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_opendatasus_vigilancia.py` (8 testes offline) e `tests/connectors/test_opendatasus_vigilancia_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(health): add opendatasus_vigilancia connector`, PR com checks verdes, squash-merge em `main`, remoção segura das junctions/worktree, reinstalação editável canônica, revalidação end-to-end em `public-data-gateway` e acompanhamento do CI em `main`.
