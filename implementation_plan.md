# Implementation Plan — `capes_sucupira` (`ODCS-BR-CAPES-SUCUPIRA-001`)

> **Fase 07 — Ingestão Wave 5 (`07_educacao_capital_humano` / `educacao`)**  
> **Conector #42 de 49 (`85.7%` do catálogo homologado) — Wave 5 100% concluída! Próximo (Wave 6): `inpe_terrabrasilis`**  
> **Branch / Worktree:** `feat/conn-capes-sucupira` em `../worktrees/feat-conn-capes-sucupira`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-capes-sucupira` a partir de `main` (`77c4b55`), junctions para `.venv` e `data`, e apontamento editável local em `.venv`.
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/educacao/capes_sucupira/contract.yaml` (`ODCS-BR-CAPES-SUCUPIRA-001`).
  - `CapesSucupiraConnector` (`connector.py`) registrado em `capes_sucupira`, materializando 4 tabelas Silver (`sucupira_programas_avaliacao`, `sucupira_discentes_titulados_area`, `sucupira_docentes_pesquisa`, `sucupira_producao_cientifica_teses`) e 1 Gold Feature Store (`features_capes_sucupira_pos_graduacao`) em `data/capes_sucupira/{silver,gold}/`.
  - Probe fail-hard `probe_live_endpoints()` nos portais oficiais da CAPES (`https://dadosabertos.capes.gov.br/api/3/action/package_list` e `https://www.gov.br/capes/pt-br/acesso-a-informacao/dados-abertos`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_sucupira_programas_avaliacao`, `stg_sucupira_discentes_titulados_area`, `stg_sucupira_docentes_pesquisa`, `stg_sucupira_producao_cientifica_teses`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_sucupira_pos_graduacao_pesquisa_municipio`, `fct_sucupira_discentes_area_municipio`) e 1 mart (`mart_pos_graduacao_capes_sucupira_brasil`).
  - Execução de `dbt build` (`PASS=144 WARN=0 ERROR=0`), `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED` — 355 modelos / 5.217 colunas / 194 fontes) e validação de paridade estrita SQL–Python (`DBT_ROWS: 12`, `PARITY_DIFFS: []`).
- **Gate 8 — Paridade Multi-Interface:**
  - `EducationFacade.sucupira_connector` e `BrazilData` (`sdk.py`), grupo CLI `bdg sucupira` (`programs`, `students`, `faculty`, `production`, `features`, `ranking` em `cli/main.py`), 7 rotas REST (`/api/v1/education/sucupira/*` + `/api/v1/sucupira/{id_municipio}` em `api/app.py`) e 4 tools MCP `bdg_*_sucupira*` com `wrap_untrusted_gov_data(result, source_id="capes_sucupira")` em `mcp/server.py`.
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_capes_sucupira.py` (8 testes offline) e `tests/connectors/test_capes_sucupira_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(education): add capes_sucupira connector`, PR com checks verdes, squash-merge em `main`, remoção segura das junctions/worktree, restauração do `.pth` editável canônico, revalidação end-to-end em `public-data-gateway` e acompanhamento do CI em `main`.
