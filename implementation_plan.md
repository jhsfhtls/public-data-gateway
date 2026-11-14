# Implementation Plan — `inep_enem` (`ODCS-BR-INEP-ENEM-001`)

> **Fase 07 — Ingestão Wave 5 (`07_educacao_capital_humano` / `educacao`)**  
> **Conector #40 de 49 (`81.6%` do catálogo homologado) — Próximo: `capitais_iptu_itbi`**  
> **Branch / Worktree:** `feat/conn-inep-enem` em `../worktrees/feat-conn-inep-enem`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-inep-enem` a partir de `main` (`7f91da6`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` copiado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/educacao/inep_enem/contract.yaml` (`ODCS-BR-INEP-ENEM-001`).
  - `INEPEnemConnector` (`connector.py`) registrado em `inep_enem`, materializando 4 tabelas Silver (`enem_desempenho_municipal_areas`, `enem_desempenho_rede_ensino`, `enem_redacao_competencias_municipio`, `enem_equidade_socioeconomica_acesso`) e 1 Gold Feature Store (`features_enem_desempenho_equidade_municipios_brasil`) em `data/inep_enem/{silver,gold}/`.
  - Probe fail-hard `probe_live()` nos portais oficiais (`https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem` e `https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/enem`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_enem_desempenho_municipal_areas`, `stg_enem_desempenho_rede_ensino`, `stg_enem_redacao_competencias_municipio`, `stg_enem_equidade_socioeconomica_acesso`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_enem_desempenho_redacao_equidade_municipio`, `fct_enem_desempenho_rede_ensino_municipio`) e 1 mart (`mart_desempenho_equidade_enem_municipios_brasil`).
  - Execução de `dbt build`, `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED` — 341 modelos / 4.943 colunas / 186 fontes) e validação de paridade estrita SQL–Python (23 colunas compartilhadas, zero divergências).
- **Gate 8 — Paridade Multi-Interface:**
  - `EducationFacade.enem_connector` e `BrazilData` (`sdk.py`), grupo CLI `bdg enem` (`scores`, `networks`, `essay`, `equity`, `features`, `ranking` em `cli/main.py`), 6 rotas REST `/api/v1/education/enem/*` + `/api/v1/enem/{id_municipio}` (`api/app.py`) e 4 tools MCP `bdg_*_enem*` com `wrap_untrusted_gov_data(result, source_id="inep_enem")` em `mcp/server.py`.
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_inep_enem.py` (8 testes offline) e `tests/connectors/test_inep_enem_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(education): add inep_enem connector`, PR com checks verdes, squash-merge em `main`, remoção segura das junctions/worktree, reinstalação editável canônica, revalidação end-to-end em `public-data-gateway` e acompanhamento do CI em `main`.
