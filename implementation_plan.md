# Implementation Plan — `inep_ideb` (`ODCS-BR-INEP-IDEB-001`)

> **Fase 07 — Ingestão Wave 5 (`07_educacao_capital_humano` / `educacao`)**  
> **Conector #37 de 49 (`75.5%` do catálogo homologado)**  
> **Branch / Worktree:** `feat/conn-inep-ideb` em `../worktrees/feat-conn-inep-ideb`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-inep-ideb` a partir de `main` (`406eff2`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` sincronizado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/educacao/inep_ideb/contract.yaml` (`ODCS-BR-INEP-IDEB-001`).
  - `INEPIdebConnector` (`connector.py`) registrado em `inep_ideb`, com resolução por código IBGE de 7 dígitos (incluindo aliases), CEP-8 de referência ou nome oficial do município, materializando 4 tabelas Silver (`ideb_resultados_municipios`, `ideb_etapas_redes_ensino`, `ideb_proficiencia_saeb_aprendizagem`, `ideb_metas_evolucao_bienal`) e 1 Gold Feature Store (`features_ideb_qualidade_educacional_municipios_brasil.parquet`) em `data/inep_ideb/{silver,gold}/`.
  - Probe fail-hard `verify_live()` nos portais oficiais do INEP IDEB (`gov.br/inep/.../ideb` e `gov.br/inep/.../ideb/resultados`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_ideb_*`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_ideb_desempenho_aprendizagem_municipio`, `fct_ideb_etapas_redes_municipio`) e 1 mart (`mart_qualidade_educacao_basica_ideb_brasil`).
  - Execução de `dbt build`, `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED`) e validação de paridade estrita SQL–Python (tolerância `1e-6`).
- **Gate 8 — Paridade Multi-Interface:**
  - `EducationFacade` e `BrazilData` (`sdk.py`), subcomando CLI `ideb` (`cli/main.py`), endpoints REST `/api/v1/education/ideb/*` e `/api/v1/ideb/{id_municipio}` (`api/app.py`) e tools MCP (`bdg_get_ideb_*`, `bdg_list_top_ideb_*` com `wrap_untrusted_gov_data` em `mcp/server.py`).
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_inep_ideb.py` (8 testes offline) e `tests/connectors/test_inep_ideb_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(education): add inep_ideb connector`, PR com 4/4 checks verdes, squash-merge em `main`, remoção segura do worktree/junctions, reinstalação editável canônica e revalidação end-to-end em `public-data-gateway`.
