# Implementation Plan — `inpe_terrabrasilis` (`ODCS-BR-INPE-TERRABRASILIS-001`)

> **Fase 08 — Ingestão Wave 6 (`11_agronegocio_meio_ambiente` / `ambiental`)**  
> **Conector #43 de 49 (`87.8%` do catálogo homologado) — Próximo (Wave 6): `ana_hidroweb`**  
> **Branch / Worktree:** `feat/conn-inpe-terrabrasilis` em `../worktrees/feat-conn-inpe-terrabrasilis`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-inpe-terrabrasilis` a partir de `main` (`bcff6ec`), junctions para `.venv` e `data`, e apontamento editável local em `.venv`.
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/ambiental/inpe_terrabrasilis/contract.yaml` (`ODCS-BR-INPE-TERRABRASILIS-001`).
  - `InpeTerrabrasilisConnector` (`connector.py`) registrado em `inpe_terrabrasilis`, materializando 4 tabelas Silver (`terrabrasilis_prodes_desmatamento_anual`, `terrabrasilis_deter_alertas_classe`, `terrabrasilis_cobertura_uso_terra_bioma`, `terrabrasilis_governanca_fiscalizacao_esg`) e 1 Gold Feature Store (`features_inpe_terrabrasilis_desmatamento`) em `data/inpe_terrabrasilis/{silver,gold}/`.
  - Probe fail-hard `probe_live_endpoints()` nos endpoints oficiais do INPE TerraBrasilis (`https://terrabrasilis.dpi.inpe.br/` e `https://terrabrasilis.dpi.inpe.br/geoserver/ows?service=WFS&request=GetCapabilities`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_terrabrasilis_prodes_desmatamento_anual`, `stg_terrabrasilis_deter_alertas_classe`, `stg_terrabrasilis_cobertura_uso_terra_bioma`, `stg_terrabrasilis_governanca_fiscalizacao_esg`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_terrabrasilis_desmatamento_governanca_municipio`, `fct_terrabrasilis_deter_alertas_classe_municipio`) e 1 mart (`mart_desmatamento_terrabrasilis_brasil`).
  - Execução de `dbt build` (`PASS=140 WARN=0 ERROR=0`), `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED` — 362 modelos / 5.345 colunas / 198 fontes) e validação de paridade estrita SQL–Python (`DBT_ROWS: 12`, `PARITY_DIFFS: []`).
- **Gate 8 — Paridade Multi-Interface:**
  - `EnvironmentFacade.terrabrasilis_connector` e `BrazilData` (`sdk.py`), grupo CLI `bdg terrabrasilis` (`prodes`, `deter`, `land-cover`, `governance`, `features`, `ranking` em `cli/main.py`), 7 rotas REST (`/api/v1/environment/terrabrasilis/*` + `/api/v1/terrabrasilis/{id_municipio}` em `api/app.py`) e 4 tools MCP `bdg_*_terrabrasilis*` com `wrap_untrusted_gov_data(result, source_id="inpe_terrabrasilis")` em `mcp/server.py`.
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_inpe_terrabrasilis.py` (8 testes offline) e `tests/connectors/test_inpe_terrabrasilis_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(environment): add inpe_terrabrasilis connector`, PR com checks verdes, squash-merge em `main`, remoção segura das junctions/worktree, restauração do `.pth` editável canônico, revalidação end-to-end em `public-data-gateway` e acompanhamento do CI em `main`.
