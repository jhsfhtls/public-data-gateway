# Implementation Plan — `capitais_iptu_itbi` (`ODCS-BR-CAPITAIS-IPTU-ITBI-001`)

> **Fase 07 — Ingestão Wave 5 (`12_imobiliario_cartorial` / `imobiliario`)**  
> **Conector #41 de 49 (`83.7%` do catálogo homologado) — Próximo: `capes_sucupira`**  
> **Branch / Worktree:** `feat/conn-capitais-iptu-itbi` em `../worktrees/feat-conn-capitais-iptu-itbi`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-capitais-iptu-itbi` a partir de `main` (`769d387`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` copiado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/imobiliario/capitais_iptu_itbi/contract.yaml` (`ODCS-BR-CAPITAIS-IPTU-ITBI-001`).
  - `CapitaisIptuItbiConnector` (`connector.py`) registrado em `capitais_iptu_itbi`, materializando 4 tabelas Silver (`iptu_itbi_cadastro_imobiliario_municipal`, `itbi_transacoes_imobiliarias_padrao`, `iptu_itbi_dinamica_precos_bairros`, `iptu_itbi_governanca_fiscal_geoespacial`) e 1 Gold Feature Store (`features_iptu_itbi_mercado_imobiliario_capitais`) em `data/capitais_iptu_itbi/{silver,gold}/`.
  - Probe fail-hard `probe_live()` nos portais oficiais (`https://dados.gov.br/` e `https://www.prefeitura.sp.gov.br/cidade/secretarias/fazenda/servicos/itbi/`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_iptu_itbi_cadastro_imobiliario_municipal`, `stg_itbi_transacoes_imobiliarias_padrao`, `stg_iptu_itbi_dinamica_precos_bairros`, `stg_iptu_itbi_governanca_fiscal_geoespacial`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_iptu_itbi_cadastro_precos_governanca_municipio`, `fct_itbi_transacoes_segmento_municipio`) e 1 mart (`mart_mercado_imobiliario_iptu_itbi_capitais_brasil`).
  - Execução de `dbt build`, `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED` — 348 modelos / 5.085 colunas / 190 fontes) e validação de paridade estrita SQL–Python (23 colunas compartilhadas, zero divergências).
- **Gate 8 — Paridade Multi-Interface:**
  - `RealEstateFacade.iptu_itbi_connector` e `BrazilData` (`sdk.py`), grupo CLI `bdg iptu-itbi` (`cadastre`, `transactions`, `prices`, `governance`, `features`, `ranking` em `cli/main.py`), 6 rotas REST `/api/v1/real-estate/iptu-itbi/*` + `/api/v1/iptu-itbi/{id_municipio}` (`api/app.py`) e 4 tools MCP `bdg_*_iptu_itbi*` com `wrap_untrusted_gov_data(result, source_id="capitais_iptu_itbi")` em `mcp/server.py`.
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_capitais_iptu_itbi.py` (8 testes offline) e `tests/connectors/test_capitais_iptu_itbi_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(real-estate): add capitais_iptu_itbi connector`, PR com checks verdes, squash-merge em `main`, remoção segura das junctions/worktree, reinstalação editável canônica, revalidação end-to-end em `public-data-gateway` e acompanhamento do CI em `main`.
