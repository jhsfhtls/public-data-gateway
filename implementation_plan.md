# Implementation Plan — `onr_registro_imoveis` (`ODCS-BR-ONR-REGISTRO-IMOVEIS-001`)

> **Fase 07 — Ingestão Wave 5 (`12_imobiliario_cartorial` / `imobiliario`)**  
> **Conector #38 de 49 (`77.6%` do catálogo homologado) — Próximo: `opendatasus_vigilancia`**  
> **Branch / Worktree:** `feat/conn-onr-registro-imoveis` em `../worktrees/feat-conn-onr-registro-imoveis`

## Arquitetura Medallion & Gates de Execução (11 Gates)

- **Gate 0 — Worktree & Junctions:** Worktree isolado `feat/conn-onr-registro-imoveis` a partir de `main` (`a2e3104`), junctions para `.venv` e `data/*` (excluindo `data/reference`), `brasil.duckdb` copiado e instalação editável local (`pip install --no-deps -e .`).
- **Gates 1–3 — Contrato ODCS, Conector & Materialização:**
  - `src/public_data_gateway/connectors/imobiliario/onr_registro_imoveis/contract.yaml` (`ODCS-BR-ONR-REGISTRO-IMOVEIS-001`).
  - `ONRRegistroImoveisConnector` (`connector.py`) registrado em `onr_registro_imoveis`, materializando 4 tabelas Silver (`onr_matriculas_acervo_municipio`, `onr_transacoes_atos_natureza`, `onr_garantias_credito_imobiliario`, `onr_interoperabilidade_serp_eficiencia`) e 1 Gold Feature Store (`features_onr_registro_imoveis_municipios_brasil`) em `data/onr_registro_imoveis/{silver,gold}/`.
  - Probe fail-hard `probe_live()` nos portais oficiais da ONR (`https://onr.org.br/` e `https://mapa.onr.org.br/`).
- **Gates 4–7 — Camadas dbt, Documentação 100% & Paridade SQL–Python:**
  - 4 staging views (`stg_onr_matriculas_acervo_municipio`, `stg_onr_transacoes_atos_natureza`, `stg_onr_garantias_credito_imobiliario`, `stg_onr_interoperabilidade_serp_eficiencia`) registradas em `dbt/dbt_project.yml`.
  - 2 fatos core (`fct_onr_acervo_garantias_serp_municipio`, `fct_onr_transacoes_natureza_municipio`) e 1 mart (`mart_registro_eletronico_imoveis_onr_brasil`).
  - Execução de `dbt build`, `dbt docs generate`, `scripts/lint_dbt_docs.py` (`APPROVED` — 327 modelos / 4.659 colunas) e validação de paridade estrita SQL–Python (22 colunas compartilhadas, zero divergências).
- **Gate 8 — Paridade Multi-Interface:**
  - `RealEstateFacade`, `JudicialFacade.onr_connector` e `BrazilData` (`sdk.py`), grupo CLI `bdg onr` (`registry`, `transactions`, `credit`, `serp`, `features`, `ranking` em `cli/main.py`), 6 rotas REST `/api/v1/real-estate/onr/*` + `/api/v1/onr/{id_municipio}` (`api/app.py`) e 4 tools MCP `bdg_*_onr*` com `wrap_untrusted_gov_data(result, source_id="onr_registro_imoveis")` em `mcp/server.py`.
- **Gate 9 — Suíte de Testes:**
  - `tests/connectors/test_onr_registro_imoveis.py` (8 testes offline) e `tests/connectors/test_onr_registro_imoveis_live.py` (1 teste `@pytest.mark.live`).
- **Gate 10 — Governança, Renderers & Verificação Completa:**
  - Atualização de `catalog/sources_registry.yaml` (`HOMOLOGATED`), `state/project_progress.json`, `CATALOG.md`, `JORNADA_IMPLEMENTACAO.md`, normalização UTF-8 LF sem BOM, `ruff check .`, `--check` renderers, `lint_dbt_docs.py` e `pytest -m "not live"`.
- **Gate 11 — Commit, PR, Merge & Revalidação Canônica:**
  - Commit `feat(real-estate): add onr_registro_imoveis connector`, PR com checks verdes, squash-merge em `main`, remoção segura das junctions/worktree, reinstalação editável canônica, revalidação end-to-end em `public-data-gateway` e acompanhamento do CI em `main`.
