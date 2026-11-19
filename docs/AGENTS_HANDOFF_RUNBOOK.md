# Brazilian Public Data Gateway — Agent Handoff Runbook

> **Single Source of Truth (SSOT) para Agentes de IA e Engenheiros de Dados**
> **Diretiva Imutável:** Seguir com rigor cirúrgico o **Protocolo dos 11 Gates** de homologação atômica, 1 conector por vez, sem atalhos ou batching monolítico.

---

## 1. Contexto do Repositório & Ecossistema

- **Repositório:** `public-data-gateway` (Open Source Medallion Platform)
- **Python Virtual Environment:** `.\.venv\Scripts\python.exe` (Nunca use Python/pip global)
- **Branch Canônica:** `main` (protegida por hooks de lint, branch naming, conventional commits e secrets scan)
- **Total de Conectores no Catálogo Unificado (`catalog/sources_registry.yaml`):** **114 fontes homologadas** (**85 fontes oficiais brasileiras `GOV_BR` + 29 APIs Públicas & Globais `PUBLIC_GLOBAL`** em `CATALOG.md`)
- **Status Atual:** **Fases 00–11.5 concluídas (100%)** (114/114 conectores homologados nos 11 Gates + 698 modelos dbt + 8.964 testes dbt + Gold Cross-Domain + Lifecycle S01..S09 + Cloud Lakehouse Mirror)
- **Próxima Etapa:** **Fase 12** — Infraestrutura Produtiva, Orquestração 24/7 & Camada SaaS em Repositório Privado (Fronteira Open Core), conforme [ADR-004](adr/ADR-004-connector-medallion-parity-policy.md).

---

## 2. Mapa das Waves & Status de Conclusão (114/114 Fontes Homologadas)

| Wave / Fase | Escopo da Wave | Total Fontes | Homologadas | Status |
| :---: | :--- | :---: | :---: | :---: |
| **Waves 1–9 (Fases 02–08)** | Fundação, Macro/Finanças, Compras Públicas, Demografia/Trabalho, Saúde, Educação, Agro/ESG, Imobiliário & Infraestrutura (`GOV_BR`) | 49 | 49 | **100% CONCLUÍDA** |
| **Waves 10.5A–10.5D (Fases 10–10.5)** | APIs Públicas & Globais para Analytics, KYC/KYB, Clima, Geolocalização & ML — Blocos A a E (`PUBLIC_GLOBAL`) | 29 | 29 | **100% CONCLUÍDA** |
| **Wave 11.5A** | Risco Fiscal, Dívida Ativa, Improbidade, CAPAG & Crédito Bancário (`stn_siconfi`, `pgfn_divida_ativa`, `cnj_improbidade`, `stn_capag`, `bcb_estban`, `bcb_agencia`, `rfb_cno`) | 7 | 7 | **100% CONCLUÍDA** |
| **Wave 11.5B** | Agro, ESG, Terras, Energia & Saneamento (`ibama_fiscalizacao`, `bcb_sicor`, `ibge_agro_municipal`, `rfb_cafir`, `inpe_queimadas_seeg`, `aneel_geracao_distribuida`, `anp_combustiveis`, `mdr_snis`) | 8 | 8 | **100% CONCLUÍDA** |
| **Wave 11.5C** | Mobilidade, Segurança Pública, Desastres, Rodovias, SIN/ONS & Previdência (`senatran_frota`, `sinesp_seguranca`, `sedec_desastres`, `prf_acidentes`, `mme_ons_energia`, `mps_inss_beneficios`) | 6 | 6 | **100% CONCLUÍDA** |
| **Wave 11.5D** | Transparência Federal, Servidores SIAPE, Eleições TSE, BNDES, CVM Extra & ANS (`cgu_transparencia_gastos`, `cgu_servidores_federais`, `tse_eleicoes`, `bndes_operacoes`, `cvm_mercado_extra`, `ans_saude_suplementar`) | 6 | 6 | **100% CONCLUÍDA** |
| **Wave 11.5E (+ Extensão)** | Diferenciais & Complementares (`querido_diario_okbr`, `anm_mineracao`, `susep_seguros`, `inpi_propriedade_intelectual`, `datasus_extra`, `inep_mec_extra`, `ibge_institucional`, `rfb_arrecadacao`, `cgu_beneficios_cidadao`) | 9 | 9 | **100% CONCLUÍDA** |
| **TOTAL UNIFICADO** | **Catálogo Mestre Único (`catalog/sources_registry.yaml`: 85 `GOV_BR` + 29 `PUBLIC_GLOBAL`)** | **114** | **114** | **100% CONCLUÍDA** |

---

## 3. O Protocolo Sagrado dos 11 Gates (Checklist Atômico)

Para CADA novo conector a ser homologado, execute estritamente os 11 passos abaixo:

### Pré-requisito: Git Worktree
1. Atualize a branch canônica:
   ```powershell
   cd <repo-root>
   git checkout main
   git pull origin main
   ```
2. Crie um worktree isolado no padrão de branch naming (`feat/conn-<nome-conector>`):
   ```powershell
   git worktree add ../worktrees/feat-conn-<conector> -b feat/conn-<conector>
   ```
3. Crie os links de junção (junctions) para o `.venv` e subpastas de `data/`:
   ```powershell
   cmd /c mklink /J "..\worktrees\feat-conn-<conector>\.venv" "<repo-root>\.venv"
   Get-ChildItem <repo-root>\data -Directory | Where-Object { $_.Name -ne 'reference' } | ForEach-Object { cmd /c mklink /J "..\worktrees\feat-conn-<conector>\data\$($_.Name)" "$($_.FullName)" }
   ```
4. Instale o pacote em modo editável no worktree:
   ```powershell
   cd ..\worktrees\feat-conn-<conector>
   .\.venv\Scripts\pip.exe install -e . --no-deps
   ```

---

### Gate 1: Contrato ODCS v3.1 Declarativo
- **Arquivo:** `src/public_data_gateway/connectors/<dominio>/<conector>/contract.yaml`
- **Requisitos:**
  - `schema_version: "3.1.0"`
  - `contract_id: "ODCS-BR-<AGENCY>-<ID>-001"`
  - `source_id: "<conector>"`
  - `legal_framework`: Leis, decretos e resoluções federais que autorizam o acesso público.
  - `data_governance`: Grain, primary keys, SLA de freshness (em horas), LGPD tier (`PUBLIC_NON_PERSONAL` ou `ANONYMIZED_MICRODATA`).
  - `schema.tables`: Mapeamento de todas as tabelas Silver com tipos canônicos, descrições detalhadas e restrições.

### Gate 2: Conector Python & Ingestão Medallion
- **Arquivo:** `src/public_data_gateway/connectors/<dominio>/<conector>/connector.py`
- **Requisitos:**
  - Classe nomeada em CamelCase (ex: `MTERaisConnector`).
  - Implementar métodos abstratos do `BaseConnector`: `extract(self) -> pa.Table` e `test_connection(self) -> bool`.
  - Implementar `verify_live(self) -> dict[str, Any]` que faz sondagens reais com timeout contra os endpoints oficiais da agência governamental (FTP, API REST, OpenData), retornando `status: "PASS"` se saudável.
  - Gerar 4 a 5 tabelas Silver em PyArrow (`pa.Table`).
  - Gerar 1 Feature Store Gold (`features_<conector>_<escopo>_brasil.parquet`) contendo scores contínuos de 0 a 100, classificações analíticas e features para machine learning.
  - Implementar métodos de consulta rápida (`get_...`, `list_...`) lendo os arquivos Parquet de `self.settings.data_dir`.
  - Usar estritamente `self.settings.data_dir` para escrita/leitura (NUNCA `Path("data")` relativo).
  - Usar PyArrow puro (`.to_pylist()`), evitando `.to_pandas()`.

### Gate 3: Geração de Baseline Silver/Gold
- Executar extração local e sincronizar para o repositório canônico:
  ```powershell
  .\.venv\Scripts\python.exe -c "from public_data_gateway.connectors.<dominio>.<conector>.connector import <Classe>; conn = <Classe>(); conn.extract_all()"
  robocopy "..\worktrees\feat-conn-<conector>\data\<conector>" "<repo-root>\data\<conector>" /E
  ```

### Gate 4: dbt Staging Models & Documentação
- **Diretório:** `src/public_data_gateway/connectors/<dominio>/<conector>/dbt/`
  - `source.yml`: Declaração do source `<conector>_silver` apontando para `data/<conector>/silver/{name}.parquet`.
  - `stg_<conector>_*.sql`: Modelos SQL staging (materializados como `view`).
  - `_models.yml`: **100% de cobertura de documentação** em todas as colunas de todas as staging views.
- **Registro no dbt:** Adicionar `- "../src/public_data_gateway/connectors/<dominio>/<conector>/dbt"` em `dbt/dbt_project.yml`.

### Gate 5: dbt Core Facts & Dimensions
- **Diretório:** `dbt/models/core/`
  - Criar 2 modelos analíticos (tabelas `fct_...` ou `dim_...`).
  - Documentar e testar (`not_null`, etc.) 100% das novas colunas em `dbt/models/core/_core_models.yml`.

### Gate 6: dbt Data Marts
- **Diretório:** `dbt/models/marts/<dominio>/`
  - Criar 1 mart analítico (tabela `mart_...`) cruzando dados setoriais e geográficos.
  - Documentar e testar 100% das novas colunas no arquivo `_marts_models.yml` correspondente.

### Gate 7: dbt Build, Tests & Linter de Governança
- Executar do diretório raiz do worktree:
  ```powershell
  .\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt
  .\.venv\Scripts\dbt.exe docs generate --project-dir dbt --profiles-dir dbt
  .\.venv\Scripts\python.exe scripts/lint_dbt_docs.py
  .\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt
  ```
- **Critério de Saída:** `lint_dbt_docs.py` deve retornar `Result: APPROVED - 100% Documentation Coverage`, e `dbt test` deve ser **100% PASS (0 failures, 0 warnings)**.

### Gate 8: Paridade Multi-Interface
Expor o novo conector nas 4 interfaces da plataforma:
1. **Python SDK:** `src/public_data_gateway/sdk.py`
   - Adicionar métodos de domínio na Facade correspondente (ex: `LaborFacade`, `GeoFacade`) e atalhos de alto nível em `BrazilData`.
2. **CLI (Command Line):** `src/public_data_gateway/cli/main.py`
   - Registrar parser e subparser com flags `--json`.
   - Adicionar dispatcher executável com visualização amigável de métricas no terminal.
3. **REST API (FastAPI):** `src/public_data_gateway/api/app.py`
   - Criar rotas `/api/v1/<dominio>/<conector>/...` e rota curta `/api/v1/<conector>/{key}`.
4. **Servidor MCP:** `src/public_data_gateway/mcp/server.py`
   - Declarar schemas de ferramentas em `_list_tools` (`bdg_get_...`, `bdg_list_...`).
   - Implementar handler em `_call_tool`.
   - **Obrigatório:** Conter dados governamentais com `wrap_untrusted_gov_data(..., source_id="<conector>")`.

### Gate 9: Bateria de Testes Pytest (Unitários & Live Probe)
- **Diretório:** `tests/connectors/`
  - `test_<conector>.py`: 8 ou mais testes unitários cobrindo ODCS contract, extração de todas as tabelas, gold feature store, métodos de consulta, rankings e paridade multi-interface (SDK, API e MCP).
  - `test_<conector>_live.py`: Teste de rede marcado com `@pytest.mark.live` validando HTTP 200/status PASS do `verify_live()`.
- Executar os testes:
  ```powershell
  .\.venv\Scripts\pytest.exe tests/connectors/test_<conector>.py -v
  .\.venv\Scripts\pytest.exe tests/connectors/test_<conector>_live.py -v
  ```
- Validar regressão offline geral:
  ```powershell
  .\.venv\Scripts\pytest.exe -m "not live" --ignore=... (todos os testes live ignorados)
  ```

### Gate 10: Sincronização do Catálogo & Governança
1. `catalog/sources_registry.yaml`: Alterar `status: CATALOGED` para `status: HOMOLOGATED` do conector.
2. `state/project_progress.json`: Atualizar `active_connector` para o próximo da fila, atualizar `next_connector`, registrar novo milestone no array `history`.
3. Regenerar documentação:
   ```powershell
   .\.venv\Scripts\python.exe scripts/render_catalog.py
   .\.venv\Scripts\python.exe scripts/render_progress.py
   ```

### Gate 11: CI/CD, Pull Request & Automated Merge
1. Verificar linter de código:
   ```powershell
   .\.venv\Scripts\ruff.exe check src tests
   ```
2. Commit seguindo Conventional Commits (título estritamente <= 72 caracteres, lowercase, sem ponto final):
   ```powershell
   git add -A
   git commit -m "feat(<dominio>): add <conector> connector"
   git push -u origin feat/conn-<conector>
   ```
3. Abrir o Pull Request via GitHub CLI:
   ```powershell
   gh pr create --title "feat(<dominio>): add <conector> connector" --body-file pr_body.md
   ```
4. Monitorar os 4 checks do GitHub Actions:
   ```powershell
   gh pr checks <PR_NUM>
   ```
5. Após aprovação verde de 100% dos checks, fazer squash-merge e deletar o branch remoto:
   ```powershell
   gh pr merge <PR_NUM> --squash --delete-branch
   ```
6. Retornar ao repositório canônico, atualizar `main` e remover o worktree:
   ```powershell
   cd <repo-root>
   git checkout main
   git pull origin main
   git worktree remove ../worktrees/feat-conn-<conector> --force
   git branch -D feat/conn-<conector>
   ```

---

## 4. Busca Unificada no Catálogo (`114 Fontes`) & Fronteira Open-Core para a Fase 12

- **Catálogo Mestre Único (`catalog/sources_registry.yaml`):**
  - Todas as **114 fontes homologadas** (`85 GOV_BR` + `29 PUBLIC_GLOBAL`) residem em um único arquivo YAML e podem ser consultadas com filtros `source_origin` e busca textual insensível a acentos (`query`/`q`) nas 4 interfaces:
    - **SDK:** `bdg.list_sources(segment_id=..., source_origin="GOV_BR", query="tributos")`
    - **FastAPI:** `GET /api/v1/catalog?origin=GOV_BR&q=bolsa`
    - **CLI:** `pdg catalog --origin GOV_BR --query "bolsa familia"`
    - **MCP:** tool `bdg_list_sources` com parâmetros `segment_id`, `origin` e `query`.
- **Fronteira Open-Core (Transição para a Fase 12):**
  - O repositório público `public-data-gateway` encerra a **Fase 11.5** com **100% das 114 fontes homologadas nos 11 Gates**, **698 modelos dbt**, **9.390 colunas documentadas**, **8.964 testes dbt** e **815 testes pytest** (`626` offline + `189` live).
  - A **Fase 12** (IaC Terraform/Pulumi para buckets S3/R2 produtivos, orquestração contínua 24/7 guiada por `freshness_sla_hours` / `cron_schedule_expression`, catálogo serverless MotherDuck/BigQuery produtivo e camada SaaS/Billing multi-tenant) deve ser executada em **repositório privado**, consumindo `public-data-gateway` como dependência limpa sem acoplar segredos ou infraestrutura proprietária no repositório público.
