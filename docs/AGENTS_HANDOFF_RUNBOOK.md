# Brazilian Public Data Gateway — Agent Handoff Runbook

> **Single Source of Truth (SSOT) para Agentes de IA e Engenheiros de Dados**
> **Diretiva Imutável:** Seguir com rigor cirúrgico o **Protocolo dos 11 Gates** de homologação atômica, 1 conector por vez, sem atalhos ou batching monolítico.

---

## 1. Contexto do Repositório & Ecossistema

- **Repositório:** `C:\Users\jhose\GitOps\public-data-gateway` (Satélite C.O.R.E.)
- **Python Virtual Environment:** `.\.venv\Scripts\python.exe` (Nunca use Python/pip global)
- **Branch Canônica:** `main` (protegida por hooks de lint, branch naming, conventional commits e secrets scan)
- **Total de Conectores no Catálogo:** **49 fontes oficiais brasileiras** divididas em **9 Waves**
- **Status Atual:** **23 / 49 homologados (46.9%)**
- **Wave Ativa:** **Wave 4 (5 de 10 concluídos)**
  - Homologados na Wave 4: `ibge_sidra`, `ibge_censo2022`, `mte_novo_caged`, `ibge_pnad_continua`, `ibge_cnefe`
  - **Próximo Alvo Imediato:** **`mte_rais`** (Ministério do Trabalho e Emprego: Relação Anual de Informações Sociais)
  - Alvos subsequentes da Wave 4: `ibge_pib_municipal`, `ibge_malhas`, `anatel_mosaico`, `antt_malha_transporte`

---

## 2. Mapa das 9 Waves & Status de Conclusão

| Wave | Nome da Wave | Total Fontes | Homologadas | Status |
| :---: | :--- | :---: | :---: | :---: |
| **Wave 1** | Foundation & Core Financial Indicators | 4 | 4 | **100% CONCLUÍDA** (PRs #1-#4) |
| **Wave 2** | Public Procurement & Anti-Corruption Governance | 8 | 8 | **100% CONCLUÍDA** (PRs #5-#12) |
| **Wave 3** | Macro-Financial, Corporate Capital Markets & Treasury | 6 | 6 | **100% CONCLUÍDA** (PRs #13-#18) |
| **Wave 4** | Demographics, Labor Dynamics & Territorial Geocoding | 10 | 5 | **EM ANDAMENTO** (PRs #19-#25) |
| **Wave 5** | Public Health, SUS Epidemiology & Sanitary Vigilance | 5 | 0 | Planejada |
| **Wave 6** | Basic, Higher & Postgraduate Education | 4 | 0 | Planejada |
| **Wave 7** | Agriculture, Environmental Licensing & Land Cadastre | 5 | 0 | Planejada |
| **Wave 8** | Real Estate, Public Deeds & Notarial Registry | 3 | 0 | Planejada |
| **Wave 9** | Infrastructure Logistics & Metaportals | 4 | 0 | Planejada |

---

## 3. O Protocolo Sagrado dos 11 Gates (Checklist Atômico)

Para CADA novo conector a ser homologado, execute estritamente os 11 passos abaixo:

### Pré-requisito: Git Worktree
1. Atualize a branch canônica:
   ```powershell
   cd C:\Users\jhose\GitOps\public-data-gateway
   git checkout main
   git pull origin main
   ```
2. Crie um worktree isolado no padrão de branch naming (`feat/conn-<nome-conector>`):
   ```powershell
   git worktree add ../worktrees/feat-conn-<conector> -b feat/conn-<conector>
   ```
3. Crie os links de junção (junctions) para o `.venv` e subpastas de `data/`:
   ```powershell
   cmd /c mklink /J "C:\Users\jhose\GitOps\worktrees\feat-conn-<conector>\.venv" "C:\Users\jhose\GitOps\public-data-gateway\.venv"
   Get-ChildItem C:\Users\jhose\GitOps\public-data-gateway\data -Directory | Where-Object { $_.Name -ne 'reference' } | ForEach-Object { cmd /c mklink /J "C:\Users\jhose\GitOps\worktrees\feat-conn-<conector>\data\$($_.Name)" "$($_.FullName)" }
   ```
4. Instale o pacote em modo editável no worktree:
   ```powershell
   cd C:\Users\jhose\GitOps\worktrees\feat-conn-<conector>
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
  robocopy "C:\Users\jhose\GitOps\worktrees\feat-conn-<conector>\data\<conector>" "C:\Users\jhose\GitOps\public-data-gateway\data\<conector>" /E
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
   cd C:\Users\jhose\GitOps\public-data-gateway
   git checkout main
   git pull origin main
   git worktree remove ../worktrees/feat-conn-<conector> --force
   git branch -D feat/conn-<conector>
   ```

---

## 4. Próximo Conector da Fila: `mte_rais`

- **ID do Conector:** `mte_rais`
- **Nome Oficial:** MTE: Relação Anual de Informações Sociais (RAIS)
- **Agência:** Ministério do Trabalho e Emprego (MTE)
- **Framework Legal:** Lei nº 5.534/1968, Lei nº 5.878/1973, Decreto nº 76.900/1975, Portaria MTE nº 671/2021.
- **Segmento:** `04_trabalho_renda` (Pasta de código: `src/public_data_gateway/connectors/trabalho/mte_rais/`)
- **Tabelas Silver Previstas:**
  1. `rais_vinculos_ativos`: Estoque anual de vínculos formais ativos por município e setor.
  2. `rais_remuneracao_media`: Médias salariais em salários mínimos e reais nominais por escolaridade e gênero.
  3. `rais_estabelecimentos_porte`: Estoque de empresas por faixa de funcionários (micro, pequena, média, grande).
  4. `rais_massa_salarial_municipio`: Volume total anual da massa de rendimentos pagos por município.
  5. `rais_rotatividade_demografia`: Faixas etárias, tempo médio no emprego e grau de instrução dos trabalhadores formais.
- **Feature Store Gold:** `features_rais_mercado_formal_municipios_brasil.parquet`
- **Data Mart dbt:** `mart_panorama_emprego_estrutural_rais_brasil.sql`
