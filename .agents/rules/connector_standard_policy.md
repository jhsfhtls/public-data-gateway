# Diretiva Obrigatoria de Padronizacao Medallion & 11-Gates para Todas as APIs

> **Escopo:** Regra local obrigatoria do repositorio `public-data-gateway`. Nenhuma API ou fonte de dados pode ser considerada `HOMOLOGATED` sem cumprir 100% dos requisitos abaixo.

## 1. Principio da Paridade Total (Sem Excecoes)
Toda API integrada ao projeto — seja fonte governamental nacional ou API publica/global de Analytics, KYC/KYB, Mercado, Clima, Espacial ou ML — **DEVE** seguir exatamente o mesmo padrao arquitetural dos conectores das Waves 1 a 6:

1. **Registro no Catalogo Mestre:**
   - Constar formalmente no catalogo (`catalog/sources_registry.yaml` / `catalog/public_apis_registry.yaml`) com metadados completos (`auth_type`, `rate_limit_rpm`, `finops_tier`, `target_packs`, `status`) e renderizadores sincronizados (`--check` verde).
2. **Conector Dedicado & Contrato ODCS v3.1:**
   - Modulo proprio herdando de `BaseConnector` + `@register_connector`, com `contract.yaml` (ODCS v3.1, chaves canonicas STRING, regras semanticas e politica de quarentena).
3. **Armazenamento Medallion Isolado:**
   - Estrutura fisica isolada em `data/<connector_id>/{bronze,silver,gold,quarantine}/` com Parquet ZSTD, checksum SHA-256 e cache SQLite WAL.
   - *Regra para APIs parametrizadas de borda (ex.: CEP, CNPJ, Lat/Lon, Rota, Dominio):* devem materializar um lote canonico de referencia (capitais/UFs/entidades-semente) para alimentar as camadas Silver/Gold e o dbt, alem de persistir consultas sob demanda no cache/Bronze.
4. **Camada dbt Completa (Staging -> Intradominio -> Cross-Domain):**
   - **`staging` (`stg_<connector>_*`):** `source.yml` + views de staging com 100% das colunas documentadas e testadas.
   - **`intradominio` (`fct_*`, `dim_*`, `mart_*`):** fatos, dimensoes e data mart analitico do dominio proprio do conector.
   - **`cross_domain`:** integracao das features/scores na `feature_store_cross_domain_municipio` (ou mart cross-domain correspondente) e nos `mart_solution_packs_insights_brasil` / marts `S01..S09` quando aplicavel.
   - **Cobertura 100%:** `dbt build` com 0 erros e `scripts/lint_dbt_docs.py` com **100% Documentation Coverage**.
5. **Testes Offline + Live Fail-Hard + Gates (GATE_01..GATE_11):**
   - Suite offline deterministica (`tests/connectors/test_<connector>.py`) com `httpx.MockTransport` / fixtures.
   - Bateria `tests/live/test_<connector>_live.py` de 1 linha com fail-hard para endpoints gratuitos sem chave (ou pular graciosamente apenas quando exigir credencial paga/ausente).
6. **Paridade Multi-Interface Completa:**
   - Exposicao obrigatoria nas 4 interfaces: **SDK** (`BrazilData`), **FastAPI** (`/api/v1/...`), **CLI** (`bdg ...`) e **MCP** (`bdg_*` com `wrap_untrusted_gov_data`).
