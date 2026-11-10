# Homologação atômica: mte_rais

- Um conector, um worktree (`feat/conn-mte-rais`) e um PR.
- Engenheiro de ingestão: contrato ODCS, conector, Silver/Gold e prova de origem dos dados.
- Engenheiro de analytics: staging, core, mart e documentação dbt.
- Engenheiro de interfaces e QA: SDK, CLI, API, MCP e testes.
- Supervisor: integração, 11 gates, catálogo, CI, PR e merge após checks verdes.

Cada etapa depende da validação da anterior; o estado `HOMOLOGATED` será aplicado somente após os gates técnicos locais passarem.
