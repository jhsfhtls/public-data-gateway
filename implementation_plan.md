# Handoff de implementação parcial: ibge_pib_municipal

- Branch/worktree: `feat/conn-ibge-pib-municipal`; preservar o worktree independente de `mte_rais`.
- O `ibge_sidra` já consulta a tabela 5938 e já oferece APIs de PIB; avaliar sobreposição antes de ampliar interfaces.
- Implementado: contrato ODCS, consulta SIDRA com município/ano explícitos e limite validado de uma observação, persistência Silver pontual, modelos dbt e testes locais/live.
- Validação feita: pytest offline do conector, pytest live de uma observação, Ruff nos arquivos Python alterados, `dbt parse` e `dbt ls` dos modelos adicionados.
- Não executados: ingestão em massa, baseline/parquet no diretório `data/`, construção/validação de Gold Feature Store, dbt run/test, paridade SDK/REST/CLI/MCP, sync do catálogo/progresso, PR e homologação.
- Manter a fonte em `CATALOGED` e o progresso não homologado até fechar os gates restantes com evidências; respeitar a instrução de não carregar bases completas.
