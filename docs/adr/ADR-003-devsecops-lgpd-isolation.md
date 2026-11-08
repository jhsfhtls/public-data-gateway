# ADR-003: Hardening DevSecOps, Proteção LGPD e Isolamento Cross-Repo C.O.R.E.

- **Status:** Accepted
- **Date:** 2026-11-07
- **Governing POPs:** `POP_T_175`, `POP_T_154`, `POP_T_021`, `POP_B_049`, `POP_B_051`

## Context
O `brazil-data-gateway` processa arquivos compactados externos, enriquece planilhas corporativas de clientes do repositório `operations` e expõe dados textuais governamentais a agentes LLM via MCP.

## Decision
1. **Ingestão & Exportação Defensiva (`POP_T_175`):** Verificação de *Magic Bytes*, limite de razão de expansão `<= 100:1` contra *Zip Bombs* e neutralização de prefixos executáveis (`=`, `+`, `-`, `@` — `CWE-1236`) em exportações CSV.
2. **Isolamento MCP contra Prompt Injection (`OWASP LLM01`):** Textos livres oriundos de cadastros externos são delimitados em tags `<untrusted_gov_data>` nas respostas MCP.
3. **LGPD & Isolamento de Clientes (`POP_B_051` / `POP_T_154`):** Preservação estrita de máscaras de CPF (`***.123.456-**`), bloqueio de cruzamentos reidentificadores em microdados educacionais/saúde infantil (Art. 14 LGPD) e proibição de persistir bases privadas de clientes dentro de `brazil-data-gateway/data/`.
4. **Licenciamento Limpo (`POP_B_049` / `POP_B_123`):** Dependências restritas a licenças permissivas (`MIT`, `BSD`, `Apache-2.0`).

## Consequences
- Plataforma auditável para clientes enterprise (SOC 2 / BACEN 85) e segura para uso autônomo pelos agentes do C.O.R.E.
