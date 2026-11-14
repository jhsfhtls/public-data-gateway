# ADR-003: Hardening DevSecOps, Proteção LGPD e Isolamento de Diretórios (Client Path Shield)

- **Status:** Accepted
- **Date:** 2026-11-07
- **Governance & Standards:** `CWE-1236`, `Zip-Bomb Guard`, `Client Path Shield`, `LGPD (Art. 14)`, `Permissive Licensing`

## Context
O `public-data-gateway` processa arquivos compactados externos, enriquece datasets corporativos externos e expõe dados textuais governamentais a agentes LLM via MCP.

## Decision
1. **Ingestão & Exportação Defensiva:** Verificação de *Magic Bytes*, limite de razão de expansão `<= 100:1` contra *Zip Bombs* e neutralização de prefixos executáveis (`=`, `+`, `-`, `@` — `CWE-1236`) em exportações CSV.
2. **Isolamento MCP contra Prompt Injection (`OWASP LLM01`):** Textos livres oriundos de cadastros externos são delimitados em tags `<untrusted_gov_data>` nas respostas MCP.
3. **LGPD & Isolamento de Saída (Client Path Shield):** Preservação estrita de máscaras de CPF (`***.123.456-**`), bloqueio de cruzamentos reidentificadores em microdados educacionais/saúde infantil (Art. 14 LGPD) e proibição de persistir bases privadas externas dentro de `data/` (lakehouse interno).
4. **Licenciamento Limpo (OSI Approved):** Dependências restritas a licenças permissivas (`MIT`, `BSD`, `Apache-2.0`).

## Consequences
- Plataforma auditável para ambientes enterprise (SOC 2 / BACEN 85) e segura para consumo autônomo via MCP, CLI, REST ou SDK.
