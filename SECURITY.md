# Security Policy

## Reporting a Vulnerability

If you discover a security issue, data leak, or unsafe handling of external payloads in **Public Data Gateway**, please report it privately instead of opening a public issue:

- Use GitHub **Private Vulnerability Reporting / Security Advisories**:
  `https://github.com/jhsfhtls/public-data-gateway/security/advisories/new`

Please include:
1. A clear description of the vulnerability and potential impact.
2. Steps to reproduce (or proof-of-concept).
3. Any affected connector, interface (SDK, REST API, CLI, MCP), or dependency.

## Security Architecture & Guarantees

Public Data Gateway ingests untrusted external payloads and enforces the following built-in controls:

- **Ingestion Hardening:** Magic-byte validation on binary downloads and archive decompression limits (`<= 100:1` expansion ratio) to mitigate Zip Bombs and path traversal.
- **CSV Formula Injection Prevention (CWE-1236):** Neutralizes leading `=`, `+`, `-`, `@`, tab, and carriage-return characters during CSV exports.
- **Client Path Shield:** Enforces strict directory isolation so external enrichment outputs cannot be written inside the gateway's internal Medallion `data/` directory.
- **Untrusted Data Boundary (MCP / LLM):** External government text exposed via MCP tools is wrapped to prevent indirect prompt injection.
- **LGPD & Privacy Protection:** Preserves CPF masking (`***.123.456-**`) and avoids re-identification joins on sensitive microdata.
- **No Committed Secrets:** `.env`, credentials, local SQLite caches, and Parquet artifacts are excluded via `.gitignore`.
