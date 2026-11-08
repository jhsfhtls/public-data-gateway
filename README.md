<div align="center">
  <h2>Public Data Gateway — Plataforma Unificada de Dados Públicos, Analytics/ML & Enriquecimento</h2>
  <p><i>Repositório Satélite Oficial do Ecossistema C.O.R.E.</i></p>
  <p><b>Versão:</b> 0.1.0 &nbsp;|&nbsp; <b>Governança:</b> POP_T_176 • POP_T_177 • POP_T_175 • POP_T_070 • POP_T_074</p>
  <hr/>
</div>
<br/>

# `public-data-gateway`

Barramento modular corporativo que unifica **bases de dados públicas oficiais, APIs governamentais do Brasil e fontes públicas globais** estruturadas nos macrosegmentos estratégicos, curadas em arquitetura **Medallion (`Bronze -> Silver -> Gold / ML Feature Views`)** e expostas simultaneamente via **Python SDK (`PublicData` / `BrazilData`)**, **FastAPI (`/api/v1`)**, **CLI (`pdg` / `bdg`)** e **Servidor MCP (`pdg-mcp` / `bdg-mcp`)** para todo o ecossistema **C.O.R.E.** (`core`, `operations`, `career`, `studies`).

---

## Painéis Vivos de Governança

- **[JORNADA_IMPLEMENTACAO.md](JORNADA_IMPLEMENTACAO.md)**: Controle em tempo real do que estamos construindo, conector ativo (`IN_PROGRESS`), checklist de 9 Gates, histórico de entregas e habilitação dos **10 Produtos Modulares**.
- **[CATALOG.md](CATALOG.md)**: Cobertura completa das fontes públicas nos 13 macrosegmentos, protocolos, chaves canônicas, ondas (`Wave 1..6`) e scores de priorização `RICE`.
- **[docs/adr/](docs/adr/)**: *Architecture Decision Records* documentando escolhas de motor analítico, contratos de dados e hardening DevSecOps.

---

## Arquitetura Hexagonal & Diferenciais Enterprise

1. **Medallion Analytics & ML-Ready (`POP_T_143` / `POP_T_002` / `POP_T_128`):**
   - **Bronze:** Cache local SQLite (`WAL`) com TTL + snapshots brutos selados com manifesto `SHA-256`.
   - **Gate 5 Pilares & Quarentena (`POP_T_074` / `POP_T_070`):** Validação automática de *Freshness, Volume, Schema ODCS v3.1, Nulls e Sentinelas*. Lotes governamentais truncados vão para `data/quarantine/` sem corromper `Silver`.
   - **Silver:** Parquet `ZSTD` via DuckDB (`atomic swap`), chaves canônicas estritas em `STRING` (`id_municipio` 7D, `cnpj_14`, `cep_8`, `cnae_7`, `cbo_6`), conversão de códigos sentinela (`99`, `999`, `-1`, `"SEM INFORMACAO"`) em `NULL` reais e `reference_date` explícita.
   - **Gold (ML Feature Views):** Vetores de *features* por entidade, deflacionamento monetário (`IPCA`/`IGP-M`/`PTAX`) e suporte nativo a **`ASOF JOIN` (*Point-in-Time*)** prevenindo *Data Leakage* em modelos de Machine Learning.
2. **Motor de Enriquecimento Híbrido (`EnrichmentEngine` — `POP_T_174` / `POP_T_154`):**
   - Validação matemática offline de dígitos verificadores Mod-11 (`CNPJ`/`CPF`/`IBGE`) antes de qualquer chamada de rede.
   - Conciliação tolerante a ruído de nomes municipais + UF para `id_municipio` 7D.
   - Isolamento estrito cross-repo: ao enriquecer bases de clientes do `operations`, lê e grava exclusivamente no diretório isolado do cliente.
3. **Hardening DevSecOps & LGPD (`POP_T_175` / `POP_B_051`):**
   - Proteção contra *Zip Bombs* (`<= 100:1`) e verificação de *Magic Bytes*.
   - Sanitização contra *CSV Formula Injection* (`CWE-1236`) na exportação de planilhas enriquecidas.
   - Envelopamento `<untrusted_gov_data>` nas ferramentas MCP prevenindo *Indirect Prompt Injection* (`OWASP LLM01`).
   - Preservação compulsória de máscaras de CPF (`***.123.456-**`) e conformidade com o Art. 7º, §§ 3º e 4º e Art. 14 da LGPD.

---

## Instalação Rápida (Isolamento Python 3.12)

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt
pip install --no-deps -e .
```

## Uso Rápido

```powershell
# Consultar status evolutivo e catálogo de fontes (suporta pdg ou bdg)
pdg status
pdg catalog --wave 1

# Consultar schema semântico ou perfilar um Parquet sem estourar contexto de IA
pdg schema ibge_localidades

# Subir servidor REST OpenAPI 3.1
uvicorn public_data_gateway.api.app:create_app --factory --reload

# Iniciar servidor MCP stdio para agentes C.O.R.E.
pdg-mcp
```
