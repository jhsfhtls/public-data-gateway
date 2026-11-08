# ADR-001: Arquitetura Híbrida Medallion Local (SQLite WAL + DuckDB Parquet ZSTD)

- **Status:** Accepted
- **Date:** 2026-11-07
- **Governing POPs:** `POP_T_112`, `POP_T_132`, `POP_T_143`, `POP_T_177`

## Context
As fontes públicas brasileiras dividem-se em APIs síncronas de baixa latência (< 300 ms, como BrasilAPI, BCB Olinda/SGS e IBGE Localidades) e dumps colunares/CSV massivos (como RFB CNPJ com ~80 GB descompactados, RAIS e Censo). Carregar dumps governamentais em Pandas na memória RAM causa OOM, enquanto depender exclusivamente de APIs remotas sofre rate-limiting e instabilidades de WAF.

## Decision
Adotamos o `HybridStorageEngine` com segregação de cargas OLTP/Cache vs OLAP:
1. **Cache Síncrono (Bronze Online):** SQLite local com `PRAGMA journal_mode=WAL` e `busy_timeout=5000` (`POP_T_132`), *single-flight lock* e fallback *Stale-While-Error* (`POP_T_152`).
2. **Lakehouse Analítico (Bronze/Silver/Gold Offline):** DuckDB embarcado (`memory_limit='4GB'`) persistindo tabelas Apache Parquet comprimidas com `ZSTD` via *atomic swap* (`.tmp.parquet` -> rename atômico), seladas por manifesto `SHA-256` e consultáveis com `ASOF JOIN` *point-in-time* para Machine Learning (`POP_T_128`).

## Consequences
- Zero custo de infraestrutura cloud para processamento local e enriquecimento em lote de centenas de milhares de registros em milissegundos.
- Exportação *zero-copy* via `pyarrow.Table` para pipelines de ML e BI do ecossistema C.O.R.E.
