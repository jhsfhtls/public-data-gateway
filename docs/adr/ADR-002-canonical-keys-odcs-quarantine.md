# ADR-002: Chaves Canônicas `STRING`, Contratos ODCS v3.1 e Zona de Quarentena

- **Status:** Accepted
- **Date:** 2026-11-07
- **Governing POPs:** `POP_T_070`, `POP_T_074`, `POP_T_176`, `POP_T_177`

## Context
Bases governamentais apresentam três falhas recorrentes que corrompem modelos analíticos e de Machine Learning: (1) tipagem inteira que destrói zeros à esquerda em `CNPJ`, `CEP`, `CNAE` e `CBO` e mistura códigos municipais IBGE de 6 e 7 dígitos; (2) códigos sentinela numéricos (`99`, `999`, `-1`) representando valores ausentes; e (3) publicação eventual de arquivos mensais truncados.

## Decision
1. Todos os identificadores territoriais, fiscais e taxonômicos trafegam exclusivamente como `STRING` padronizada (`id_municipio` 7D, `cnpj_14`, `cep_8`, `cnae_7`, `cbo_6`, `ncm_8`, `cpf_masked`).
2. Cada conector possui um contrato declarativo `contract.yaml` (perfil ODCS v3.1) definindo unidades, granularidade, mapeamento explícito de sentinelas para `NULL` e regras dos 5 Pilares de Observabilidade (`POP_T_074`).
3. Qualquer extração que viole o contrato ou apresente queda anômala de volume é desviada para `data/quarantine/`, mantendo intacto o snapshot `Silver` anterior.

## Consequences
- Integridade referencial determinística entre os 13 macrosegmentos e eliminação de *Silent Data Corruption* em modelos downstream.
