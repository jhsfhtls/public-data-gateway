<div align="center">
  <h2>Jornada de Implementação — Brazil Data Gateway</h2>
  <p><i>Painel Vivo de Evolução Arquitetural, Ciclo de Vida de Conectores & Habilitação de Produtos C.O.R.E.</i></p>
  <p><b>Versão:</b> 1.0.0 &nbsp;|&nbsp; <b>Última Atualização:</b> 2026-11-08T06:55:00.000000+00:00 &nbsp;|&nbsp; <b>Governança:</b> POP_T_177 • POP_T_176 • POP_T_175 • POP_T_070 • POP_T_074 • POP_T_148</p>
  <hr/>
</div>
<br/>

# Jornada de Implementação & Painel Vivo de Governança

Este artefato constitui a **Fonte Única da Verdade Operacional (SSOT)** do status de desenvolvimento do `public-data-gateway`. Ele rastreia o avanço das macrofases arquiteturais, o ciclo de vida dos 9 Gates de qualidade por conector, o desbloqueio comercial dos 10 Produtos Modulares do ecossistema C.O.R.E. e o **Runbook Anti-Bus-Factor** para continuidade operacional autônoma.

---

## 1. Painel de Status Executivo em Tempo Real

- **Macrofase Ativa:** `Fase 04 — Ingestão Wave 2 (B2B, Compras Públicas & Compliance: rfb_cnpj_lakehouse, rfb_simples_mei, pncp_contratacoes, etc.)`
- **Status Geral de Governança:** `🟢 ON_TRACK`
- **Conector Ativo em Desenvolvimento:** `suframa_pim_zfm`
- **Próximo Conector na Fila de Execução:** `cvm_cia_aberta` (Wave 1 — Prioridade RICE 950)
- **Progresso Estrutural das Macrofases:** **5 de 12 Concluídas (41.7%)**
- **Universo de Fontes Homologadas no Catálogo:** **49 fontes públicas** nos 13 macrosegmentos

```mermaid
flowchart LR
    F00["Fase 00: ADRs & Governança<br/>✅ CONCLUÍDO"] --> F01["Fase 01: Setup & DevSecOps<br/>✅ CONCLUÍDO"]
    F01 --> F02["Fase 02: Catálogo & Interfaces<br/>✅ CONCLUÍDO"]
    F02 --> F03["Fase 03: Wave 1 Borda & Macro<br/>⏳ PRÓXIMO"]
    F03 --> F04["Fases 04..08: Waves 2 a 6<br/>⚪ PENDENTE"]
```

---

## 2. Acompanhamento das Macrofases Estruturais (Fases 00 a 08)

| ID | Macrofase de Desenvolvimento | Status | Conclusão | Entregáveis Principais & Evidências |
| :---: | :--- | :---: | :---: | :--- |
| **Fase 00** | **Arquitetura, Governança & Decisões Fundacionais (ADRs)** | 🟢 CONCLUÍDO | `2026-11-07` | • ADR-001: Storage Híbrido Medallion (SQLite WAL + DuckDB Parquet ZSTD)<br/>• ADR-002: Chaves Canônicas STRING, Contratos ODCS v3.1 & Quarentena<br/>• ADR-003: Hardening DevSecOps, Proteção LGPD & Client Path Shield |
| **Fase 01** | **Estrutura do Repositório Satélite, CI/CD & DevSecOps** | 🟢 CONCLUÍDO | `2026-11-07` | • Repositório satélite brazil-data-gateway inicializado<br/>• Isolamento Python 3.12 (.venv) e dependências pinadas (requirements.txt, requirements-dev.txt)<br/>• Configuração de CI (.github/workflows/ci.yml e pr-lint.yml)<br/>• Templates padronizados de Pull Request (feature, bugfix, docs) |
| **Fase 02** | **Fundação do Catálogo (49 Fontes), Normalização Canônica & Interfaces Unificadas** | 🟢 CONCLUÍDO | `2026-11-07` | • Base de referência sanitizada dos 5.571 municípios brasileiros (zero falhas de UF)<br/>• Catálogo formal de 49 fontes governamentais nos 13 macrosegmentos (catalog/sources_registry.yaml)<br/>• Renderizador determinístico do catálogo (scripts/render_catalog.py & CATALOG.md)<br/>• Tracker vivo da jornada de implementação (state/project_progress.json, scripts/render_progress.py & JORNADA_IMPLEMENTACAO.md)<br/>• Validação de integridade (--check) e conformidade ruff 100% verde |
| **Fase 03-SPIKE** | **Golden Spike: Piloto ibge_localidades com Todas as Tabelas & Camada de Negócio Territorial** | 🟢 CONCLUÍDO | `2026-11-07` | • Ingestão e normalização das 5 tabelas da API (municipios, distritos, subdistritos, estados, regioes)<br/>• Estrutura física isolada data/ibge_localidades/{bronze,silver,gold,quarantine}/<br/>• Encapsulamento dbt no conector (source.yml + 5 staging models + _models.yml com 100% colunas documentadas)<br/>• Camada de negócio territorial (dim_municipio SCD2, dim_distrito, crosswalk 6D/7D, mart_hierarquia_territorial)<br/>• Bateria de testes ao vivo de 1 linha com fail-hard e suite offline com cidades diversas |
| **Fase 03** | **Ingestão Wave 1 — APIs de Borda & Macroeconomia** | 🟢 CONCLUÍDO | `2026-11-07` | • Conector bcb_sgs (Séries macroeconômicas: IPCA, Selic, IGP-M)<br/>• Conector bcb_olinda_ptax (Cotações PTAX diárias OData v4)<br/>• Conector brasilapi_core (CEPs, bancos, DDDs, borda síncrona) |
| **Fase 04** | **Ingestão Wave 2 — B2B, Compras Públicas & Compliance** | 🟡 EM ANDAMENTO | `—` | • Conector rfb_cnpj_lakehouse (55M CNPJs em streaming DuckDB)<br/>• Conector rfb_simples_mei (Enquadramento tributário Simples/MEI)<br/>• Conector pncp_contratacoes (Licitações Lei 14.133)<br/>• Conector cgu_ceis_cnep & cgu_pep (Sanções e autoridades PEP)<br/>• Conector cnj_datajud (Metadados processuais de 90+ tribunais)<br/>• Conector tst_cndt (Certidões trabalhistas negativas)<br/>• Conector suframa_pim_zfm (Empresas incentivadas Polo Industrial) |
| **Fase 05** | **Ingestão Wave 3 — Mercado de Capitais & Finanças Públicas** | ⚪ PENDENTE | `—` | • Conector cvm_dfp_itr (Balanços e DRE de companhias abertas)<br/>• Conector cvm_fundos (Informes diários de fundos)<br/>• Conector bcb_ifdata (Balanços de instituições financeiras)<br/>• Conector stn_tesouro_transparente (SADIPEM e dívida pública)<br/>• Conector mdic_comexstat (Balança comercial por NCM e município)<br/>• Conector comprasgov_catmat (Catálogo unificado de compras públicas) |
| **Fase 06** | **Ingestão Wave 4 — Censo, Trabalho, Logística & Telecom** | ⚪ PENDENTE | `—` | • Conectores ibge_sidra, ibge_pib_municipal, ibge_censo2022, ibge_pnad_continua<br/>• Conectores mte_novo_caged & mte_rais<br/>• Conectores ibge_cnefe, ibge_malhas, anatel_mosaico, antt_malha_transporte |
| **Fase 07** | **Ingestão Wave 5 — Saúde, Educação & Setor Notarial** | ⚪ PENDENTE | `—` | • Conectores DataSUS (CNES, SIH, SIM, SINASC, OpenDataSUS) & anvisa_cmed<br/>• Conectores INEP (Censo Escolar, Superior, IDEB, ENEM) & capes_sucupira<br/>• Conectores Notariais (cnj_justica_aberta, onr_registro_imoveis, capitais_iptu_itbi) |
| **Fase 08** | **Ingestão Wave 6 — Meio Ambiente, Agro & Metaportais** | ⚪ PENDENTE | `—` | • Conectores Ambientais (ana_hidroweb, inpe_terrabrasilis, mapbiomas, sfb_sicar, mapa_zarc)<br/>• Conectores Metaportais (basedosdados_bq, dadosgov_ckan) |
| **Fase 09** | **Pós-Ingestão Total — Tabelas Gold Analíticas Cross-Domain, Datamarts & Insights** | ⚪ PENDENTE | `—` | • Data Marts analíticos multidisciplinares (B2B x Território x Macro x Compras Públicas)<br/>• Feature Store consolidada para modelos preditivos de propensão e risco<br/>• Extração de insights comerciais e operacionais acoplados aos 10 Produtos Modulares C.O.R.E.<br/>• Bateria de Regressão Live Global com Rotação Sistemática de IDs: Re-execução obrigatória de testes reais ao vivo contra todos os órgãos públicos, alternando e rotacionando os IDs de consulta (novos CNPJs, novos municípios IBGE, novas moedas, séries BACEN e novos contratos PNCP) para validar a resiliência contínua contra drift em produção sem overfitting a registros pontuais |
| **Fase 10** | **Expansão de Fronteira: Auditoria Conecta GOV.BR, Google APIs & Inclusão de APIs Públicas Globais e Geoespaciais** | ⚪ PENDENTE | `—` | • Auditoria e varredura do Conecta GOV.BR (https://www.gov.br/conecta/catalogo/) para identificar e homologar APIs governamentais federais adicionais que ficaram fora do catálogo inicial<br/>• Mapeamento, seleção e pré-configuração de APIs públicas globais e geoespaciais de alto valor analítico: Google APIs (Google Maps Geocoding, Places, Routes) e alternativas abertas como OpenStreetMap / Nominatim (https://www.openstreetmap.org/) e Mapbox API (https://www.mapbox.com/)<br/>• Ampliação do escopo do gateway para além das APIs governamentais do Brasil: mapeamento sistemático e suporte a outras APIs públicas úteis (ex.: dados climáticos, câmbio internacional, georreferenciamento global), deixando os conectores pré-configurados<br/>• Bipartição da documentação oficial, dbt e catálogo mestre estruturada em: 1) Catálogo APIs Gov Brasil e 2) Catálogo Outras APIs Públicas & Globais<br/>• Disponibilização de conectores com o mesmo padrão ODCS v3.1, camadas Medallion (Bronze/Silver/Gold) e testes fail-hard com limites de rate limit e FinOps |

---

## 3. Checklist dos 9 Gates de Qualidade do Conector Ativo

Todo conector integrado ao `public-data-gateway` deve percorrer rigorosamente os **9 Gates de Qualidade** antes de ser promovido a `HOMOLOGATED`. O alvo atual do ciclo de vida é o conector `rfb_simples_mei` (Wave 2):

| Gate | Nome do Gate de Homologação | Critério Operacional de Aceite (`POP_T_070` / `POP_T_074` / `POP_T_177`) | Status |
| :---: | :--- | :--- | :---: |
| **GATE_01** | **Contrato Declarativo ODCS v3.1 das N Tabelas da API** | Schema tipado, chaves primárias STRING e regras semânticas para simples_nacional_cadastral, simei_cadastral, simples_sublimites_estaduais, simples_eventos_historico e simples_tabelas_anexos. | ⚪ NÃO INICIADO |
| **GATE_02** | **Ingestão Bronze Isolada (Todas as Tabelas) & Cache WAL** | Download resiliente das 5 tabelas em data/rfb_simples_mei/bronze/ com cache SQLite WAL. | ⚪ NÃO INICIADO |
| **GATE_03** | **Sanitização de Sentinelas & Nulls** | Conversão determinística de códigos sentinela, zeros e hífens em NULLs reais em todas as tabelas. | ⚪ NÃO INICIADO |
| **GATE_04** | **Materialização Silver Parquet Isolada (Todas as Tabelas)** | Gravação de simples_nacional_cadastral.parquet, simei_cadastral.parquet, etc., em data/rfb_simples_mei/silver/ com SHA-256. | ⚪ NÃO INICIADO |
| **GATE_05** | **Validação dos 5 Pilares & Quarentena** | Aprovação em Freshness, Volume, Schema, Null Ceiling e Lineage; desvio para data/rfb_simples_mei/quarantine/ se violado. | ⚪ NÃO INICIADO |
| **GATE_06** | **Gold Feature View Isolada & ASOF Join ML** | Features analíticas tributárias em data/rfb_simples_mei/gold/features_tributarias_simples.parquet com propensão, tempo de permanência e risco de desenquadramento. | ⚪ NÃO INICIADO |
| **GATE_07** | **Integração ao EnrichmentEngine** | Disponibilização para enriquecimento tributário e cadastral batch/online respeitando o Client Path Shield (POP_T_154). | ⚪ NÃO INICIADO |
| **GATE_08** | **Exposição Multi-Interface** | Exposição síncrona/analítica simultânea via Python SDK (PublicData / BrazilData), REST (/api/v1/simples), CLI (pdg simples) e Servidor MCP. | ⚪ NÃO INICIADO |
| **GATE_09** | **Bateria de Testes Offline com Períodos Diversos** | Testes unitários offline com fixtures cobrindo optantes, MEI ativo, desenquadramentos, eventos e sublimites estaduais. | ⚪ NÃO INICIADO |
| **GATE_10** | **dbt Staging, source.yml e 100% Colunas Documentadas** | Pasta connectors/.../rfb_simples_mei/dbt/ com source.yml próprio, 5 modelos staging, dim_simples_nacional, dim_simei, dim_sublimites_icms, mart_regimes_tributarios_brasil e 100% colunas documentadas. | ⚪ NÃO INICIADO |
| **GATE_11** | **Teste Real de 1 Linha com API ao Vivo (Fail-Hard)** | Execução obrigatória contra endpoint ao vivo de consulta de situação Simples/MEI limitado a 1 registro; falha reprova o pipeline. | ⚪ NÃO INICIADO |

---

## 4. Progresso por Onda de Ingestão (Waves 1 a 6)

Estatísticas dinâmicas cruzadas diretamente com o catálogo mestre `catalog/sources_registry.yaml`:

| Onda | Foco Estratégico | Total Fontes | RICE Médio | Modos Predominantes | Fontes Âncora | Status da Onda |
| :---: | :--- | :---: | :---: | :--- | :--- | :---: |
| **Wave 1** | APIs de Borda & Macroeconomia | 4 | 878 | `FEDERATED_API` | `ibge_localidades`, `brasilapi_core`, `bcb_sgs` (+1) | 🟡 Em Preparação |
| **Wave 2** | B2B, Compras Públicas & Compliance | 8 | 744 | `FEDERATED_API, LAKEHOUSE_BATCH` | `rfb_cnpj_lakehouse`, `pncp_contratacoes`, `cnj_datajud` (+5) | ⚪ Planejado |
| **Wave 3** | Mercado de Capitais & Finanças Públicas | 6 | 645 | `FEDERATED_API, LAKEHOUSE_BATCH` | `cvm_dfp_itr`, `mdic_comexstat`, `cvm_fundos` (+3) | ⚪ Planejado |
| **Wave 4** | Censo, Trabalho, Logística & Telecom | 10 | 553 | `FEDERATED_API, LAKEHOUSE_BATCH` | `ibge_sidra`, `ibge_pib_municipal`, `ibge_censo2022` (+7) | ⚪ Planejado |
| **Wave 5** | Saúde, Educação & Setor Notarial | 14 | 451 | `FEDERATED_API, LAKEHOUSE_BATCH` | `datasus_cnes`, `datasus_sih`, `inep_censo_escolar` (+11) | ⚪ Planejado |
| **Wave 6** | Meio Ambiente, Agro & Metaportais | 7 | 369 | `BIGQUERY_FEDERATED, FEDERATED_API, LAKEHOUSE_BATCH` | `inpe_terrabrasilis`, `basedosdados_bq`, `ana_hidroweb_telemetria` (+4) | ⚪ Planejado |

---

## 5. Matriz de Habilitação dos 10 Produtos Modulares C.O.R.E.

O `public-data-gateway` atua como barramento alimentador de dados para os **10 Produtos Modulares** do ecossistema corporativo (definidos em `biz-feasibility-evaluator` e no Catálogo de Ofertas GTM):

| Produto | Nome Comercial do Módulo | Status de Prontidão no Gateway | Fontes Mapeadas que Viabilizam o Módulo |
| :---: | :--- | :--- | :--- |
| **P01** | **Modern Data Platform & Analytics (Lakehouse as a Service)** | 🟢 Fundação Operacional (SQLite WAL, DuckDB Medallion & Quarentena) | `ibge_localidades`, `brasilapi_core`, `bcb_sgs`, `bcb_olinda_ptax` *(+45 fontes)* |
| **P02** | **IA Agêntica & Automação Cognitiva (Enterprise AI)** | 🟢 Fundação Operacional (Servidor MCP bdg-mcp & Prompt Shield) | `ibge_localidades`, `brasilapi_core`, `bcb_sgs`, `rfb_cnpj_lakehouse` *(+22 fontes)* |
| **P03** | **Software FullStack & Mobile Offline-First (Clean Arch/PostGIS)** | 🟢 Base Territorial Pronta (5.571 Municípios Sanitizados) | `ibge_localidades`, `brasilapi_core`, `ibge_censo2022`, `ibge_cnefe` *(+5 fontes)* |
| **P04** | **Automação Inteligente de Processos (DPA & Power Platform)** | 🟡 Aguardando Ingestão Wave 1/2 (BrasilAPI & PNCP) | `brasilapi_core`, `rfb_cnpj_lakehouse`, `pncp_contratacoes`, `comprasgov_catmat` *(+3 fontes)* |
| **P05** | **FinOps & Otimização de Custos em Nuvem (Gain-Sharing)** | 🟡 Aguardando Ingestão Wave 1/3 (BCB SGS, PTAX & CVM) | `bcb_sgs`, `bcb_olinda_ptax`, `cvm_dfp_itr`, `cvm_fundos` *(+3 fontes)* |
| **P06** | **Data Clean Rooms & Parcerias Seguras (Privacidade Diferencial)** | 🟡 Aguardando Ingestão Wave 2 (RFB CNPJ & Quarentena) | `rfb_cnpj_lakehouse`, `cgu_ceis_cnep`, `cgu_pep`, `inpe_terrabrasilis` *(+3 fontes)* |
| **P07** | **Cibersegurança Corporativa & Homologação Enterprise (SOC 2/BACEN)** | 🟡 Aguardando Ingestão Wave 2 (CGU PEP/CEIS & CNJ DataJud) | `cnj_datajud`, `cgu_ceis_cnep`, `cgu_pep`, `tst_cndt` |
| **P08** | **AI Red Teaming & Auditoria Ética de Algoritmos (OWASP/SHAP)** | 🟢 Ativo (Proteção CWE-1236, Magic Bytes & Anti-Prompt Injection) | *(em definição)* |
| **P09** | **Convergência Industrial OT/IT & IoT Analytics (OEE ISO 22400)** | 🟡 Aguardando Ingestão Wave 2/6 (SUFRAMA PIM & ANA Telemetria) | `suframa_pim_zfm`, `anatel_mosaico`, `antt_malha_transporte`, `inpe_terrabrasilis` *(+4 fontes)* |
| **P10** | **Retainer de Sustentação Contínua & SRE as a Service (ARR 99.9%)** | 🟢 Ativo (5 Pilares de Observabilidade & Health Check API) | *(em definição)* |

---

## 6. Trilha Histórica de Marcos Concluídos

| Data | Fase | Marco Estrutural Realizado | Resumo Executivo & Impacto |
| :---: | :---: | :--- | :--- |
| `2026-11-07` | **Fase 00** | **Arquitetura & Governança Fundacional** | Elaboração das ADR-001 (Storage Híbrido Medallion), ADR-002 (Chaves Canônicas e Quarentena) e ADR-003 (DevSecOps e LGPD) fundamentadas nos POP_T_177, POP_T_176, POP_T_175, POP_T_070 e POP_T_074. |
| `2026-11-07` | **Fase 01** | **Setup do Repositório Satélite & Workflows CI** | Inicialização do repositório satélite brazil-data-gateway com isolamento venv Python 3.12, pyproject.toml, requirements.txt pinados, templates de PR e workflows de CI (pr-lint e ci.yml). |
| `2026-11-07` | **Fase 02** | **Catálogo Mestre (49 Fontes), Normalização Canônica & Interfaces Unificadas** | Catalogação formal de 49 fontes públicas nos 13 macrosegmentos e Ondas 1..6; sanitização da base de referência dos 5.571 municípios brasileiros; implementação dos motores determinísticos render_catalog.py e render_progress.py com verificação --check. |
| `2026-11-07` | **Fase 03-SPIKE** | **Homologação do Golden Spike ibge_localidades (5 Tabelas, dbt, Negócio & Live Fail-Hard)** | Execução completa do Golden Spike: 5 tabelas da API do IBGE (5.571 municípios, 10.751 distritos, 646 subdistritos, 27 UFs, 5 regiões) materializadas em data/ibge_localidades/{bronze,silver,gold}/; 9 modelos dbt (5 staging, dim_municipio, dim_distrito, xwalk_ibge_6d_7d, mart_hierarquia_territorial_brasil) com 109 testes 100% aprovados; 108 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard e suite com 35 testes unitários/integração 100% verde. |
| `2026-11-07` | **Fase 03** | **Homologação do Conector bcb_sgs (6 Tabelas, dbt, Mart Macroeconômico & Live Fail-Hard)** | Implementação de ponta a ponta do conector bcb_sgs: 6 tabelas (sgs_catalogo, sgs_ipca, sgs_igpm, sgs_selic, sgs_cdi, sgs_ibcbr) materializadas em data/bcb_sgs/{bronze,silver,gold}/; 9 novos modelos dbt (6 staging, dim_serie_macroeconomica, fct_macro_indicadores_mensal, mart_cenario_macroeconomico_brasil) com 167 testes dbt 100% aprovados; 195 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard contra os 7 endpoints oficiais do BACEN e suíte offline com 37 testes pytest 100% verde. |
| `2026-11-07` | **Fase 03** | **Homologação do Conector bcb_olinda_ptax (4 Tabelas, dbt, Mart de Volatilidade Cambial & Live Fail-Hard)** | Implementação de ponta a ponta do conector bcb_olinda_ptax: 4 tabelas (ptax_moedas, ptax_cotacao_dolar, ptax_cotacao_moedas, ptax_boletins) materializadas em data/bcb_olinda_ptax/{bronze,silver,gold}/; 7 novos modelos dbt (4 staging, dim_moeda, fct_cambio_ptax_diario, mart_cambio_volatilidade_brasil) com 220 testes dbt 100% aprovados; 260 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard contra os endpoints oficiais Olinda PTAX e suíte offline com 42 testes pytest 100% verde. |
| `2026-11-07` | **Fase 03** | **Homologação do Conector brasilapi_core & Conclusão da Wave 1 (6 Tabelas, dbt, Mart Bancário & Live Fail-Hard)** | Implementação de ponta a ponta do conector brasilapi_core concluindo com sucesso 100% da Wave 1: 6 tabelas (brasilapi_bancos, brasilapi_pix_participantes, brasilapi_corretoras, brasilapi_feriados, brasilapi_ddds, brasilapi_ceps) materializadas em data/brasilapi_core/{bronze,silver,gold}/; 9 novos modelos dbt (6 staging, dim_instituicao_financeira, dim_calendario_nacional, mart_infraestrutura_bancaria_brasil) totalizando 34 modelos e 288 testes dbt 100% aprovados; 337 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard contra os 6 endpoints da BrasilAPI e suíte offline com 47 testes pytest 100% verde. |
| `2026-11-08` | **Fase 04** | **Homologação do Conector rfb_cnpj_lakehouse (10 Tabelas, dbt, Mart B2B & Live Fail-Hard)** | Implementação de ponta a ponta do conector rfb_cnpj_lakehouse iniciando a Wave 2: 10 tabelas (rfb_cnaes, rfb_motivos, rfb_municipios, rfb_naturezas, rfb_paises, rfb_qualificacoes, rfb_empresas, rfb_estabelecimentos, rfb_socios, rfb_simples) materializadas em data/rfb_cnpj_lakehouse/{bronze,silver,gold}/; 14 novos modelos dbt (10 staging, dim_empresa, dim_estabelecimento, dim_socio_qsa, mart_densidade_empresarial_brasil) totalizando 48 modelos e 416 testes dbt 100% aprovados; 481 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard contra o endpoint oficial de CNPJ e suíte offline com 54 testes pytest 100% verde. |
| `2026-11-08` | **Fase 04** | **Transição de Identidade: Renomeação para public-data-gateway & Registro da Fase 10 (Conecta GOV.BR & Geo/Maps)** | Renomeação oficial do repositório e projeto para public-data-gateway, integrando suporte simultâneo aos comandos pdg/bdg e pdg-mcp/bdg-mcp, atualização dos vínculos do ecossistema C.O.R.E. e registro da Fase 10 no plano de implementação contemplando a auditoria do Conecta GOV.BR, viabilidade de APIs geoespaciais (Google Maps, OpenStreetMap, Mapbox) e futura bipartição entre catálogo Gov Brasil e catálogo público global. |
| `2026-11-08` | **Fase 04** | **Homologação do Conector rfb_simples_mei (5 Tabelas, dbt, Mart Tributário & Live Fail-Hard)** | Implementação de ponta a ponta do conector rfb_simples_mei consolidando a Wave 2: 5 tabelas (simples_nacional_cadastral, simei_cadastral, simples_sublimites_estaduais, simples_eventos_historico, simples_tabelas_anexos) materializadas em data/rfb_simples_mei/{bronze,silver,gold}/; 9 novos modelos dbt (5 staging, dim_simples_nacional, dim_simei, dim_sublimites_icms, mart_regimes_tributarios_brasil) totalizando 57 modelos e 477 testes dbt 100% aprovados; 541 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard contra o endpoint oficial de CNPJ/Simples e suíte offline com 60 testes pytest 100% verde; registro formal da expansão de escopo da Fase 10 para inclusão de APIs públicas globais e geoespaciais (Google APIs, OpenStreetMap, Mapbox). |
| `2026-11-08` | **Fase 04** | **Homologação do Conector pncp_contratacoes (5 Tabelas, dbt, Mart de Compras Públicas & Live Fail-Hard)** | Implementação completa do conector pncp_contratacoes (Lei nº 14.133/2021): 5 tabelas (pncp_contratacoes, pncp_contratos, pncp_atas, pncp_orgaos, pncp_modalidades) materializadas em data/pncp_contratacoes/{bronze,silver,gold}/; 10 novos modelos dbt (5 staging views, dim_modalidade_licitacao, dim_orgao_publico, fct_contratacao_publica, fct_contrato_administrativo, mart_compras_publicas_brasil) totalizando 67 modelos e 576 testes dbt 100% aprovados; 730 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard estrito contra a API oficial de produção do PNCP e 65 testes offline aprovados; feature store features_compras_publicas_brasil.parquet gerada com score de maturidade licitatória. |
| `2026-11-08` | **Fase 04** | **Homologação do Conector cgu_ceis_cnep (5 Tabelas, dbt, Mart de Sanções & Compliance & Live Fail-Hard)** | Implementação completa do conector cgu_ceis_cnep (Leis nº 12.846/2013 e 14.133/2021): 5 tabelas (cgu_ceis, cgu_cnep, cgu_cepim, cgu_acordos_leniencia, cgu_orgaos_sancionadores) materializadas em data/cgu_ceis_cnep/{bronze,silver,gold}/; 9 novos modelos dbt (5 staging views, dim_tipo_sancao, dim_empresa_sancionada, fct_sancao_administrativa, mart_compliance_sancoes_brasil) totalizando 76 modelos e 672 testes dbt 100% aprovados; 846 colunas 100% documentadas no linter; testes reais de 1 linha com fail-hard estrito contra a OpenAPI e endpoints oficiais da CGU; feature store features_compliance_risco_brasil.parquet gerada com score de risco de compliance; multi-interface completa via SDK (PublicData.compliance), CLI (pdg compliance), REST API (/api/v1/compliance) e MCP (bdg_get_compliance_status). |
| `2026-11-08` | **Fase 04** | **Homologação do Conector cgu_pep (5 Tabelas, dbt, Mart de Exposição Política & Live Fail-Hard)** | Implementação completa do conector cgu_pep (Resolução COAF nº 40/2021 & Circular BACEN nº 3.978/2020 / PLD/FTP): 5 tabelas (cgu_pep, cgu_pep_orgaos, cgu_pep_funcoes, cgu_pep_historico_carencia, cgu_pep_relacionados) materializadas em data/cgu_pep/{bronze,silver,gold}/; 9 novos modelos dbt (5 staging views, dim_pep, dim_funcao_pep, fct_mandato_pep, mart_pep_distribuicao_brasil) totalizando 85 modelos e 759 testes dbt 100% aprovados; 931 colunas 100% documentadas no linter; feature store features_pep_exposicao_politica_brasil.parquet gerada com score de risco PLD e categorização de quarentena de 5 anos; testes reais de 1 linha com fail-hard contra a API oficial da CGU (/api-de-dados/peps) e 11 testes pytest aprovados; multi-interface completa via SDK (PublicData.compliance.pep_status), CLI (pdg pep status), REST API (/api/v1/pep) e MCP (bdg_get_pep_status). |
| `2026-11-08` | **Fase 04** | **Homologação do Conector cnj_datajud (5 Tabelas, dbt, Mart de Litigiosidade & Live Fail-Hard)** | Implementação completa do conector cnj_datajud (Base Nacional do Poder Judiciário — Resolução CNJ nº 331/2020 & Resolução CNJ nº 46/2007 TPU): 5 tabelas (cnj_processos, cnj_tribunais, cnj_classes_tpu, cnj_assuntos_tpu, cnj_movimentos_processuais) materializadas em data/cnj_datajud/{bronze,silver,gold}/; 10 novos modelos dbt (5 staging views, dim_tribunal_judiciario, dim_classe_judicial_tpu, dim_assunto_judicial_tpu, fct_processo_judicial, mart_litigiosidade_tribunais_brasil) totalizando 95 modelos e 863 testes dbt 100% aprovados; 1028 colunas 100% documentadas no linter; feature store features_litigiosidade_juridica_brasil.parquet gerada com score de litigiosidade e categorização de risco judicial; testes reais de 1 linha com fail-hard contra a API pública e Wiki do CNJ DataJud e 12 testes pytest aprovados; multi-interface completa via SDK (PublicData.judicial.litigation_risk e search_processos), CLI (pdg judicial risk e search), REST API (/api/v1/judicial) e MCP (bdg_get_litigation_risk). |
| `2026-11-08` | **Fase 04** | **Homologação do Conector tst_cndt (4 Tabelas, dbt, Mart de Compliance Trabalhista & Live Fail-Hard)** | Implementação completa do conector tst_cndt (Certidão Negativa de Débitos Trabalhistas & Banco Nacional de Devedores Trabalhistas - Lei nº 12.440/2011, Res. TST nº 1.470/2011 e CLT): 4 tabelas (tst_cndt_certidoes, tst_cndt_debitos_bndt, tst_cndt_tribunais_regionais, tst_cndt_status_dominio) materializadas em data/tst_cndt/{bronze,silver,gold}/; 9 novos modelos dbt (4 staging views, dim_tribunal_regional_trabalho, dim_status_cndt, fct_certidao_trabalhista, fct_debito_trabalhista_bndt, mart_compliance_trabalhista_brasil) totalizando 104 modelos e 968 testes dbt 100% aprovados; 1122 colunas 100% documentadas no linter; feature store features_compliance_trabalhista_brasil.parquet gerada com score contínuo de 0 a 100 de risco de passivo trabalhista; testes reais de 1 linha com fail-hard contra o portal oficial do TST CNDT e 10 testes pytest aprovados; multi-interface completa via SDK (PublicData.judicial.cndt, labor_risk, get_cndt, get_labor_risk), CLI (pdg judicial cndt, labor-risk, debitos), REST API (/api/v1/judicial/cndt, labor-risk, debitos) e MCP (bdg_get_cndt_certificate, bdg_get_labor_risk). |

---

## 7. Runbook Operacional Anti-Bus-Factor

Este runbook documenta os procedimentos padronizados para garantir que **qualquer engenheiro ou agente autônomo** possa assumir e operar o repositório sem atrito cognitivo:

### 7.1. Setup do Ambiente de Desenvolvimento (Python 3.12 Isolado)

```powershell
# 1. Clonar repositório e navegar para a raiz
cd c:\Users\jhose\GitOps\public-data-gateway

# 2. Criar e ativar o ambiente virtual isolado (.venv)
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Instalar dependências pinadas de produção e desenvolvimento
pip install -r requirements.txt -r requirements-dev.txt

# 4. Instalar o gateway em modo editável sem tocar nas dependências pinadas
pip install --no-deps -e .
```

### 7.2. Verificação de Integridade dos Painéis de Governança (`--check`)

Antes de qualquer commit ou abertura de Pull Request, execute a verificação estrita de sincronia:

```powershell
# Verificar conformidade do CATALOG.md
python scripts/render_catalog.py --check

# Verificar conformidade da JORNADA_IMPLEMENTACAO.md
python scripts/render_progress.py --check

# Executar linter ruff no diretório de scripts
ruff check scripts/
```

### 7.3. Como Iniciar a Ingestão do Próximo Conector (`ibge_localidades`)

1. **Atualizar o Estado:** Edite `state/project_progress.json` alterando `active_connector: 'ibge_localidades'` e defina a fase como `03`.
2. **Criar o Contrato:** Crie `src/brazil_data_gateway/connectors/ibge/contract.yaml` (perfil ODCS v3.1) definindo primary keys `id_municipio` e `sigla_uf`.
3. **Executar os 9 Gates:** Implemente o conector herdando de `BaseConnector`, conecte o cache SQLite WAL, valide os 5 pilares e gere Parquet ZSTD.
4. **Regenerar os Painéis:** Execute `python scripts/render_progress.py` para sincronizar `JORNADA_IMPLEMENTACAO.md`.
