<div align="center">
  <h2>Catálogo Mestre de Fontes Públicas & Registros Federativos Brasileiros</h2>
  <p><i>Mapeamento Unificado de 49 Fontes Governamentais nos 13 Macrosegmentos, Ondas 1 a 6 e Chaves Canônicas</i></p>
  <p><b>Versão:</b> 1.0.0 &nbsp;|&nbsp; <b>Última Atualização:</b> 2026-11-07 &nbsp;|&nbsp; <b>Governança:</b> POP_T_177 • POP_T_176 • POP_T_174 • POP_T_070 • POP_T_148</p>
  <hr/>
</div>
<br/>

# Catálogo Mestre de Fontes Públicas Governamentais (49 Fontes)

Este catálogo consolida formalmente as **49 bases de dados públicas e APIs governamentais do Brasil** integradas ao `public-data-gateway`. As fontes abrangem a totalidade dos **13 macrosegmentos estruturais federativos**, organizadas em **6 Ondas Estratégicas de Implementação** ordenadas por priorização ponderada `RICE` (`POP_B_091`), garantindo integridade cadastral canônica, observabilidade de 5 pilares (`POP_T_074`) e conformidade com a LGPD (`POP_B_051`).

---

## 1. Visão Geral & Métricas Consolidadas

- **Total de Fontes Mapeadas:** 49 fontes governamentais homologadas
- **Total de Macrosegmentos:** 13 macrosegmentos federativos cobertos (100% de cobertura)
- **Ondas de Implementação:** 6 ondas estruturadas por retorno analítico e complexidade operacional
- **Chaves Canônicas Unificadas:** 8 chaves relacionais padronizadas em `STRING` (`id_municipio` 7D, `sigla_uf`, `cnpj_14`, `cpf_masked`, `cep_8`, `cnae_7`, `cbo_6`, `ncm_8`)
- **Habilitação de Produtos C.O.R.E.:** 10 de 10 Produtos Modulares (`P01` a `P10`) suportados por dados oficiais

### 1.1. Distribuição por Modo de Acesso

| Modo de Acesso | Total Fontes | % do Catálogo | Perfil Operacional |
| :--- | :---: | :---: | :--- |
| `FEDERATED_API` | 26 | 53.1% | APIs REST / OData síncronas de borda e baixa latência (< 300 ms) |
| `LAKEHOUSE_BATCH` | 21 | 42.9% | Dumps colunares e volumosos em DuckDB / Parquet com streaming |
| `BIGQUERY_FEDERATED` | 2 | 4.1% | Consultas SQL em datalake federado (Base dos Dados / MapBiomas) |

### 1.2. Distribuição por Classificação LGPD

| Tier LGPD | Total Fontes | % do Catálogo | Base Legal & Salvaguarda |
| :--- | :---: | :---: | :--- |
| `PUBLIC_NON_PERSONAL` | 29 | 59.2% | LAI / Dados abertos de pessoas jurídicas ou estatísticas sem PII |
| `PUBLIC_CORPORATE_QSA` | 8 | 16.3% | Art. 7º, §§ 3º e 4º LGPD: Cadastros empresariais com máscara CPF |
| `ANONYMIZED_MICRODATA` | 11 | 22.4% | Art. 12 LGPD: Microdados com bloqueio de reidentificação |
| `PUBLIC_AGENT_PEP` | 1 | 2.0% | Finalidade estrita KYC/Compliance com trilha de auditoria |

---

## 2. Matriz de Cobertura dos 13 Macrosegmentos

| ID Segmento | Macrosegmento Governamental | Total Fontes | Ondas Cobertas | Descrição Executiva | Status |
| :--- | :--- | :---: | :---: | :--- | :---: |
| `01_b2b_empresarial` | **Empresarial, Cadastral & Societário B2B** | 3 | `W2` | Dados de mais de 55 milhões de CNPJs (matrizes, filiais, sócios QSA, capital social, CNAEs, situação cadastral, optantes pelo Simples Nacional e MEI). | 🟢 Ativo |
| `02_economico_macrofinanceiro` | **Econômico, Monetário & Macrofinanceiro** | 6 | `W1, W3` | Séries macroeconômicas oficiais (IPCA, Selic, IGP-M), balanços contábeis trimestrais de instituições financeiras (IFData), cotações diárias de moedas (PTAX) e dados contábeis da CVM. | 🟢 Ativo |
| `03_demografico_censitario` | **Demográfico, Censitário & Domiciliar** | 3 | `W4` | Censo Demográfico 2022 em nível de 450.000 setores censitários, características de domicílios, PNAD Contínua (renda e trabalho) e PIB municipal do IBGE. | 🟢 Ativo |
| `04_trabalho_renda` | **Trabalho, Emprego & Renda** | 3 | `W4` | Fluxos mensais de contratações e demissões do Novo CAGED (salários de admissão por CBO e setor) e estoque de empregos da RAIS. | 🟢 Ativo |
| `05_geografico_logistica` | **Geográfico, Logístico & Endereçamento** | 5 | `W1, W4` | 111+ milhões de coordenadas pontuais do CNEFE/IBGE, limites territoriais vetoriais, base de logradouros/CEPs e antenas de telecomunicação da Anatel. | 🟢 Ativo |
| `06_saude_publica` | **Saúde Pública & Epidemiologia** | 6 | `W5` | Registros de óbitos (SIM), nascimentos (SINASC), internações hospitalares (SIH/SUS), estabelecimentos de saúde (CNES) e preços regulados de medicamentos (CMED/ANVISA). | 🟢 Ativo |
| `07_educacao_capital_humano` | **Educação & Capital Humano** | 5 | `W5` | Infraestrutura de escolas e matrículas (Censo Escolar), universidades e cursos (Censo Superior), notas por escola (IDEB) e microdados do ENEM. | 🟢 Ativo |
| `08_judicial_compliance` | **Judicial, Integridade & Compliance** | 4 | `W2` | Metadados de processos de todos os 90+ tribunais do Brasil (CNJ DataJud), cadastros de inidôneos e punidos (CEIS/CNEP/CEPIM), autoridades (PEP) e certidões negativas (CNDT). | 🟢 Ativo |
| `09_comercio_exterior` | **Comércio Exterior & Balança Comercial** | 1 | `W3` | Exportações e importações detalhadas por NCM, valor FOB, peso líquido, modal de transporte e município do exportador (Comex Stat). | 🟢 Ativo |
| `10_compras_publicas` | **Compras Públicas, Licitações & Contratos** | 2 | `W2, W3` | Repositório unificado da Lei nº 14.133/2021 (PNCP) com editais, atas e contratos de todos os entes federados e catálogo unificado CATMAT/CATSER. | 🟢 Ativo |
| `11_agronegocio_meio_ambiente` | **Agronegócio, Terra & Meio Ambiente** | 5 | `W6` | 7+ milhões de imóveis rurais com polígonos de reserva legal e APP (SICAR/CAR), desmatamento e alertas por satélite (INPE DETER/PRODES), uso da terra (MapBiomas) e zoneamento climático (ZARC). | 🟢 Ativo |
| `12_imobiliario_cartorial` | **Imobiliário, Notarial & Cartorial** | 3 | `W5` | Cadastro nacional de cartórios e produtividade (CNJ Justiça Aberta), transações registradas (ONR) e cadastros imobiliários municipais (IPTU/ITBI). | 🟢 Ativo |
| `13_metaportais_agregadores` | **Metaportais & Hubs Agregadores** | 3 | `W1, W6` | Plataformas de facilitação técnica que unificam e padronizam centenas de fontes em protocolos únicos (Base dos Dados, BrasilAPI, Dados.gov.br). | 🟢 Ativo |

---

## 3. Roadmap por Ondas de Implementação (Waves 1 a 6)

| Onda | Foco Estratégico & Justificativa | Total Fontes | Intervalo RICE | Modos de Acesso | Fontes Críticas | Status |
| :---: | :--- | :---: | :---: | :--- | :--- | :---: |
| **Wave 1** | APIs de Borda & Séries Macroeconômicas (Baixa Latência < 300ms) | 4 | 820–950 | `FEDERATED_API` | `ibge_localidades`, `brasilapi_core`, `bcb_sgs` (+1) | 🟡 Em Preparação |
| **Wave 2** | B2B, Compras Públicas, Integridade & Compliance Cadastral | 8 | 610–840 | `FEDERATED_API, LAKEHOUSE_BATCH` | `rfb_cnpj_lakehouse`, `pncp_contratacoes`, `cnj_datajud` (+5) | ⚪ Planejado |
| **Wave 3** | Mercado de Capitais, Balanços Contábeis & Finanças Públicas | 6 | 610–680 | `FEDERATED_API, LAKEHOUSE_BATCH` | `cvm_dfp_itr`, `mdic_comexstat`, `cvm_fundos` (+3) | ⚪ Planejado |
| **Wave 4** | Censo Demográfico, Trabalho, Renda, Logística & Telecomunicações | 10 | 500–590 | `FEDERATED_API, LAKEHOUSE_BATCH` | `ibge_sidra`, `ibge_pib_municipal`, `ibge_censo2022` (+7) | ⚪ Planejado |
| **Wave 5** | Saúde Pública, Educação, Capital Humano & Serviços Notariais | 14 | 410–480 | `FEDERATED_API, LAKEHOUSE_BATCH` | `datasus_cnes`, `datasus_sih`, `inep_censo_escolar` (+11) | ⚪ Planejado |
| **Wave 6** | Meio Ambiente, Agronegócio, Monitoramento Satelital & Metaportais | 7 | 340–390 | `BIGQUERY_FEDERATED, FEDERATED_API, LAKEHOUSE_BATCH` | `basedosdados_bq`, `inpe_terrabrasilis`, `ana_hidroweb_telemetria` (+4) | ⚪ Planejado |

---

## 4. Tabela Completa do Catálogo de Fontes Governamentais

Ordenada por **Onda de Implementação (`roadmap_wave`)**, **Score RICE decrescente** e **Identificador da Fonte**:

| ID Fonte | Dataset / Órgão Oficial | Macrosegmento | Onda | RICE | Modo / Protocolo | Chaves Canônicas | Tier LGPD | Produtos C.O.R.E. | Status |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- | :--- | :--- | :---: |
| `ibge_localidades` | [IBGE Localidades & Malha Territorial](https://servicodados.ibge.gov.br/api/docs/localidades)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Geográfico, Logístico | **W1** | 950 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` | `HOMOLOGATED` |
| `brasilapi_core` | [BrasilAPI: CEPs, FIPE, Bancos, DDD & CNPJ de Borda](https://brasilapi.com.br/docs)<br/>*Comunidade Open Source BrasilAPI* | Metaportais | **W1** | 880 | `FEDERATED_API`<br/>`REST_JSON` | `cep_8`<br/>`cnpj_14`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P04` | `HOMOLOGATED` |
| `bcb_sgs` | [Banco Central: Sistema Gerenciador de Séries Temporais (SGS)](https://dadosabertos.bcb.gov.br/dataset/series-temporais)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W1** | 860 | `FEDERATED_API`<br/>`REST_JSON` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P05` | `HOMOLOGATED` |
| `bcb_olinda_ptax` | [Banco Central Olinda: Cotações PTAX Moedas Diárias](https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/swagger-ui3)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W1** | 820 | `FEDERATED_API`<br/>`ODATA_V4` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `HOMOLOGATED` |
| `rfb_cnpj_lakehouse` | [Receita Federal: Cadastro Nacional da Pessoa Jurídica (CNPJ & QSA)](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)<br/>*Receita Federal do Brasil* | Empresarial, Cadastral | **W2** | 840 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`cnae_7`<br/>`id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P04` `P06` | `HOMOLOGATED` |
| `pncp_contratacoes` | [PNCP: Portal Nacional de Contratações Públicas (Lei 14.133)](https://pncp.gov.br/api/consulta/swagger-ui/index.html)<br/>*Governo Federal / PNCP* | Compras Públicas, Licitações | **W2** | 790 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P04` | `HOMOLOGATED` |
| `cnj_datajud` | [CNJ: Base Nacional do Poder Judiciário (DataJud)](https://datajud-wiki.cnj.jus.br/)<br/>*Conselho Nacional de Justiça (CNJ)* | Judicial, Integridade | **W2** | 780 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P07` | `CATALOGED` |
| `cgu_ceis_cnep` | [CGU: Cadastros de Empresas Inidôneas, Suspensas e Punidas (CEIS/CNEP)](https://portaldatransparencia.gov.br/swagger-ui.html)<br/>*Controladoria-Geral da União (CGU)* | Judicial, Integridade | **W2** | 760 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked` | `PUBLIC_CORPORATE_QSA` | `P01` `P06` `P07` | `HOMOLOGATED` |
| `cgu_pep` | [CGU: Cadastro de Pessoas Expostas Politicamente (PEP)](https://portaldatransparencia.gov.br/swagger-ui.html)<br/>*Controladoria-Geral da União (CGU)* | Judicial, Integridade | **W2** | 750 | `FEDERATED_API`<br/>`REST_JSON` | `cpf_masked` | `PUBLIC_AGENT_PEP` | `P01` `P06` `P07` | `CATALOGED` |
| `rfb_simples_mei` | [Receita Federal: Opção pelo Simples Nacional & MEI](https://dados.gov.br/dados/conjuntos-dados/simples-nacional)<br/>*Receita Federal do Brasil* | Empresarial, Cadastral | **W2** | 720 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` | `HOMOLOGATED` |
| `tst_cndt` | [TST: Certidão Negativa de Débitos Trabalhistas (CNDT)](https://cndt-certidao.tst.jus.br/)<br/>*Tribunal Superior do Trabalho (TST)* | Judicial, Integridade | **W2** | 700 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked` | `PUBLIC_CORPORATE_QSA` | `P01` `P07` | `CATALOGED` |
| `suframa_pim_zfm` | [SUFRAMA: Cadastro de Empresas Incentivadas do PIM / ZFM](https://www.gov.br/suframa/pt-br)<br/>*Superintendência da Zona Franca de Manaus (SUFRAMA)* | Empresarial, Cadastral | **W2** | 610 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `CATALOGED` |
| `cvm_dfp_itr` | [CVM: Demonstrações Financeiras Padronizadas (DFP / ITR)](https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/)<br/>*Comissão de Valores Mobiliários (CVM)* | Econômico, Monetário | **W3** | 680 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14` | `PUBLIC_CORPORATE_QSA` | `P01` `P05` | `CATALOGED` |
| `mdic_comexstat` | [MDIC: Comex Stat Exportações e Importações Detalhadas](https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas)<br/>*Ministério do Desenvolvimento, Indústria e Comércio Exterior (MDIC)* | Comércio Exterior | **W3** | 660 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `ncm_8`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |
| `cvm_fundos` | [CVM: Informe Diário de Fundos de Investimento](https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/)<br/>*Comissão de Valores Mobiliários (CVM)* | Econômico, Monetário | **W3** | 650 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `CATALOGED` |
| `bcb_ifdata` | [Banco Central: Informações Financeiras das IFs (IF.data)](https://www3.bcb.gov.br/ifdata/)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W3** | 640 | `FEDERATED_API`<br/>`ODATA_V4` | `cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `CATALOGED` |
| `stn_tesouro_transparente` | [Secretaria do Tesouro Nacional: SADIPEM & Dívida Pública](https://www.tesourotransparente.gov.br/)<br/>*Secretaria do Tesouro Nacional (STN)* | Econômico, Monetário | **W3** | 630 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `CATALOGED` |
| `comprasgov_catmat` | [Compras.gov.br: Catálogo Unificado de Materiais e Serviços (CATMAT/CATSER)](https://dados.gov.br/dados/conjuntos-dados/catalogo-de-materiais-e-servicos---catmat-catser)<br/>*Ministério da Gestão e Inovação em Serviços Públicos (MGI)* | Compras Públicas, Licitações | **W3** | 610 | `FEDERATED_API`<br/>`REST_JSON` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P04` | `CATALOGED` |
| `ibge_sidra` | [IBGE: Sistema IBGE de Recuperação Automática (SIDRA)](https://apisidra.ibge.gov.br/)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Demográfico, Censitário | **W4** | 590 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |
| `ibge_pib_municipal` | [IBGE: Produto Interno Bruto dos Municípios](https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Demográfico, Censitário | **W4** | 580 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |
| `ibge_censo2022` | [IBGE: Microdados do Censo Demográfico 2022](https://censo2022.ibge.gov.br/)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Demográfico, Censitário | **W4** | 570 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` `P03` | `CATALOGED` |
| `mte_novo_caged` | [MTE: Microdados do Novo CAGED](http://pdet.mte.gov.br/novo-caged)<br/>*Ministério do Trabalho e Emprego (MTE)* | Trabalho, Emprego | **W4** | 570 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cbo_6`<br/>`cnae_7`<br/>`id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `ibge_cnefe` | [IBGE: Cadastro Nacional de Endereços para Fins Estatísticos (CNEFE)](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Geográfico, Logístico | **W4** | 560 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cep_8`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `CATALOGED` |
| `ibge_pnad_continua` | [IBGE: Pesquisa Nacional por Amostra de Domicílios Contínua (PNAD Contínua)](https://www.ibge.gov.br/estatisticas/sociais/trabalho/9171-pesquisa-nacional-por-amostra-de-domicilios-continua-mensal.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Trabalho, Emprego | **W4** | 560 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `mte_rais` | [MTE: Relação Anual de Informações Sociais (RAIS)](http://pdet.mte.gov.br/rais)<br/>*Ministério do Trabalho e Emprego (MTE)* | Trabalho, Emprego | **W4** | 550 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `cbo_6`<br/>`cnae_7`<br/>`id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `ibge_malhas` | [IBGE: Malhas Territoriais Vetoriais (Municípios e UFs)](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Geográfico, Logístico | **W4** | 540 | `LAKEHOUSE_BATCH`<br/>`OGC_WFS_SHP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `CATALOGED` |
| `anatel_mosaico` | [ANATEL: Sistema Mosaico de Estações e Cobertura de Telecomunicações](https://sistemas.anatel.gov.br/mosaico/)<br/>*Agência Nacional de Telecomunicações (ANATEL)* | Geográfico, Logístico | **W4** | 510 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `CATALOGED` |
| `antt_malha_transporte` | [ANTT: Malha de Transporte Rodoviário e Ferroviário de Cargas](https://dados.antt.gov.br/)<br/>*Agência Nacional de Transportes Terrestres (ANTT)* | Geográfico, Logístico | **W4** | 500 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` `P09` | `CATALOGED` |
| `datasus_cnes` | [DataSUS: Cadastro Nacional de Estabelecimentos de Saúde (CNES)](https://cnes.datasus.gov.br/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 480 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |
| `datasus_sih` | [DataSUS: Sistema de Informações Hospitalares (SIH/SUS)](https://datasus.saude.gov.br/transferencia-de-arquivos/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 470 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `inep_censo_escolar` | [INEP: Microdados do Censo Escolar da Educação Básica](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 470 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `cnj_justica_aberta` | [CNJ: Justiça Aberta (Cadastro e Produtividade de Cartórios)](https://www.cnj.jus.br/corregedoria/justica_aberta/)<br/>*Conselho Nacional de Justiça (CNJ)* | Imobiliário, Notarial | **W5** | 460 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P04` | `CATALOGED` |
| `datasus_sim` | [DataSUS: Sistema de Informações sobre Mortalidade (SIM)](https://datasus.saude.gov.br/transferencia-de-arquivos/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 460 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `inep_censo_superior` | [INEP: Microdados do Censo da Educação Superior](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 460 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `anvisa_cmed` | [ANVISA: Câmara de Regulação do Mercado de Medicamentos (CMED)](https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos)<br/>*Agência Nacional de Vigilância Sanitária (ANVISA)* | Saúde Pública | **W5** | 450 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P04` | `CATALOGED` |
| `datasus_sinasc` | [DataSUS: Sistema de Informações sobre Nascidos Vivos (SINASC)](https://datasus.saude.gov.br/transferencia-de-arquivos/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 450 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `inep_ideb` | [INEP: Índice de Desenvolvimento da Educação Básica (IDEB)](https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 450 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |
| `onr_registro_imoveis` | [ONR: Sistema de Registro Eletrônico de Imóveis (SREI)](https://onr.org.br/)<br/>*Operador Nacional do Registro de Imóveis Eletrônico (ONR)* | Imobiliário, Notarial | **W5** | 450 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `CATALOGED` |
| `inep_enem` | [INEP: Microdados do Exame Nacional do Ensino Médio (ENEM)](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 440 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `opendatasus_vigilancia` | [OpenDataSUS: Notificações de Síndromes e Vigilância Epidemiológica](https://opendatasus.saude.gov.br/)<br/>*Ministério da Saúde* | Saúde Pública | **W5** | 440 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `CATALOGED` |
| `capitais_iptu_itbi` | [Capitais: Cadastros Imobiliários Municipais (IPTU / ITBI)](https://dados.gov.br/)<br/>*Prefeituras Municipais das Capitais* | Imobiliário, Notarial | **W5** | 430 | `FEDERATED_API`<br/>`REST_JSON` | `cep_8`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `CATALOGED` |
| `capes_sucupira` | [CAPES: Plataforma Sucupira (Pós-Graduação Stricto Sensu)](https://dadosabertos.capes.gov.br/)<br/>*Coordenação de Aperfeiçoamento de Pessoal de Nível Superior (CAPES)* | Educação | **W5** | 410 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |
| `basedosdados_bq` | [Base dos Dados: Datalake Público Federado (BigQuery)](https://basedosdados.org/)<br/>*Base dos Dados* | Metaportais | **W6** | 390 | `BIGQUERY_FEDERATED`<br/>`BIGQUERY_SQL` | `id_municipio`<br/>`sigla_uf`<br/>`cnpj_14`<br/>`cep_8` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P04` `P05` `P06` | `CATALOGED` |
| `inpe_terrabrasilis` | [INPE: Plataforma TerraBrasilis (DETER & PRODES)](http://terrabrasilis.dpi.inpe.br/)<br/>*Instituto Nacional de Pesquisas Espaciais (INPE)* | Agronegócio, Terra | **W6** | 390 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P06` `P09` | `CATALOGED` |
| `ana_hidroweb_telemetria` | [ANA: Rede Hidrometeorológica Nacional (HidroWeb)](https://www.snirh.gov.br/hidroweb/)<br/>*Agência Nacional de Águas e Saneamento Básico (ANA)* | Agronegócio, Terra | **W6** | 380 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `CATALOGED` |
| `mapbiomas_cobertura` | [MapBiomas: Cobertura e Uso da Terra no Brasil (1985-2024)](https://mapbiomas.org/)<br/>*Projeto MapBiomas* | Agronegócio, Terra | **W6** | 370 | `BIGQUERY_FEDERATED`<br/>`BIGQUERY_SQL` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P06` `P09` | `CATALOGED` |
| `sfb_sicar` | [SFB: Sistema Nacional de Cadastro Ambiental Rural (SICAR)](https://www.car.gov.br/)<br/>*Serviço Florestal Brasileiro (SFB) / MMA* | Agronegócio, Terra | **W6** | 360 | `LAKEHOUSE_BATCH`<br/>`OGC_WFS_SHP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P06` `P09` | `CATALOGED` |
| `mapa_zarc` | [MAPA: Zoneamento Agrícola de Risco Climático (ZARC)](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programas-de-gestao-de-risco/zoneamento-agricola-de-risco-climatico)<br/>*Ministério da Agricultura e Pecuária (MAPA) / Embrapa* | Agronegócio, Terra | **W6** | 350 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `CATALOGED` |
| `dadosgov_ckan` | [Dados.gov.br: Portal Brasileiro de Dados Abertos (Catálogo CKAN)](https://dados.gov.br/dados/conjuntos-dados)<br/>*Governo Federal (Dados.gov.br)* | Metaportais | **W6** | 340 | `FEDERATED_API`<br/>`REST_JSON` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `CATALOGED` |

---

## 5. Dicionário de Chaves Canônicas & Tipagem Estrita (`ADR-002`)

Conforme estabelecido no **ADR-002** e **POP_T_174**, todas as chaves identificadoras trafegam **compulsoriamente como `STRING`**, impedindo a perda de zeros à esquerda provocada por tipagens numéricas inadequadas:

| Chave Canônica | Tipo Estrito | Padrão / Máscara Canônica | Algoritmo de Validação | POPs de Ground Truth |
| :--- | :--- | :--- | :--- | :--- |
| `id_municipio` | `STRING(7)` | IBGE 7 Dígitos (ex: '3550308') | Módulo 10 com pesos [1, 2] | POP_T_174, POP_T_177 |
| `sigla_uf` | `STRING(2)` | UF Oficial 2 Letras maiúsculas (ex: 'SP') | Tabela IBGE das 27 UFs | POP_T_174, POP_T_177 |
| `cnpj_14` | `STRING(14)` | CNPJ Canônico Zfill 14D (ex: '00000000000191') | Módulo 11 (2 dígitos verificadores) | POP_T_174, POP_T_175 |
| `cpf_masked` | `STRING(14)` | Máscara Protegida (ex: '***.123.456-**') | Preservação compulsória Art. 7 LGPD | POP_T_175, POP_B_051 |
| `cep_8` | `STRING(8)` | CEP Canônico Zfill 8D (ex: '01310100') | DNE Correios / BrasilAPI | POP_T_174, POP_T_177 |
| `cnae_7` | `STRING(7)` | CNAE Subclasse 2.0 (ex: '6201501') | Tabela CONCLA / IBGE | POP_T_174, POP_T_177 |
| `cbo_6` | `STRING(6)` | CBO-2002 Ocupação (ex: '212405') | Tabela MTE / CBO | POP_T_174, POP_T_177 |
| `ncm_8` | `STRING(8)` | Nomenclatura Comum Mercosul (ex: '84713012') | Sistema Harmonizado MDIC | POP_T_174, POP_T_177 |

---

## 6. Governança da Privacidade & Tiers de Conformidade LGPD (`ADR-003`)

Em conformidade com a Lei Federal nº 13.709/2018 (LGPD) e o **ADR-003**, todo dataset incorporado ao catálogo opera sob um dos quatro tiers de privacidade:

| Tier de Privacidade | Base Legal Principal | Regra de Governança & Minimização | Exemplos de Datasets |
| :--- | :--- | :--- | :--- |
| `PUBLIC_NON_PERSONAL` | Art. 7º LAI / Art. 1º LGPD | Dados cadastrais públicos de entes, agregados estatísticos ou ambientais sem PII. | IBGE Localidades, BCB SGS, CNEFE, MapBiomas |
| `PUBLIC_CORPORATE_QSA` | Art. 7º, § 3º e 4º LGPD | Dados cadastrais corporativos públicos. Preserva máscara de CPF de sócios e anonimiza para BI. | RFB CNPJ, RFB Simples, PNCP, DataJud |
| `PUBLIC_AGENT_PEP` | Art. 7º, § 3º e 4º LGPD | Agentes públicos e autoridades. Uso estrito para KYC/Compliance com trilha de auditoria. | CGU PEP, CGU CEIS/CNEP, CNJ Justiça Aberta |
| `ANONYMIZED_MICRODATA` | Art. 12 LGPD / Art. 14 LGPD | Microdados censitários, de saúde ou educacionais. Vedado cruzamento reidentificador. | IBGE Censo, DataSUS SIH, INEP Censo Escolar |

---

## 7. Instruções de Manutenção & Verificação de Integridade

O arquivo `CATALOG.md` é gerado de forma **100% determinística** a partir de `catalog/sources_registry.yaml`.

```powershell
# 1. Verificar se CATALOG.md está em sincronia com o sources_registry.yaml (exit 0/1)
python scripts/render_catalog.py --check

# 2. Regenerar CATALOG.md após adicionar ou atualizar fontes no sources_registry.yaml
python scripts/render_catalog.py
```
