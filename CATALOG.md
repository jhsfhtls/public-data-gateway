<div align="center">
  <h2>Catálogo Mestre de Fontes Públicas & Registros Federativos Brasileiros</h2>
  <p><i>Mapeamento Unificado de 80 Fontes Governamentais nos 13 Macrosegmentos, Ondas 1 a 6 e Chaves Canônicas</i></p>
  <p><b>Versão:</b> 1.0.0 &nbsp;|&nbsp; <b>Última Atualização:</b> 2026-11-07 &nbsp;|&nbsp; <b>Padrões:</b> ADR-001 • ADR-002 • ADR-003 • ODCS v3.1 • LGPD</p>
  <hr/>
</div>
<br/>

# Catálogo Mestre de Fontes Públicas Governamentais (80 Fontes)

Este catálogo consolida formalmente as **80 bases de dados públicas e APIs governamentais do Brasil** integradas ao `public-data-gateway`. As fontes abrangem a totalidade dos **13 macrosegmentos estruturais federativos**, organizadas em **6 Ondas Estratégicas de Implementação** ordenadas por priorização ponderada `RICE`, garantindo integridade cadastral canônica (`ADR-002`), observabilidade de 5 pilares (`ODCS v3.1`) e conformidade com a LGPD (`ADR-003`, Lei nº 13.709/2018).

---

## 1. Visão Geral & Métricas Consolidadas

- **Total de Fontes Mapeadas:** 80 fontes governamentais homologadas
- **Total de Macrosegmentos:** 13 macrosegmentos federativos cobertos (100% de cobertura)
- **Ondas de Implementação:** 6 ondas estruturadas por retorno analítico e complexidade operacional
- **Chaves Canônicas Unificadas:** 8 chaves relacionais padronizadas em `STRING` (`id_municipio` 7D, `sigla_uf`, `cnpj_14`, `cpf_masked`, `cep_8`, `cnae_7`, `cbo_6`, `ncm_8`)
- **Perfis de Capacidade Analítica (Solution Packs):** 10 de 10 Perfis Modulares (`P01` a `P10`) suportados por dados oficiais

### 1.1. Distribuição por Modo de Acesso

| Modo de Acesso | Total Fontes | % do Catálogo | Perfil Operacional |
| :--- | :---: | :---: | :--- |
| `FEDERATED_API` | 33 | 41.2% | APIs REST / OData síncronas de borda e baixa latência (< 300 ms) |
| `LAKEHOUSE_BATCH` | 45 | 56.2% | Dumps colunares e volumosos em DuckDB / Parquet com streaming |
| `BIGQUERY_FEDERATED` | 2 | 2.5% | Consultas SQL em datalake federado (Datalake Federado SQL / MapBiomas) |

### 1.2. Distribuição por Classificação LGPD

| Tier LGPD | Total Fontes | % do Catálogo | Base Legal & Salvaguarda |
| :--- | :---: | :---: | :--- |
| `PUBLIC_NON_PERSONAL` | 53 | 66.2% | LAI / Dados abertos de pessoas jurídicas ou estatísticas sem PII |
| `PUBLIC_CORPORATE_QSA` | 12 | 15.0% | Art. 7º, §§ 3º e 4º LGPD: Cadastros empresariais com máscara CPF |
| `ANONYMIZED_MICRODATA` | 13 | 16.2% | Art. 12 LGPD: Microdados com bloqueio de reidentificação |
| `PUBLIC_AGENT_PEP` | 2 | 2.5% | Finalidade estrita KYC/Compliance com trilha de auditoria |

---

## 2. Matriz de Cobertura dos 13 Macrosegmentos

| ID Segmento | Macrosegmento Governamental | Total Fontes | Ondas Cobertas | Descrição Executiva | Status |
| :--- | :--- | :---: | :---: | :--- | :---: |
| `01_b2b_empresarial` | **Empresarial, Cadastral & Societário B2B** | 5 | `W2` | Dados de mais de 55 milhões de CNPJs (matrizes, filiais, sócios QSA, capital social, CNAEs, situação cadastral, optantes pelo Simples Nacional e MEI). | 🟢 Ativo |
| `02_economico_macrofinanceiro` | **Econômico, Monetário & Macrofinanceiro** | 13 | `W1, W2, W3` | Séries macroeconômicas oficiais (IPCA, Selic, IGP-M), balanços contábeis trimestrais de instituições financeiras (IFData), cotações diárias de moedas (PTAX) e dados contábeis da CVM. | 🟢 Ativo |
| `03_demografico_censitario` | **Demográfico, Censitário & Domiciliar** | 3 | `W4` | Censo Demográfico 2022 em nível de 450.000 setores censitários, características de domicílios, PNAD Contínua (renda e trabalho) e PIB municipal do IBGE. | 🟢 Ativo |
| `04_trabalho_renda` | **Trabalho, Emprego & Renda** | 4 | `W2, W4` | Fluxos mensais de contratações e demissões do Novo CAGED (salários de admissão por CBO e setor) e estoque de empregos da RAIS. | 🟢 Ativo |
| `05_geografico_logistica` | **Geográfico, Logístico & Endereçamento** | 7 | `W1, W2, W4` | 111+ milhões de coordenadas pontuais do CNEFE/IBGE, limites territoriais vetoriais, base de logradouros/CEPs e antenas de telecomunicação da Anatel. | 🟢 Ativo |
| `06_saude_publica` | **Saúde Pública & Epidemiologia** | 7 | `W2, W5` | Registros de óbitos (SIM), nascimentos (SINASC), internações hospitalares (SIH/SUS), estabelecimentos de saúde (CNES) e preços regulados de medicamentos (CMED/ANVISA). | 🟢 Ativo |
| `07_educacao_capital_humano` | **Educação & Capital Humano** | 5 | `W5` | Infraestrutura de escolas e matrículas (Censo Escolar), universidades e cursos (Censo Superior), notas por escola (IDEB) e microdados do ENEM. | 🟢 Ativo |
| `08_judicial_compliance` | **Judicial, Integridade & Compliance** | 10 | `W2` | Metadados de processos de todos os 90+ tribunais do Brasil (CNJ DataJud), cadastros de inidôneos e punidos (CEIS/CNEP/CEPIM), autoridades (PEP) e certidões negativas (CNDT). | 🟢 Ativo |
| `09_comercio_exterior` | **Comércio Exterior & Balança Comercial** | 1 | `W3` | Exportações e importações detalhadas por NCM, valor FOB, peso líquido, modal de transporte e município do exportador (Comex Stat). | 🟢 Ativo |
| `10_compras_publicas` | **Compras Públicas, Licitações & Contratos** | 3 | `W2, W3` | Repositório unificado da Lei nº 14.133/2021 (PNCP) com editais, atas e contratos de todos os entes federados e catálogo unificado CATMAT/CATSER. | 🟢 Ativo |
| `11_agronegocio_meio_ambiente` | **Agronegócio, Terra & Meio Ambiente** | 16 | `W2, W6` | 7+ milhões de imóveis rurais com polígonos de reserva legal e APP (SICAR/CAR), desmatamento e alertas por satélite (INPE DETER/PRODES), uso da terra (MapBiomas) e zoneamento climático (ZARC). | 🟢 Ativo |
| `12_imobiliario_cartorial` | **Imobiliário, Notarial & Cartorial** | 3 | `W5` | Cadastro nacional de cartórios e produtividade (CNJ Justiça Aberta), transações registradas (ONR) e cadastros imobiliários municipais (IPTU/ITBI). | 🟢 Ativo |
| `13_metaportais_agregadores` | **Metaportais & Hubs Agregadores** | 3 | `W1, W6` | Plataformas de facilitação técnica que unificam e padronizam centenas de fontes em protocolos únicos (Datalake Federado SQL, BrasilAPI, Dados.gov.br). | 🟢 Ativo |

---

## 3. Roadmap por Ondas de Implementação (Waves 1 a 6)

| Onda | Foco Estratégico & Justificativa | Total Fontes | Intervalo RICE | Modos de Acesso | Fontes Críticas | Status |
| :---: | :--- | :---: | :---: | :--- | :--- | :---: |
| **Wave 1** | APIs de Borda & Séries Macroeconômicas (Baixa Latência < 300ms) | 4 | 820–950 | `FEDERATED_API` | `ibge_localidades`, `brasilapi_core`, `bcb_sgs` (+1) | 🟢 100% Homologada |
| **Wave 2** | B2B, Compras Públicas, Integridade & Compliance Cadastral | 36 | 610–920 | `FEDERATED_API, LAKEHOUSE_BATCH` | `pgfn_divida_ativa`, `ibama_fiscalizacao`, `stn_capag` (+33) | 🟢 100% Homologada |
| **Wave 3** | Mercado de Capitais, Balanços Contábeis & Finanças Públicas | 9 | 610–910 | `FEDERATED_API, LAKEHOUSE_BATCH` | `bcb_estban`, `stn_siconfi`, `bcb_agencia` (+6) | 🟢 100% Homologada |
| **Wave 4** | Censo Demográfico, Trabalho, Renda, Logística & Telecomunicações | 10 | 500–590 | `FEDERATED_API, LAKEHOUSE_BATCH` | `ibge_sidra`, `ibge_pib_municipal`, `ibge_censo2022` (+7) | 🟢 100% Homologada |
| **Wave 5** | Saúde Pública, Educação, Capital Humano & Serviços Notariais | 14 | 410–480 | `FEDERATED_API, LAKEHOUSE_BATCH` | `datasus_cnes`, `datasus_sih`, `inep_censo_escolar` (+11) | 🟢 100% Homologada |
| **Wave 6** | Meio Ambiente, Agronegócio, Monitoramento Satelital & Metaportais | 7 | 340–390 | `BIGQUERY_FEDERATED, FEDERATED_API, LAKEHOUSE_BATCH` | `datalake_federado_bq`, `inpe_terrabrasilis`, `ana_hidroweb_telemetria` (+4) | 🟢 100% Homologada |

---

## 4. Tabela Completa do Catálogo de Fontes Governamentais

Ordenada por **Onda de Implementação (`roadmap_wave`)**, **Score RICE decrescente** e **Identificador da Fonte**:

| ID Fonte | Dataset / Órgão Oficial | Macrosegmento | Onda | RICE | Modo / Protocolo | Chaves Canônicas | Tier LGPD | Solution Packs | Status |
| :--- | :--- | :--- | :---: | :---: | :--- | :--- | :--- | :--- | :---: |
| `ibge_localidades` | [IBGE Localidades & Malha Territorial](https://servicodados.ibge.gov.br/api/docs/localidades)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Geográfico, Logístico | **W1** | 950 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` | `HOMOLOGATED` |
| `brasilapi_core` | [BrasilAPI: CEPs, FIPE, Bancos, DDD & CNPJ de Borda](https://brasilapi.com.br/docs)<br/>*Comunidade Open Source BrasilAPI* | Metaportais | **W1** | 880 | `FEDERATED_API`<br/>`REST_JSON` | `cep_8`<br/>`cnpj_14`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P04` | `HOMOLOGATED` |
| `bcb_sgs` | [Banco Central: Sistema Gerenciador de Séries Temporais (SGS)](https://dadosabertos.bcb.gov.br/dataset/series-temporais)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W1** | 860 | `FEDERATED_API`<br/>`REST_JSON` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P05` | `HOMOLOGATED` |
| `bcb_olinda_ptax` | [Banco Central Olinda: Cotações PTAX Moedas Diárias](https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/swagger-ui3)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W1** | 820 | `FEDERATED_API`<br/>`ODATA_V4` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `HOMOLOGATED` |
| `pgfn_divida_ativa` | [PGFN: Créditos Inscritos em Dívida Ativa da União e do FGTS (Dados Abertos)](https://dadosabertos.pgfn.gov.br/)<br/>*Procuradoria-Geral da Fazenda Nacional (PGFN / Ministério da Fazenda)* | Judicial, Integridade | **W2** | 920 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`cpf_masked`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P04` `P06` `P07` `P08` `P10` | `HOMOLOGATED` |
| `ibama_fiscalizacao` | [IBAMA: Autos de Infração, Termos de Embargo & Fiscalização Ambiental (SIFISC)](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-auto-de-infracao)<br/>*Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renováveis (IBAMA)* | Agronegócio, Terra | **W2** | 915 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`cpf_masked`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P03` `P04` `P06` `P08` `P09` | `HOMOLOGATED` |
| `stn_capag` | [STN: Capacidade de Pagamento (CAPAG) Subnacional & Índice FIRJAN de Gestão Fiscal (IFGF)](https://www.tesourotransparente.gov.br/temas/estados-e-municipios/capacidade-de-pagamento-capag)<br/>*Secretaria do Tesouro Nacional (STN / Ministério da Fazenda) & FIRJAN* | Econômico, Monetário | **W2** | 915 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P04` `P05` `P06` `P08` `P10` | `HOMOLOGATED` |
| `anm_mineracao` | [ANM Mineração (SIGMINE & CFEM): Processos Minerários, Lavra & Royalties](https://www.gov.br/anm/pt-br/acesso-a-informacao/dados-abertos)<br/>*Agência Nacional de Mineração (ANM)* | Agronegócio, Terra | **W2** | 910 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P04` `P08` | `HOMOLOGATED` |
| `bcb_sicor` | [BACEN: Sistema de Operações do Crédito Rural e do Proagro (SICOR)](https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata/)<br/>*Banco Central do Brasil (BACEN)* | Agronegócio, Terra | **W2** | 910 | `FEDERATED_API`<br/>`ODATA_V4` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` `P06` `P08` `P09` | `HOMOLOGATED` |
| `mdr_snis` | [MDR/MCidades SNIS & SINISA: Saneamento Básico (Água, Esgoto & Resíduos)](https://www.gov.br/cidades/pt-br/acesso-a-informacao/acoes-e-programas/saneamento/snis)<br/>*Ministério das Cidades (SNIS / SINISA)* | Agronegócio, Terra | **W2** | 910 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` `P06` `P08` | `HOMOLOGATED` |
| `aneel_geracao_distribuida` | [ANEEL Geração Distribuída: Empreendimentos MMGD & Transição Solar](https://dadosabertos.aneel.gov.br/dataset/relacao-de-empreendimentos-de-geracao-distribuida)<br/>*Agência Nacional de Energia Elétrica (ANEEL)* | Agronegócio, Terra | **W2** | 905 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P05` `P06` `P08` `P09` | `HOMOLOGATED` |
| `cnj_improbidade` | [CNJ: Cadastro Nacional de Condenações por Ato de Improbidade Administrativa (CNCIAI)](https://www.cnj.jus.br/improbidade_adm/consultar_requerido.php)<br/>*Conselho Nacional de Justiça (CNJ)* | Judicial, Integridade | **W2** | 905 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_AGENT_PEP` | `P01` `P02` `P04` `P06` `P07` `P08` `P10` | `HOMOLOGATED` |
| `ibge_agro_municipal` | [IBGE: Produção Agrícola, Pecuária & Silvicultura Municipal (PAM / PPM / PEVS)](https://apisidra.ibge.gov.br/)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Agronegócio, Terra | **W2** | 905 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` `P06` `P09` | `HOMOLOGATED` |
| `cgu_transparencia_gastos` | [CGU Portal da Transparência: Gastos Diretos, Transferências & Convênios](https://portaldatransparencia.gov.br/despesas)<br/>*Controladoria-Geral da União (CGU) / Portal da Transparência do Governo Federal* | Judicial, Integridade | **W2** | 900 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` `P04` `P07` | `HOMOLOGATED` |
| `rfb_cafir` | [Receita Federal: Cadastro de Imóveis Rurais (CAFIR / CIB / CNIR)](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/tributos/itr)<br/>*Secretaria Especial da Receita Federal do Brasil (RFB) / INCRA* | Agronegócio, Terra | **W2** | 900 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`cpf_masked`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P03` `P04` `P06` `P08` `P09` | `HOMOLOGATED` |
| `rfb_cno` | [Receita Federal: Cadastro Nacional de Obras (CNO — Construção Civil & Infraestrutura)](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/cno)<br/>*Secretaria Especial da Receita Federal do Brasil (RFB)* | Empresarial, Cadastral | **W2** | 900 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`cpf_masked`<br/>`id_municipio`<br/>`sigla_uf`<br/>`cnae_7`<br/>`cep_8` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P03` `P04` `P06` `P08` `P09` `P10` | `HOMOLOGATED` |
| `bndes_operacoes` | [BNDES Transparência: Operações Diretas, Indiretas & Fomento Municipal](https://dadosabertos.bndes.gov.br/)<br/>*Banco Nacional de Desenvolvimento Econômico e Social (BNDES)* | Econômico, Monetário | **W2** | 895 | `LAKEHOUSE_BATCH`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` | `HOMOLOGATED` |
| `inpe_queimadas_seeg` | [INPE Queimadas & SEEG: Focos de Calor & Emissões de Gases de Efeito Estufa](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/)<br/>*Instituto Nacional de Pesquisas Espaciais (INPE) / Observatório do Clima (SEEG)* | Agronegócio, Terra | **W2** | 895 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P06` `P08` `P09` | `HOMOLOGATED` |
| `sinesp_seguranca` | [SINESP / FBSP Segurança Pública: Ocorrências Criminais, MVI & Risco Patrimonial](https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/dados-nacionais-1/base-de-dados-e-notas-metodologicas-dos-gestores-estaduais-sinesp-vde-2022-e-2023)<br/>*Ministério da Justiça e Segurança Pública (SENASP / SINESP) & Fórum Brasileiro de Segurança Pública (FBSP)* | Judicial, Integridade | **W2** | 895 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` `P04` `P05` `P07` | `HOMOLOGATED` |
| `anp_combustiveis` | [ANP Combustíveis: Levantamento de Preços, Margens & Biocombustíveis](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/levantamento-de-precos-de-combustiveis-ultimas-semanas-pesquisadas)<br/>*Agência Nacional do Petróleo, Gás Natural e Biocombustíveis (ANP)* | Agronegócio, Terra | **W2** | 890 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P05` `P06` `P08` `P09` | `HOMOLOGATED` |
| `querido_diario_okbr` | [Querido Diário (OKBR): Diários Oficiais Municipais, Licitações & Atos](https://queridodiario.ok.org.br/)<br/>*Open Knowledge Brasil (OKBR) / Projeto Querido Diário* | Compras Públicas, Licitações | **W2** | 890 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` `P04` `P07` | `HOMOLOGATED` |
| `ans_saude_suplementar` | [ANS Saúde Suplementar: Beneficiários, Operadoras, IDSS & Sinistralidade](https://dadosabertos.ans.gov.br/FTP/PDA/)<br/>*Agência Nacional de Saúde Suplementar (ANS)* | Saúde Pública | **W2** | 885 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P03` `P05` `P06` | `HOMOLOGATED` |
| `senatran_frota` | [SENATRAN Frota de Veículos: RENAVAM, Mobilidade & Eletrificação](https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/estatisticas-frota-de-veiculos-senatran)<br/>*Secretaria Nacional de Trânsito (SENATRAN / Ministério dos Transportes)* | Geográfico, Logístico | **W2** | 885 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P05` `P07` | `HOMOLOGATED` |
| `mme_ons_energia` | [MME / ONS Energia: Sistema Interligado Nacional, Balanço & Segurança Energética](https://dados.ons.org.br)<br/>*Operador Nacional do Sistema Elétrico (ONS) & Ministério de Minas e Energia (MME)* | Agronegócio, Meio Ambiente | **W2** | 880 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P02` `P05` `P06` `P08` | `HOMOLOGATED` |
| `susep_seguros` | [SUSEP Mercado Segurador (SES): Prêmios Emitidos, Sinistralidade, Previdência & Solvência](https://www2.susep.gov.br/menuestatistica/SES/principal.aspx)<br/>*Superintendência de Seguros Privados (SUSEP / Ministério da Fazenda)* | Econômico, Monetário | **W2** | 880 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` | `HOMOLOGATED` |
| `tse_eleicoes` | [TSE Dados Abertos Eleitorais: Candidaturas, Votação & Prestação de Contas](https://dadosabertos.tse.jus.br/)<br/>*Tribunal Superior Eleitoral (TSE)* | Judicial, Integridade | **W2** | 880 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` `P04` `P07` | `HOMOLOGATED` |
| `inpi_propriedade_intelectual` | [INPI Propriedade Intelectual (RPI): Patentes, Marcas, Desenhos Industriais & Software](https://www.gov.br/inpi/pt-br/acesso-a-informacao/dados-abertos)<br/>*Instituto Nacional da Propriedade Industrial (INPI / MDIC)* | B2B, Empresas, Sócios | **W2** | 875 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P07` | `HOMOLOGATED` |
| `mps_inss_beneficios` | [MPS / INSS Benefícios: Previdência Social, BPC/LOAS & Arrecadação](https://dadosabertos.inss.gov.br)<br/>*Instituto Nacional do Seguro Social (INSS) & Ministério da Previdência Social (MPS)* | Trabalho, Emprego | **W2** | 875 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P06` | `HOMOLOGATED` |
| `cvm_mercado_extra` | [CVM Mercado de Capitais Extra: Ofertas RCVM 160, Administradores & Agentes](https://dados.cvm.gov.br/)<br/>*Comissão de Valores Mobiliários (CVM)* | Econômico, Monetário | **W2** | 870 | `LAKEHOUSE_BATCH`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` | `HOMOLOGATED` |
| `sedec_desastres` | [SEDEC / S2ID & CEMADEN: Desastres Naturais, Decretações & Alertas](https://www.gov.br/mdr/pt-br/assuntos/protecao-e-defesa-civil)<br/>*Secretaria Nacional de Proteção e Defesa Civil (SEDEC / MIDR) & CEMADEN (MCTI)* | Agronegócio, Meio Ambiente | **W2** | 870 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P02` `P04` `P05` `P07` | `HOMOLOGATED` |
| `prf_acidentes` | [PRF Dados Abertos: Sinistros Rodoviários, Letalidade & Corredores Logísticos](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf)<br/>*Polícia Rodoviária Federal (PRF / Ministério da Justiça e Segurança Pública)* | Geográfico, Logístico | **W2** | 865 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P05` `P07` | `HOMOLOGATED` |
| `cgu_servidores_federais` | [CGU / SIAPE Servidores Federais: Funcionalismo, Folha & Acúmulos](https://portaldatransparencia.gov.br/servidores)<br/>*Controladoria-Geral da União (CGU) & Ministério da Gestão e da Inovação (MGI / SIAPE)* | Judicial, Integridade | **W2** | 860 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P03` `P04` `P07` | `HOMOLOGATED` |
| `rfb_cnpj_lakehouse` | [Receita Federal: Cadastro Nacional da Pessoa Jurídica (CNPJ & QSA)](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)<br/>*Receita Federal do Brasil* | Empresarial, Cadastral | **W2** | 840 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`cnae_7`<br/>`id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P04` `P06` | `HOMOLOGATED` |
| `pncp_contratacoes` | [PNCP: Portal Nacional de Contratações Públicas (Lei 14.133)](https://pncp.gov.br/api/consulta/swagger-ui/index.html)<br/>*Governo Federal / PNCP* | Compras Públicas, Licitações | **W2** | 790 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P04` | `HOMOLOGATED` |
| `cnj_datajud` | [CNJ: Base Nacional do Poder Judiciário (DataJud)](https://datajud-wiki.cnj.jus.br/)<br/>*Conselho Nacional de Justiça (CNJ)* | Judicial, Integridade | **W2** | 780 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` `P07` | `HOMOLOGATED` |
| `cgu_ceis_cnep` | [CGU: Cadastros de Empresas Inidôneas, Suspensas e Punidas (CEIS/CNEP)](https://portaldatransparencia.gov.br/swagger-ui.html)<br/>*Controladoria-Geral da União (CGU)* | Judicial, Integridade | **W2** | 760 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked` | `PUBLIC_CORPORATE_QSA` | `P01` `P06` `P07` | `HOMOLOGATED` |
| `cgu_pep` | [CGU: Cadastro de Pessoas Expostas Politicamente (PEP)](https://portaldatransparencia.gov.br/swagger-ui.html)<br/>*Controladoria-Geral da União (CGU)* | Judicial, Integridade | **W2** | 750 | `FEDERATED_API`<br/>`REST_JSON` | `cpf_masked` | `PUBLIC_AGENT_PEP` | `P01` `P06` `P07` | `HOMOLOGATED` |
| `rfb_simples_mei` | [Receita Federal: Opção pelo Simples Nacional & MEI](https://dados.gov.br/dados/conjuntos-dados/simples-nacional)<br/>*Receita Federal do Brasil* | Empresarial, Cadastral | **W2** | 720 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14` | `PUBLIC_CORPORATE_QSA` | `P01` `P02` | `HOMOLOGATED` |
| `tst_cndt` | [TST: Certidão Negativa de Débitos Trabalhistas (CNDT)](https://cndt-certidao.tst.jus.br/)<br/>*Tribunal Superior do Trabalho (TST)* | Judicial, Integridade | **W2** | 700 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14`<br/>`cpf_masked` | `PUBLIC_CORPORATE_QSA` | `P01` `P07` | `HOMOLOGATED` |
| `suframa_pim_zfm` | [SUFRAMA: Cadastro de Empresas Incentivadas do PIM / ZFM](https://www.gov.br/suframa/pt-br)<br/>*Superintendência da Zona Franca de Manaus (SUFRAMA)* | Empresarial, Cadastral | **W2** | 610 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `HOMOLOGATED` |
| `bcb_estban` | [Banco Central: Estatística Bancária Mensal por Município (ESTBAN COSIF)](https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W3** | 910 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf`<br/>`cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` `P06` `P08` `P10` | `HOMOLOGATED` |
| `stn_siconfi` | [Secretaria do Tesouro Nacional: SICONFI & Finbra (Contabilidade e Gestão Fiscal)](https://apidatalake.tesouro.gov.br/docs/siconfi/)<br/>*Secretaria do Tesouro Nacional (STN)* | Econômico, Monetário | **W3** | 910 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf`<br/>`cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P04` `P05` `P06` `P07` `P08` `P10` | `HOMOLOGATED` |
| `bcb_agencia` | [Banco Central: Cadastro Nacional de Agências Bancárias e Postos de Atendimento (Olinda OData)](https://olinda.bcb.gov.br/olinda/servico/Informes_Agencias/versao/v1/odata/Agencias?$top=1&$format=json)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W3** | 905 | `FEDERATED_API`<br/>`ODATA_V4` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P05` `P06` `P08` `P10` | `HOMOLOGATED` |
| `cvm_dfp_itr` | [CVM: Demonstrações Financeiras Padronizadas (DFP / ITR)](https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/)<br/>*Comissão de Valores Mobiliários (CVM)* | Econômico, Monetário | **W3** | 680 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14` | `PUBLIC_CORPORATE_QSA` | `P01` `P05` | `HOMOLOGATED` |
| `mdic_comexstat` | [MDIC: Comex Stat Exportações e Importações Detalhadas](https://www.gov.br/produtividade-e-comercio-exterior/pt-br/assuntos/comercio-exterior/estatisticas)<br/>*Ministério do Desenvolvimento, Indústria e Comércio Exterior (MDIC)* | Comércio Exterior | **W3** | 660 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `ncm_8`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |
| `cvm_fundos` | [CVM: Informe Diário de Fundos de Investimento](https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/)<br/>*Comissão de Valores Mobiliários (CVM)* | Econômico, Monetário | **W3** | 650 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `HOMOLOGATED` |
| `bcb_ifdata` | [Banco Central: Informações Financeiras das IFs (IF.data)](https://www3.bcb.gov.br/ifdata/)<br/>*Banco Central do Brasil (BACEN)* | Econômico, Monetário | **W3** | 640 | `FEDERATED_API`<br/>`ODATA_V4` | `cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `HOMOLOGATED` |
| `stn_tesouro_transparente` | [Secretaria do Tesouro Nacional: SADIPEM & Dívida Pública](https://www.tesourotransparente.gov.br/)<br/>*Secretaria do Tesouro Nacional (STN)* | Econômico, Monetário | **W3** | 630 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P05` | `HOMOLOGATED` |
| `comprasgov_catmat` | [Compras.gov.br: Catálogo Unificado de Materiais e Serviços (CATMAT/CATSER)](https://dados.gov.br/dados/conjuntos-dados/catalogo-de-materiais-e-servicos---catmat-catser)<br/>*Ministério da Gestão e Inovação em Serviços Públicos (MGI)* | Compras Públicas, Licitações | **W3** | 610 | `FEDERATED_API`<br/>`REST_JSON` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P04` | `HOMOLOGATED` |
| `ibge_sidra` | [IBGE: Sistema IBGE de Recuperação Automática (SIDRA)](https://apisidra.ibge.gov.br/)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Demográfico, Censitário | **W4** | 590 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |
| `ibge_pib_municipal` | [IBGE: Produto Interno Bruto dos Municípios](https://www.ibge.gov.br/estatisticas/economicas/contas-nacionais/9088-produto-interno-bruto-dos-municipios.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Demográfico, Censitário | **W4** | 580 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |
| `ibge_censo2022` | [IBGE: Microdados do Censo Demográfico 2022](https://censo2022.ibge.gov.br/)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Demográfico, Censitário | **W4** | 570 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` `P03` | `HOMOLOGATED` |
| `mte_novo_caged` | [MTE: Microdados do Novo CAGED](http://pdet.mte.gov.br/novo-caged)<br/>*Ministério do Trabalho e Emprego (MTE)* | Trabalho, Emprego | **W4** | 570 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cbo_6`<br/>`cnae_7`<br/>`id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `ibge_cnefe` | [IBGE: Cadastro Nacional de Endereços para Fins Estatísticos (CNEFE)](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Geográfico, Logístico | **W4** | 560 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cep_8`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `HOMOLOGATED` |
| `ibge_pnad_continua` | [IBGE: Pesquisa Nacional por Amostra de Domicílios Contínua (PNAD Contínua)](https://www.ibge.gov.br/estatisticas/sociais/trabalho/9171-pesquisa-nacional-por-amostra-de-domicilios-continua-mensal.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Trabalho, Emprego | **W4** | 560 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `mte_rais` | [MTE: Relação Anual de Informações Sociais (RAIS)](http://pdet.mte.gov.br/rais)<br/>*Ministério do Trabalho e Emprego (MTE)* | Trabalho, Emprego | **W4** | 550 | `LAKEHOUSE_BATCH`<br/>`FTP_7Z_CSV` | `cbo_6`<br/>`cnae_7`<br/>`id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `ibge_malhas` | [IBGE: Malhas Territoriais Vetoriais (Municípios e UFs)](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais.html)<br/>*Instituto Brasileiro de Geografia e Estatística (IBGE)* | Geográfico, Logístico | **W4** | 540 | `LAKEHOUSE_BATCH`<br/>`OGC_WFS_SHP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `HOMOLOGATED` |
| `anatel_mosaico` | [ANATEL: Sistema Mosaico de Estações e Cobertura de Telecomunicações](https://sistemas.anatel.gov.br/mosaico/)<br/>*Agência Nacional de Telecomunicações (ANATEL)* | Geográfico, Logístico | **W4** | 510 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `HOMOLOGATED` |
| `antt_malha_transporte` | [ANTT: Malha de Transporte Rodoviário e Ferroviário de Cargas](https://dados.antt.gov.br/)<br/>*Agência Nacional de Transportes Terrestres (ANTT)* | Geográfico, Logístico | **W4** | 500 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` `P09` | `HOMOLOGATED` |
| `datasus_cnes` | [DataSUS: Cadastro Nacional de Estabelecimentos de Saúde (CNES)](https://cnes.datasus.gov.br/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 480 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |
| `datasus_sih` | [DataSUS: Sistema de Informações Hospitalares (SIH/SUS)](https://datasus.saude.gov.br/transferencia-de-arquivos/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 470 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `inep_censo_escolar` | [INEP: Microdados do Censo Escolar da Educação Básica](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 470 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf`<br/>`cep_8` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `cnj_justica_aberta` | [CNJ: Justiça Aberta (Cadastro e Produtividade de Cartórios)](https://www.cnj.jus.br/corregedoria/justica_aberta/)<br/>*Conselho Nacional de Justiça (CNJ)* | Imobiliário, Notarial | **W5** | 460 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_CORPORATE_QSA` | `P01` `P04` | `HOMOLOGATED` |
| `datasus_sim` | [DataSUS: Sistema de Informações sobre Mortalidade (SIM)](https://datasus.saude.gov.br/transferencia-de-arquivos/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 460 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `inep_censo_superior` | [INEP: Microdados do Censo da Educação Superior](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 460 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `cnpj_14`<br/>`id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `anvisa_cmed` | [ANVISA: Câmara de Regulação do Mercado de Medicamentos (CMED)](https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos)<br/>*Agência Nacional de Vigilância Sanitária (ANVISA)* | Saúde Pública | **W5** | 450 | `FEDERATED_API`<br/>`REST_JSON` | `cnpj_14` | `PUBLIC_NON_PERSONAL` | `P01` `P04` | `HOMOLOGATED` |
| `datasus_sinasc` | [DataSUS: Sistema de Informações sobre Nascidos Vivos (SINASC)](https://datasus.saude.gov.br/transferencia-de-arquivos/)<br/>*Ministério da Saúde / DataSUS* | Saúde Pública | **W5** | 450 | `LAKEHOUSE_BATCH`<br/>`FTP_DBC` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `inep_ideb` | [INEP: Índice de Desenvolvimento da Educação Básica (IDEB)](https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/ideb)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 450 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |
| `onr_registro_imoveis` | [ONR: Sistema de Registro Eletrônico de Imóveis (SREI)](https://onr.org.br/)<br/>*Operador Nacional do Registro de Imóveis Eletrônico (ONR)* | Imobiliário, Notarial | **W5** | 450 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `HOMOLOGATED` |
| `inep_enem` | [INEP: Microdados do Exame Nacional do Ensino Médio (ENEM)](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem)<br/>*Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)* | Educação | **W5** | 440 | `LAKEHOUSE_BATCH`<br/>`HTTP_BULK_ZIP` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `opendatasus_vigilancia` | [OpenDataSUS: Notificações de Síndromes e Vigilância Epidemiológica](https://opendatasus.saude.gov.br/)<br/>*Ministério da Saúde* | Saúde Pública | **W5** | 440 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `ANONYMIZED_MICRODATA` | `P01` `P02` | `HOMOLOGATED` |
| `capitais_iptu_itbi` | [Capitais: Cadastros Imobiliários Municipais (IPTU / ITBI)](https://dados.gov.br/)<br/>*Prefeituras Municipais das Capitais* | Imobiliário, Notarial | **W5** | 430 | `FEDERATED_API`<br/>`REST_JSON` | `cep_8`<br/>`id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P03` | `HOMOLOGATED` |
| `capes_sucupira` | [CAPES: Plataforma Sucupira (Pós-Graduação Stricto Sensu)](https://dadosabertos.capes.gov.br/)<br/>*Coordenação de Aperfeiçoamento de Pessoal de Nível Superior (CAPES)* | Educação | **W5** | 410 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |
| `datalake_federado_bq` | [PDG: Datalake Público Federado & Catálogo Nacional SQL (DuckDB / BigQuery)](https://www.gov.br/conecta/catalogo/)<br/>*Infraestrutura Nacional de Dados (IND / GOV.BR) & PDG* | Metaportais | **W6** | 390 | `BIGQUERY_FEDERATED`<br/>`BIGQUERY_SQL` | `id_municipio`<br/>`sigla_uf`<br/>`cnpj_14`<br/>`cep_8` | `PUBLIC_NON_PERSONAL` | `P01` `P02` `P03` `P04` `P05` `P06` | `HOMOLOGATED` |
| `inpe_terrabrasilis` | [INPE: Plataforma TerraBrasilis (DETER & PRODES)](http://terrabrasilis.dpi.inpe.br/)<br/>*Instituto Nacional de Pesquisas Espaciais (INPE)* | Agronegócio, Terra | **W6** | 390 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P06` `P09` | `HOMOLOGATED` |
| `ana_hidroweb_telemetria` | [ANA: Rede Hidrometeorológica Nacional (HidroWeb)](https://www.snirh.gov.br/hidroweb/)<br/>*Agência Nacional de Águas e Saneamento Básico (ANA)* | Agronegócio, Terra | **W6** | 380 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `HOMOLOGATED` |
| `mapbiomas_cobertura` | [MapBiomas: Cobertura e Uso da Terra no Brasil (1985-2024)](https://mapbiomas.org/)<br/>*Projeto MapBiomas* | Agronegócio, Terra | **W6** | 370 | `BIGQUERY_FEDERATED`<br/>`BIGQUERY_SQL` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P06` `P09` | `HOMOLOGATED` |
| `sfb_sicar` | [SFB: Sistema Nacional de Cadastro Ambiental Rural (SICAR)](https://www.car.gov.br/)<br/>*Serviço Florestal Brasileiro (SFB) / MMA* | Agronegócio, Terra | **W6** | 360 | `LAKEHOUSE_BATCH`<br/>`OGC_WFS_SHP` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P06` `P09` | `HOMOLOGATED` |
| `mapa_zarc` | [MAPA: Zoneamento Agrícola de Risco Climático (ZARC)](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programas-de-gestao-de-risco/zoneamento-agricola-de-risco-climatico)<br/>*Ministério da Agricultura e Pecuária (MAPA) / Embrapa* | Agronegócio, Terra | **W6** | 350 | `FEDERATED_API`<br/>`REST_JSON` | `id_municipio`<br/>`sigla_uf` | `PUBLIC_NON_PERSONAL` | `P01` `P09` | `HOMOLOGATED` |
| `dadosgov_ckan` | [Dados.gov.br: Portal Brasileiro de Dados Abertos (Catálogo CKAN)](https://dados.gov.br/dados/conjuntos-dados)<br/>*Governo Federal (Dados.gov.br)* | Metaportais | **W6** | 340 | `FEDERATED_API`<br/>`REST_JSON` | *(macro)* | `PUBLIC_NON_PERSONAL` | `P01` `P02` | `HOMOLOGATED` |

---

## 5. Dicionário de Chaves Canônicas & Tipagem Estrita (`ADR-002`)

Conforme estabelecido no **ADR-002**, todas as chaves identificadoras trafegam **compulsoriamente como `STRING`**, impedindo a perda de zeros à esquerda provocada por tipagens numéricas inadequadas:

| Chave Canônica | Tipo Estrito | Padrão / Máscara Canônica | Algoritmo de Validação | Referência Arquitetural & Legal |
| :--- | :--- | :--- | :--- | :--- |
| `id_municipio` | `STRING(7)` | IBGE 7 Dígitos (ex: '3550308') | Módulo 10 com pesos [1, 2] | ADR-002, IBGE |
| `sigla_uf` | `STRING(2)` | UF Oficial 2 Letras maiúsculas (ex: 'SP') | Tabela IBGE das 27 UFs | ADR-002, IBGE |
| `cnpj_14` | `STRING(14)` | CNPJ Canônico Zfill 14D (ex: '00000000000191') | Módulo 11 (2 dígitos verificadores) | ADR-002, ADR-003, RFB |
| `cpf_masked` | `STRING(14)` | Máscara Protegida (ex: '***.123.456-**') | Preservação compulsória Art. 7 LGPD | ADR-003, Art. 7 LGPD |
| `cep_8` | `STRING(8)` | CEP Canônico Zfill 8D (ex: '01310100') | DNE Correios / BrasilAPI | ADR-002, DNE / BrasilAPI |
| `cnae_7` | `STRING(7)` | CNAE Subclasse 2.0 (ex: '6201501') | Tabela CONCLA / IBGE | ADR-002, CONCLA / IBGE |
| `cbo_6` | `STRING(6)` | CBO-2002 Ocupação (ex: '212405') | Tabela MTE / CBO | ADR-002, MTE / CBO |
| `ncm_8` | `STRING(8)` | Nomenclatura Comum Mercosul (ex: '84713012') | Sistema Harmonizado MDIC | ADR-002, MDIC |

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

## 7. Catálogo Unificado — APIs Públicas & Globais (Analytics, KYC/KYB, Espacial & ML)

Complemento integrado ao catálogo mestre com **29 APIs públicas e globais curadas** (fonte: `catalog/public_apis_registry.yaml`), auditadas contra:

- **Conecta GOV.BR** (https://www.gov.br/conecta/catalogo/): Auditoria de APIs governamentais brasileiras disponiveis para integracao
- **Public APIs Directory** (https://github.com/public-apis/public-apis): Descoberta curada de APIs publicas globais para Analytics e Machine Learning

### Enriquecimento Cadastral, Geolocalizacao, KYC/KYB & Antifraude (`BLOCO_1_KYC_GEO`)

Gateways de fallback em cascata para CNPJ/CEP, geocodificacao, idade de dominio, LEI global e inteligencia de IP/ASN.

| API ID | Nome | Auth | RPM | FinOps | Packs | Método Gateway | Status |
| :--- | :--- | :--- | ---: | :--- | :--- | :--- | :--- |
| `minha_receita` | Minha Receita (CNPJ Aberto) | `NONE` | 60 | `FREE_UNLIMITED` | P04, P07 | `enrich_cnpj_cascade` | `HOMOLOGATED` |
| `cnpja_open` | CNPJa Open API | `NONE` | 5 | `FREE_TIER` | P04, P07 | `enrich_cnpj_cascade` | `HOMOLOGATED` |
| `receitaws` | ReceitaWS | `NONE` | 3 | `FREEMIUM` | P04, P07 | `enrich_cnpj_cascade` | `HOMOLOGATED` |
| `viacep` | ViaCEP | `NONE` | 120 | `FREE_UNLIMITED` | P03, P04 | `lookup_cep_cascade` | `HOMOLOGATED` |
| `opencep` | OpenCEP | `NONE` | 120 | `FREE_UNLIMITED` | P03, P04 | `lookup_cep_cascade` | `HOMOLOGATED` |
| `nominatim_osm` | Nominatim (OpenStreetMap) | `NONE` | 60 | `FREE_UNLIMITED` | P03, P09 | `geocode_nominatim` | `HOMOLOGATED` |
| `rdap_registro_br` | RDAP Registro.br | `NONE` | 60 | `FREE_UNLIMITED` | P04, P07 | `lookup_rdap_registro_br` | `HOMOLOGATED` |
| `gleif_lei` | GLEIF Global LEI API | `NONE` | 60 | `FREE_UNLIMITED` | P04, P07 | `lookup_gleif_lei` | `HOMOLOGATED` |
| `bgpview_ip_intel` | BGPView / IP-API | `NONE` | 45 | `FREE_TIER` | P04, P08 | `lookup_ip_intel` | `HOMOLOGATED` |
| `company_enrich_apollo_bdc` | CompanyEnrich / Apollo / BigDataCorp (Adaptador B2B Freemium) | `OPTIONAL_KEY` | 30 | `FREEMIUM` | P04, P06 | `lookup_b2b_domain_profile` | `HOMOLOGATED` |

### Inteligencia de Mercado, Regulatorias, Economia & Governo (`BLOCO_2_MERCADO_REGULATORIO`)

Cotacoes B3/FIIs, cambio em tempo real, OData IPEA, CKAN ANEEL/ANP, Legislativo (Camara/Senado) e series globais (World Bank/OMS).

| API ID | Nome | Auth | RPM | FinOps | Packs | Método Gateway | Status |
| :--- | :--- | :--- | ---: | :--- | :--- | :--- | :--- |
| `brapi_b3` | brapi (Cotacoes B3, Dividendos & FIIs) | `OPTIONAL_KEY` | 60 | `FREEMIUM` | P05, P01 | `fetch_brapi_quote` | `HOMOLOGATED` |
| `awesomeapi_fx` | AwesomeAPI (Cambio & Cripto) | `NONE` | 60 | `FREE_UNLIMITED` | P05 | `fetch_awesomeapi_rates` | `HOMOLOGATED` |
| `ipeadata_odata` | IPEA Data (OData v4) | `NONE` | 60 | `FREE_UNLIMITED` | P01, P05 | `fetch_ipeadata_series` | `HOMOLOGATED` |
| `aneel_anp_ckan` | ANEEL & ANP (CKAN DataStore) | `NONE` | 60 | `FREE_UNLIMITED` | P09, P01 | `query_ckan_datastore` | `HOMOLOGATED` |
| `camara_senado_dados_abertos` | Camara dos Deputados & Senado Federal | `NONE` | 60 | `FREE_UNLIMITED` | P07, P06 | `fetch_camara_deputados` | `HOMOLOGATED` |
| `world_bank_v2` | World Bank Indicators API v2 | `NONE` | 60 | `FREE_UNLIMITED` | P01, P05 | `fetch_world_bank_indicator` | `HOMOLOGATED` |
| `who_gho_odata` | Observatorio da OMS (WHO GHO) | `NONE` | 60 | `FREE_UNLIMITED` | P01, P06 | `fetch_who_gho_indicator` | `HOMOLOGATED` |

### Feature Engineering Espacial, Mobilidade, Clima & ML (`BLOCO_3_ESPACIAL_CLIMA_ML`)

POIs em raio espacial (Overpass/OSM), matriz de rotas viarias (OSRM), clima/radiacao (Open-Meteo, NASA POWER), altitude (SRTM), inferencia NLP e feature stores.

| API ID | Nome | Auth | RPM | FinOps | Packs | Método Gateway | Status |
| :--- | :--- | :--- | ---: | :--- | :--- | :--- | :--- |
| `overpass_osm` | Overpass API (POIs OpenStreetMap) | `NONE` | 30 | `FREE_UNLIMITED` | P03, P01 | `count_osm_pois_radius` | `HOMOLOGATED` |
| `osrm_routing` | OSRM (Open Source Routing Machine) | `NONE` | 60 | `FREE_UNLIMITED` | P03, P09 | `compute_osrm_route` | `HOMOLOGATED` |
| `open_meteo` | Open-Meteo Historical & Forecast | `NONE` | 60 | `FREE_UNLIMITED` | P01, P09 | `fetch_open_meteo_forecast` | `HOMOLOGATED` |
| `nasa_power` | NASA POWER API | `NONE` | 60 | `FREE_UNLIMITED` | P09, P01 | `fetch_nasa_power_daily` | `HOMOLOGATED` |
| `open_elevation` | Open-Elevation / OpenTopoData (SRTM) | `NONE` | 60 | `FREE_UNLIMITED` | P03, P09 | `fetch_elevation` | `HOMOLOGATED` |
| `huggingface_inference` | Hugging Face Inference API | `OPTIONAL_KEY` | 30 | `FREEMIUM` | P02 | `run_hf_inference` | `HOMOLOGATED` |
| `alpha_vantage` | Alpha Vantage | `API_KEY` | 5 | `FREEMIUM` | P05 | `fetch_alpha_vantage_daily` | `HOMOLOGATED` |
| `nasa_youtube_open_ml` | NASA Open APIs & YouTube Data (NLP / Visao) | `OPTIONAL_KEY` | 60 | `FREEMIUM` | P02 | `fetch_nasa_apod` | `HOMOLOGATED` |
| `upgini_adapter` | Upgini Public Feature Enrichment | `NONE` | 60 | `FREE_TIER` | P01, P09 | `describe_upgini_features` | `HOMOLOGATED` |

### Dados Globais, Demografia, Pesquisa & Sentimento (`BLOCO_4_GLOBAL_PESQUISA`)

Normalizacao ISO-3166 de paises, grafo cientifico aberto (OpenAlex) e monitoramento global de noticias/sentimento (GDELT).

| API ID | Nome | Auth | RPM | FinOps | Packs | Método Gateway | Status |
| :--- | :--- | :--- | ---: | :--- | :--- | :--- | :--- |
| `rest_countries` | REST Countries v3.1 | `NONE` | 60 | `FREE_UNLIMITED` | P01, P05 | `lookup_country` | `HOMOLOGATED` |
| `openalex_api` | OpenAlex Scientific Graph API | `NONE` | 60 | `FREE_UNLIMITED` | P02, P06 | `search_openalex_works` | `HOMOLOGATED` |
| `gdelt_doc_api` | GDELT Project Doc 2.0 API | `NONE` | 60 | `FREE_UNLIMITED` | P02, P07 | `query_gdelt_doc` | `HOMOLOGATED` |

---

## 8. Matriz de Freshness, SLA & Agendamento Cron (Phase 11)

Todas as **80 fontes governamentais** possuem metas explícitas de `freshness_sla_hours` e `cron_schedule_expression` para orquestração de mirror/CDC em nuvem:

| ID Fonte | Frequência Oficial | SLA Freshness (h) | Expressão Cron (UTC) | Modo de Acesso |
| :--- | :--- | :---: | :--- | :--- |
| `ibge_localidades` | Eventual (Criação de novos municípios) | `720h` | `0 3 1 * *` | `FEDERATED_API` |
| `brasilapi_core` | Tempo Real (< 150 ms) | `1h` | `0 * * * *` | `FEDERATED_API` |
| `bcb_sgs` | Diária / Mensal | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `bcb_olinda_ptax` | Diária (Tempo Real) | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `pgfn_divida_ativa` | Trimestral | `2160h` | `0 3 15 */3 *` | `LAKEHOUSE_BATCH` |
| `ibama_fiscalizacao` | Semanal (Dados Abertos IBAMA SIFISC) | `168h` | `0 4 * * 1` | `LAKEHOUSE_BATCH` |
| `stn_capag` | Anual / Quadrimestral | `720h` | `0 5 15 * *` | `FEDERATED_API` |
| `anm_mineracao` | Semanal (SIGMINE/SIGBM) / Mensal (CFEM) | `168h` | `0 6 * * 1` | `LAKEHOUSE_BATCH` |
| `bcb_sicor` | Mensal (Olinda OData SICOR BACEN) | `720h` | `0 5 12 * *` | `FEDERATED_API` |
| `mdr_snis` | Anual (Série Histórica SNIS/SINISA Água, Esgoto, RSU e Drenagem) | `720h` | `0 5 1 * *` | `LAKEHOUSE_BATCH` |
| `aneel_geracao_distribuida` | Semanal (Empreendimentos MMGD SIGA/SISGD) | `168h` | `0 4 * * 1` | `LAKEHOUSE_BATCH` |
| `cnj_improbidade` | Diária / Semanal | `168h` | `0 4 * * 1` | `FEDERATED_API` |
| `ibge_agro_municipal` | Anual (IBGE SIDRA Tabelas 5457 / 3939 / 289) | `8760h` | `0 6 15 9 *` | `FEDERATED_API` |
| `cgu_transparencia_gastos` | Diária (SIAFI Gastos Diretos & CPGF) / Mensal (Transferências & Emendas) | `168h` | `0 4 * * *` | `LAKEHOUSE_BATCH` |
| `rfb_cafir` | Mensal (Dados Abertos RFB CAFIR / CNIR) | `720h` | `0 4 20 * *` | `LAKEHOUSE_BATCH` |
| `rfb_cno` | Mensal | `720h` | `0 5 18 * *` | `LAKEHOUSE_BATCH` |
| `bndes_operacoes` | Trimestral (Carteira de Crédito Direta FINEM, Automático & FGI) | `720h` | `0 6 15 * *` | `LAKEHOUSE_BATCH` |
| `inpe_queimadas_seeg` | Diária (Focos INPE) / Anual (Inventário GEE SEEG) | `24h` | `0 3 * * *` | `LAKEHOUSE_BATCH` |
| `sinesp_seguranca` | Anual (SINESP VDE & Anuário FBSP) / Mensal (Série Estatística) | `720h` | `0 6 10 * *` | `LAKEHOUSE_BATCH` |
| `anp_combustiveis` | Semanal (LPC Preços) / Mensal (Vendas Derivados & PMQC) | `168h` | `0 5 * * 1` | `LAKEHOUSE_BATCH` |
| `querido_diario_okbr` | Diária / Semanal (Coleta & Indexação NLP de Diários Oficiais) | `168h` | `0 9 * * *` | `FEDERATED_API` |
| `ans_saude_suplementar` | Mensal (SIB Beneficiários) & Trimestral (DIOPS & IDSS) | `720h` | `0 8 10 * *` | `LAKEHOUSE_BATCH` |
| `senatran_frota` | Mensal (Frota Circulante por Município, Tipo & Combustível RENAVAM) | `720h` | `0 5 5 * *` | `LAKEHOUSE_BATCH` |
| `mme_ons_energia` | Diária (Operação SIN / Carga & EAR) / Mensal (Capacidade Instalada) | `168h` | `0 5 * * 1` | `LAKEHOUSE_BATCH` |
| `susep_seguros` | Mensal (SES Prêmios, Sinistros & VGBL/PGBL) / Trimestral (Solvência) | `720h` | `0 8 15 * *` | `LAKEHOUSE_BATCH` |
| `tse_eleicoes` | Bienal / Contínua (Ciclos Eleitorais & DivulgaCandContas) | `720h` | `0 6 1 * *` | `LAKEHOUSE_BATCH` |
| `inpi_propriedade_intelectual` | Semanal (Revista da Propriedade Industrial - RPI) / Mensal (Dados Abertos) | `168h` | `0 7 * * 2` | `LAKEHOUSE_BATCH` |
| `mps_inss_beneficios` | Mensal (BEPS / Síntese Municipal de Benefícios Emitidos e Concedidos) | `720h` | `0 6 10 * *` | `LAKEHOUSE_BATCH` |
| `cvm_mercado_extra` | Mensal (Ofertas RCVM 160, Administradores FII & Assessores CVM) | `720h` | `0 7 5 * *` | `LAKEHOUSE_BATCH` |
| `sedec_desastres` | Contínua (S2ID Decretações & CEMADEN Alertas) / Anual (Atlas Digital) | `720h` | `0 7 12 * *` | `LAKEHOUSE_BATCH` |
| `prf_acidentes` | Trimestral (Boletins de Ocorrência de Trânsito BRs) | `720h` | `0 6 15 * *` | `LAKEHOUSE_BATCH` |
| `cgu_servidores_federais` | Mensal (Folha de Pagamento SIAPE & Lotação UORG) | `720h` | `0 5 10 * *` | `LAKEHOUSE_BATCH` |
| `rfb_cnpj_lakehouse` | Mensal | `720h` | `0 2 15 * *` | `LAKEHOUSE_BATCH` |
| `pncp_contratacoes` | Tempo Real Contínuo | `1h` | `0 * * * *` | `FEDERATED_API` |
| `cnj_datajud` | Diária em tempo real | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `cgu_ceis_cnep` | Diária | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `cgu_pep` | Diária | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `rfb_simples_mei` | Mensal | `720h` | `0 2 15 * *` | `LAKEHOUSE_BATCH` |
| `tst_cndt` | Sob demanda / Diária | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `suframa_pim_zfm` | Mensal | `720h` | `0 3 5 * *` | `LAKEHOUSE_BATCH` |
| `bcb_estban` | Mensal | `720h` | `0 6 20 * *` | `LAKEHOUSE_BATCH` |
| `stn_siconfi` | Mensal / Bimestral (RREO) / Quadrimestral (RGF) / Anual (DCA) | `720h` | `0 4 10 * *` | `FEDERATED_API` |
| `bcb_agencia` | Mensal | `720h` | `0 6 10 * *` | `FEDERATED_API` |
| `cvm_dfp_itr` | Trimestral / Anual | `2160h` | `0 3 15 */3 *` | `LAKEHOUSE_BATCH` |
| `mdic_comexstat` | Mensal | `720h` | `0 3 5 * *` | `LAKEHOUSE_BATCH` |
| `cvm_fundos` | Diária | `24h` | `0 7 * * 1-5` | `LAKEHOUSE_BATCH` |
| `bcb_ifdata` | Trimestral | `2160h` | `0 2 1 */3 *` | `FEDERATED_API` |
| `stn_tesouro_transparente` | Mensal | `720h` | `0 3 5 * *` | `FEDERATED_API` |
| `comprasgov_catmat` | Semanal | `168h` | `0 4 * * 1` | `FEDERATED_API` |
| `ibge_sidra` | Mensal / Variável | `720h` | `0 3 5 * *` | `FEDERATED_API` |
| `ibge_pib_municipal` | Anual (defasagem 2 anos) | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `ibge_censo2022` | Decenal | `87600h` | `0 2 1 1 *` | `LAKEHOUSE_BATCH` |
| `mte_novo_caged` | Mensal | `720h` | `0 3 5 * *` | `LAKEHOUSE_BATCH` |
| `ibge_cnefe` | Decenal | `87600h` | `0 2 1 1 *` | `LAKEHOUSE_BATCH` |
| `ibge_pnad_continua` | Trimestral | `2160h` | `0 2 1 */3 *` | `LAKEHOUSE_BATCH` |
| `mte_rais` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `ibge_malhas` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `anatel_mosaico` | Mensal | `720h` | `0 3 5 * *` | `FEDERATED_API` |
| `antt_malha_transporte` | Trimestral | `2160h` | `0 2 1 */3 *` | `FEDERATED_API` |
| `datasus_cnes` | Mensal | `720h` | `0 3 5 * *` | `LAKEHOUSE_BATCH` |
| `datasus_sih` | Mensal | `720h` | `0 3 5 * *` | `LAKEHOUSE_BATCH` |
| `inep_censo_escolar` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `cnj_justica_aberta` | Semestral | `4320h` | `0 2 1 1,7 *` | `FEDERATED_API` |
| `datasus_sim` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `inep_censo_superior` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `anvisa_cmed` | Mensal | `720h` | `0 3 5 * *` | `FEDERATED_API` |
| `datasus_sinasc` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `inep_ideb` | Bienal | `17520h` | `0 2 1 1 *` | `FEDERATED_API` |
| `onr_registro_imoveis` | Mensal | `720h` | `0 3 5 * *` | `FEDERATED_API` |
| `inep_enem` | Anual | `8760h` | `0 2 15 1 *` | `LAKEHOUSE_BATCH` |
| `opendatasus_vigilancia` | Semanal | `168h` | `0 4 * * 1` | `FEDERATED_API` |
| `capitais_iptu_itbi` | Anual / Mensal | `720h` | `0 3 5 * *` | `FEDERATED_API` |
| `capes_sucupira` | Anual | `8760h` | `0 2 15 1 *` | `FEDERATED_API` |
| `datalake_federado_bq` | Contínua | `6h` | `0 */6 * * *` | `BIGQUERY_FEDERATED` |
| `inpe_terrabrasilis` | Diária (DETER) / Anual (PRODES) | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `ana_hidroweb_telemetria` | Diária (Telemetria horária) | `24h` | `0 6 * * *` | `FEDERATED_API` |
| `mapbiomas_cobertura` | Anual | `8760h` | `0 2 15 1 *` | `BIGQUERY_FEDERATED` |
| `sfb_sicar` | Semestral | `4320h` | `0 2 1 1,7 *` | `LAKEHOUSE_BATCH` |
| `mapa_zarc` | Safra / Anual | `8760h` | `0 2 15 1 *` | `FEDERATED_API` |
| `dadosgov_ckan` | Contínua | `6h` | `0 */6 * * *` | `FEDERATED_API` |

---

## 9. Instruções de Manutenção & Verificação de Integridade

O arquivo `CATALOG.md` é gerado de forma **100% determinística** a partir de `catalog/sources_registry.yaml` e `catalog/public_apis_registry.yaml`.

```powershell
# 1. Verificar se CATALOG.md está em sincronia com os registries (exit 0/1)
python scripts/render_catalog.py --check

# 2. Regenerar CATALOG.md após adicionar ou atualizar fontes
python scripts/render_catalog.py
```
