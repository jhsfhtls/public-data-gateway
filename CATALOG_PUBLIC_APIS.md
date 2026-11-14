# Catálogo Bipartido — APIs Públicas & Globais (Analytics, KYC/KYB & ML)

> Documento gerado automaticamente a partir de `catalog/public_apis_registry.yaml`.
> Catálogo complementar ao [CATALOG.md](CATALOG.md) (49 fontes oficiais Gov BR).

## Resumo Executivo

- **Total de APIs Curadas (Hub Global & Analytics):** `29`
- **APIs Homologadas no Gateway (`PublicApisHub`):** `28`
- **APIs Sem Autenticação (`auth_type=NONE`):** `24`
- **Conectores Gov BR já Auditados & Homologados:** `16` (`brasilapi_core`, `cgu_ceis_cnep`, `cgu_pep`, `ibge_localidades`, `ibge_censo2022`, `ibge_pib_municipal`, `ibge_sidra`, `ibge_cnefe`, `ibge_malhas`, `ibge_pnad_continua`, `bcb_sgs`, `bcb_olinda_ptax`, `bcb_ifdata`, `pncp_contratacoes`, `cvm_dfp_itr`, `cvm_fundos`)

### Fontes de Auditoria e Descoberta

- **Conecta GOV.BR** (https://www.gov.br/conecta/catalogo/): Auditoria de APIs governamentais brasileiras disponiveis para integracao
- **Public APIs Directory** (https://github.com/public-apis/public-apis): Descoberta curada de APIs publicas globais para Analytics e Machine Learning

## Blocos Funcionais

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
| `company_enrich_apollo_bdc` | CompanyEnrich / Apollo / BigDataCorp (Adaptador B2B Freemium) | `OPTIONAL_KEY` | 30 | `FREEMIUM` | P04, P06 | `lookup_b2b_domain_profile` | `CATALOGED` |

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
