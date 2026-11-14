{{ config(materialized='table') }}

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    classe_transicao_mapbiomas,
    area_transicao_ha,
    participacao_transicao_pct,
    taxa_conversao_liquida_pct,
    intensidade_pressao_transicao_pct,
    reference_date
from {{ ref('stg_mapbiomas_transicoes_uso_terra') }}
