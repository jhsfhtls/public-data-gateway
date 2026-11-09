{{ config(materialized='view') }}

select
    id_municipio,
    sigla_uf,
    nome_municipio,
    tipo_edificacao,
    total_edificacoes,
    participacao_pct,
    densidade_edificacoes_km2,
    reference_date,
    extracted_at
from {{ source('ibge_cnefe_silver', 'cnefe_tipos_edificacao') }}
