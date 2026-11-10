{{ config(materialized='table') }}

select
    id_municipio,
    ano,
    indicador_id,
    indicador,
    valor_mil_reais,
    fonte,
    extracted_at
from {{ ref('stg_ibge_pib_municipal') }}
