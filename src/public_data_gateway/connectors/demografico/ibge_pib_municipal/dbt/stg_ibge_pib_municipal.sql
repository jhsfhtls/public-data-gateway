{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(ano as integer) as ano,
    cast(indicador_id as varchar) as indicador_id,
    cast(indicador as varchar) as indicador,
    cast(valor_mil_reais as double) as valor_mil_reais,
    cast(fonte as varchar) as fonte,
    cast(extracted_at as timestamptz) as extracted_at
from read_parquet('data/ibge_pib_municipal/silver/pib_municipal.parquet')
