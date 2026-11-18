{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/inpe_queimadas_seeg/silver/inpe_risco_fogo_mensal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma as varchar) as bioma,
    cast(risco_fogo_medio_inpe as double) as risco_fogo_medio_inpe,
    cast(frp_medio_mw as double) as frp_medio_mw,
    cast(dias_seco_consecutivos_max as bigint) as dias_seco_consecutivos_max,
    cast(concentracao_estacao_seca_pct as double) as concentracao_estacao_seca_pct,
    cast(area_cicatriz_queimada_ha as double) as area_cicatriz_queimada_ha
from source
