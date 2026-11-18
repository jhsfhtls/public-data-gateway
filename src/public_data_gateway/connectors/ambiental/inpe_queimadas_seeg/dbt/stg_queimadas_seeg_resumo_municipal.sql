{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/inpe_queimadas_seeg/silver/queimadas_seeg_resumo_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_focos_calor_inpe as bigint) as qtd_focos_calor_inpe,
    cast(area_cicatriz_queimada_ha as double) as area_cicatriz_queimada_ha,
    cast(risco_fogo_medio_inpe as double) as risco_fogo_medio_inpe,
    cast(emissoes_liquidas_tco2e_seeg as double) as emissoes_liquidas_tco2e_seeg,
    cast(participacao_agro_lulucf_pct as double) as participacao_agro_lulucf_pct
from source
