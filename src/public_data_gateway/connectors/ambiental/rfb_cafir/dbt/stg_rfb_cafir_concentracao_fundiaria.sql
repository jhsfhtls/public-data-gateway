{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/rfb_cafir/silver/cafir_concentracao_fundiaria.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(indice_gini_fundiario_cafir as double) as indice_gini_fundiario_cafir,
    cast(area_pequenos_imoveis_ate_4mf_pct as double) as area_pequenos_imoveis_ate_4mf_pct,
    cast(area_medios_imoveis_4a15mf_pct as double) as area_medios_imoveis_4a15mf_pct,
    cast(area_grandes_imoveis_acima_15mf_pct as double) as area_grandes_imoveis_acima_15mf_pct
from source
