{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(preco_gasolina_brl as double) as preco_gasolina_brl,
    cast(preco_etanol_brl as double) as preco_etanol_brl,
    cast(preco_diesel_s10_brl as double) as preco_diesel_s10_brl,
    cast(razao_etanol_gasolina_pct as double) as razao_etanol_gasolina_pct,
    cast(participacao_biocombustivel_pct as double) as participacao_biocombustivel_pct,
    cast(volume_diesel_m3 as double) as volume_diesel_m3
from {{ source('anp_combustiveis', 'anp_combustiveis_resumo_municipal') }}
where id_municipio is not null
