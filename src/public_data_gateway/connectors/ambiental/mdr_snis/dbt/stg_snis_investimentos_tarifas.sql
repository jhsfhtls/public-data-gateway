{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(tarifa_media_agua_esgoto_m3 as double) as tarifa_media_agua_esgoto_m3,
    cast(indice_suficiencia_caixa_pct as double) as indice_suficiencia_caixa_pct,
    cast(investimento_anual_per_capita_brl as double) as investimento_anual_per_capita_brl,
    cast(investimento_total_milhoes_brl as double) as investimento_total_milhoes_brl
from {{ source('mdr_snis', 'snis_investimentos_tarifas') }}
where id_municipio is not null
