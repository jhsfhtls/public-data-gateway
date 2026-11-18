{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(premios_emitidos_total_mil_brl as double) as premios_emitidos_total_mil_brl,
    cast(sinistros_ocorridos_mil_brl as double) as sinistros_ocorridos_mil_brl,
    cast(taxa_sinistralidade_media_pct as double) as taxa_sinistralidade_media_pct,
    cast(corretoras_ativas_susep as bigint) as corretoras_ativas_susep,
    cast(premio_per_capita_brl as double) as premio_per_capita_brl
from {{ source('susep_seguros', 'susep_seguros_resumo_municipal') }}
where id_municipio is not null
