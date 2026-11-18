{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(total_ofertas_rcvm160 as bigint) as total_ofertas_rcvm160,
    cast(volume_captado_rcvm160_mil_brl as double) as volume_captado_rcvm160_mil_brl,
    cast(assessores_investimento_ativos as bigint) as assessores_investimento_ativos,
    cast(administradores_ativos_count as bigint) as administradores_ativos_count,
    cast(captacao_per_capita_brl as double) as captacao_per_capita_brl
from {{ source('cvm_mercado_extra', 'cvm_mercado_extra_resumo_municipal') }}
