{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(taxa_mvi_por_100k_hab as double) as taxa_mvi_por_100k_hab,
    cast(taxa_roubo_carga_100k_hab as double) as taxa_roubo_carga_100k_hab,
    cast(taxa_furto_roubo_veiculo_100k_hab as double) as taxa_furto_roubo_veiculo_100k_hab,
    cast(taxa_estelionato_100k_hab as double) as taxa_estelionato_100k_hab,
    cast(investimento_seguranca_per_capita_brl as double) as investimento_seguranca_per_capita_brl
from {{ source('sinesp_seguranca', 'sinesp_seguranca_resumo_municipal') }}
where id_municipio is not null
