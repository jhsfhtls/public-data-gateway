{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(indice_capacidade_institucional_munic as double) as indice_capacidade_institucional_munic,
    cast(servidores_estatutarios_municipio as bigint) as servidores_estatutarios_municipio,
    cast(indice_governanca_estadual_estadic as double) as indice_governanca_estadual_estadic,
    cast(despesa_familiar_media_pof_brl as double) as despesa_familiar_media_pof_brl,
    cast(comprometimento_habitacao_transporte_pct as double) as comprometimento_habitacao_transporte_pct
from {{ source('ibge_institucional', 'ibge_institucional_resumo_municipal') }}
