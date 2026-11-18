{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(despesa_mensal_familiar_pof_brl as double) as despesa_mensal_familiar_pof_brl,
    cast(renda_monetaria_media_domiciliar_brl as double) as renda_monetaria_media_domiciliar_brl,
    cast(comprometimento_habitacao_transporte_pct as double) as comprometimento_habitacao_transporte_pct,
    cast(participacao_alimentacao_domicilio_pct as double) as participacao_alimentacao_domicilio_pct
from {{ source('ibge_institucional', 'ibge_pof_orcamentos_consumo') }}
