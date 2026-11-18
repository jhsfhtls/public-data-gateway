{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(total_operacoes_bndes as bigint) as total_operacoes_bndes,
    cast(valor_total_contratado_mil_brl as double) as valor_total_contratado_mil_brl,
    cast(desembolso_anual_mil_brl as double) as desembolso_anual_mil_brl,
    cast(fomento_verde_esg_pct as double) as fomento_verde_esg_pct,
    cast(fomento_per_capita_brl as double) as fomento_per_capita_brl
from {{ source('bndes_operacoes', 'bndes_operacoes_resumo_municipal') }}
