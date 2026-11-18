{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(eleitorado_apto_total as bigint) as eleitorado_apto_total,
    cast(total_candidaturas_aptas as bigint) as total_candidaturas_aptas,
    cast(comparecimento_pct as double) as comparecimento_pct,
    cast(receita_total_campanha_mil_brl as double) as receita_total_campanha_mil_brl,
    cast(custo_por_eleitor_brl as double) as custo_por_eleitor_brl
from {{ source('tse_eleicoes', 'tse_eleicoes_resumo_municipal') }}
