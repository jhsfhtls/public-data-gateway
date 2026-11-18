{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(total_gastos_federais_mil_brl as double) as total_gastos_federais_mil_brl,
    cast(total_transferencias_mil_brl as double) as total_transferencias_mil_brl,
    cast(taxa_execucao_orcamentaria_pct as double) as taxa_execucao_orcamentaria_pct,
    cast(gasto_federal_per_capita_brl as double) as gasto_federal_per_capita_brl
from {{ source('cgu_transparencia_gastos', 'cgu_transparencia_resumo_municipal') }}
where id_municipio is not null
