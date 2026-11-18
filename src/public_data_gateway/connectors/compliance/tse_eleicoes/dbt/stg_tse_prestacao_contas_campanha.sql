{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(receita_total_campanha_mil_brl as double) as receita_total_campanha_mil_brl,
    cast(despesa_contratada_mil_brl as double) as despesa_contratada_mil_brl,
    cast(contas_aprovadas_pct as double) as contas_aprovadas_pct,
    cast(doadores_cpf_cnpj_distintos as bigint) as doadores_cpf_cnpj_distintos
from {{ source('tse_eleicoes', 'tse_prestacao_contas_campanha') }}
