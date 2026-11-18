{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(producao_sia_total_12m as bigint) as producao_sia_total_12m,
    cast(notificacoes_sinan_total_12m as bigint) as notificacoes_sinan_total_12m,
    cast(valor_sia_aprovado_mil_brl as double) as valor_sia_aprovado_mil_brl,
    cast(cobertura_sisvan_pct as double) as cobertura_sisvan_pct,
    cast(atendimentos_sia_per_capita as double) as atendimentos_sia_per_capita
from {{ source('datasus_extra', 'datasus_extra_resumo_municipal') }}
where id_municipio is not null
