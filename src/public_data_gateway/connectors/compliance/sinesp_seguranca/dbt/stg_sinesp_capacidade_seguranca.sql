{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(qtd_delegacias_unidades as integer) as qtd_delegacias_unidades,
    cast(possui_guarda_municipal_ativa as boolean) as possui_guarda_municipal_ativa,
    cast(efetivo_seguranca_100k_hab as double) as efetivo_seguranca_100k_hab,
    cast(investimento_seguranca_per_capita_brl as double) as investimento_seguranca_per_capita_brl
from {{ source('sinesp_seguranca', 'sinesp_capacidade_seguranca') }}
where id_municipio is not null
