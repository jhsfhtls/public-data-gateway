{{ config(materialized='view') }}

select
    cast(cod_balanco_ons as varchar) as cod_balanco_ons,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(subsistema_sin as varchar) as subsistema_sin,
    cast(carga_energia_mwmed as double) as carga_energia_mwmed,
    cast(ear_energia_armazenada_pct as double) as ear_energia_armazenada_pct,
    cast(fator_capacidade_renovavel_pct as double) as fator_capacidade_renovavel_pct,
    cast(cmo_medio_brl_mwh as double) as cmo_medio_brl_mwh
from {{ source('mme_ons_energia', 'ons_balanco_carga_subsistema_uf') }}
where id_municipio is not null
