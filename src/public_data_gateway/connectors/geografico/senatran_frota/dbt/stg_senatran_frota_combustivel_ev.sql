{{ config(materialized='view') }}

select
    cast(cod_frota_comb as varchar) as cod_frota_comb,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(matriz_propulsao as varchar) as matriz_propulsao,
    cast(qtd_veiculos as bigint) as qtd_veiculos,
    cast(participacao_eletrificados_pct as double) as participacao_eletrificados_pct,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('senatran_frota', 'senatran_frota_combustivel_ev') }}
where cod_frota_comb is not null
  and id_municipio is not null
