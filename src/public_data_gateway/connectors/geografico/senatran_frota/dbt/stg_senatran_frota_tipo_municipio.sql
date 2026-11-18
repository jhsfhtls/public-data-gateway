{{ config(materialized='view') }}

select
    cast(cod_frota_tipo as varchar) as cod_frota_tipo,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(tipo_veiculo as varchar) as tipo_veiculo,
    cast(qtd_veiculos_ativos as bigint) as qtd_veiculos_ativos,
    cast(participacao_frota_pct as double) as participacao_frota_pct,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('senatran_frota', 'senatran_frota_tipo_municipio') }}
where cod_frota_tipo is not null
  and id_municipio is not null
