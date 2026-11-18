{{ config(materialized='view') }}

select
    cast(cod_fonte_municipio as varchar) as cod_fonte_municipio,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(fonte_geracao as varchar) as fonte_geracao,
    cast(qtd_conexoes_mmgd as integer) as qtd_conexoes_mmgd,
    cast(potencia_instalada_kw as double) as potencia_instalada_kw,
    cast(participacao_potencia_pct as double) as participacao_potencia_pct,
    cast(geracao_estimada_mwh_ano as double) as geracao_estimada_mwh_ano
from {{ source('aneel_geracao_distribuida', 'aneel_geracao_fonte_municipio') }}
where cod_fonte_municipio is not null
  and id_municipio is not null
