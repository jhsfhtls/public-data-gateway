{{ config(materialized='view') }}

select
    cast(cod_votacao_tse as varchar) as cod_votacao_tse,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(eleitorado_apto_total as bigint) as eleitorado_apto_total,
    cast(comparecimento_pct as double) as comparecimento_pct,
    cast(abstencao_pct as double) as abstencao_pct,
    cast(votos_validos_pct as double) as votos_validos_pct,
    cast(ano_eleicao as integer) as ano_eleicao
from {{ source('tse_eleicoes', 'tse_votacao_comparecimento_secao') }}
