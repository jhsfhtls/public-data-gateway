{{ config(materialized='view') }}

select
    cast(cod_eleicao_tse as varchar) as cod_eleicao_tse,
    cast(ano_eleicao as integer) as ano_eleicao,
    cast(turno_eleicao as varchar) as turno_eleicao,
    cast(cargo_disputado as varchar) as cargo_disputado,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_candidaturas_aptas as bigint) as qtd_candidaturas_aptas,
    cast(partidos_competitivos_count as bigint) as partidos_competitivos_count,
    cast(competicao_eleitoral_pct as double) as competicao_eleitoral_pct
from {{ source('tse_eleicoes', 'tse_candidaturas_partidos_municipio') }}
