{{ config(materialized='table') }}

select
    sigla_trt,
    nome_tribunal,
    jurisdicao_uf,
    sede_municipio,
    regiao_brasil,
    case
        when sigla_trt in ('TRT02', 'TRT15', 'TRT01', 'TRT03', 'TRT04') then 'ALTO_VOLUME_PROCESSUAL'
        when sigla_trt in ('TRT05', 'TRT06', 'TRT09', 'TRT12', 'TRT18') then 'MEDIO_VOLUME_PROCESSUAL'
        else 'PADRAO_REGIONAL'
    end as porte_tribunal_trabalho
from {{ ref('stg_tst_cndt_tribunais_regionais') }}
