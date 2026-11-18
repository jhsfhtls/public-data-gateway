{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(processos_ativos_sigmine as bigint) as processos_ativos_sigmine,
    cast(area_total_titulada_ha as double) as area_total_titulada_ha,
    cast(arrecadacao_cfem_total_mil_brl as double) as arrecadacao_cfem_total_mil_brl,
    cast(indice_conformidade_sigbm_pct as double) as indice_conformidade_sigbm_pct,
    cast(participacao_substancias_criticas_pct as double) as participacao_substancias_criticas_pct
from {{ source('anm_mineracao', 'anm_mineracao_resumo_municipal') }}
where id_municipio is not null
