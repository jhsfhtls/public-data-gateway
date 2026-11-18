{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(receita_contraprestacoes_mil_brl as double) as receita_contraprestacoes_mil_brl,
    cast(sinistralidade_assistencial_pct as double) as sinistralidade_assistencial_pct,
    cast(provisoes_tecnicas_cobertura_pct as double) as provisoes_tecnicas_cobertura_pct,
    cast(indice_solvencia_ans_pct as double) as indice_solvencia_ans_pct
from {{ source('ans_saude_suplementar', 'ans_sinistralidade_diops_economico') }}
