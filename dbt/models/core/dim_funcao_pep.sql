{{ config(materialized='table') }}

with funcoes as (
    select * from {{ ref('stg_cgu_pep_funcoes') }}
)

select
    sigla_funcao,
    descricao_funcao,
    categoria_exposicao,
    nivel_hierarquico,
    amparo_coaf,
    grau_risco_pld,
    case
        when grau_risco_pld = 'CRITICO' then 1
        when grau_risco_pld = 'ALTO' then 2
        else 3
    end as ranking_risco_pld
from funcoes
