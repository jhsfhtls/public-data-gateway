{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_pep', 'cgu_pep_funcoes') }}
),

renamed as (
    select
        upper(trim(sigla_funcao)) as sigla_funcao,
        upper(trim(descricao_funcao)) as descricao_funcao,
        upper(trim(categoria_exposicao)) as categoria_exposicao,
        upper(trim(nivel_hierarquico)) as nivel_hierarquico,
        trim(amparo_coaf) as amparo_coaf,
        upper(trim(grau_risco_pld)) as grau_risco_pld
    from source
)

select * from renamed
