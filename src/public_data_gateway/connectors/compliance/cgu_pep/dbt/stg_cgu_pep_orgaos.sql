{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_pep', 'cgu_pep_orgaos') }}
),

renamed as (
    select
        trim(codigo_orgao) as codigo_orgao,
        upper(trim(nome_orgao)) as nome_orgao,
        upper(trim(sigla_uf)) as sigla_uf,
        upper(trim(poder)) as poder,
        upper(trim(esfera)) as esfera,
        cast(coalesce(total_cargos_pep, 0) as integer) as total_cargos_pep
    from source
)

select * from renamed
