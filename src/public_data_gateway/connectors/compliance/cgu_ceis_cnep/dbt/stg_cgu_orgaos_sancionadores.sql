{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_ceis_cnep', 'cgu_orgaos_sancionadores') }}
),

renamed as (
    select
        trim(codigo_orgao) as codigo_orgao,
        upper(trim(nome_orgao)) as nome_orgao,
        upper(trim(sigla_uf)) as sigla_uf,
        upper(trim(poder)) as poder,
        upper(trim(esfera)) as esfera,
        cast(coalesce(total_sancoes_ceis, 0) as integer) as total_sancoes_ceis,
        cast(coalesce(total_sancoes_cnep, 0) as integer) as total_sancoes_cnep,
        cast(coalesce(total_sancoes_ceis, 0) + coalesce(total_sancoes_cnep, 0) as integer) as total_sancoes_aplicadas
    from source
)

select * from renamed
