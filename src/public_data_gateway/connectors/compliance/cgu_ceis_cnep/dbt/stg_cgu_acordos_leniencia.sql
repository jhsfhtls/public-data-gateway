{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_ceis_cnep', 'cgu_acordos_leniencia') }}
),

renamed as (
    select
        trim(id_acordo) as id_acordo,
        upper(trim(nome_empresa)) as nome_empresa,
        trim(cnpj_empresa) as cnpj_empresa,
        nullif(trim(cnpj_basico), '') as cnpj_basico,
        cast(data_inicio_acordo as date) as data_inicio_acordo,
        cast(data_fim_acordo as date) as data_fim_acordo,
        upper(trim(situacao_acordo)) as situacao_acordo,
        upper(trim(orgao_responsavel)) as orgao_responsavel,
        cast(coalesce(total_sancoes_reparacoes, 0) as integer) as total_sancoes_reparacoes,
        cast(coalesce(valor_total_reparacao, 0.0) as double) as valor_total_reparacao
    from source
)

select * from renamed
