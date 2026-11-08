{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_pep', 'cgu_pep_historico_carencia') }}
),

renamed as (
    select
        trim(id_historico) as id_historico,
        trim(cpf_pep) as cpf_pep,
        upper(trim(nome_pep)) as nome_pep,
        upper(trim(descricao_funcao)) as descricao_funcao,
        upper(trim(nome_orgao)) as nome_orgao,
        cast(data_termino_mandato as date) as data_termino_mandato,
        cast(data_fim_carencia as date) as data_fim_carencia,
        cast(anos_carencia_regulatoria as integer) as anos_carencia_regulatoria,
        cast(is_em_carencia as boolean) as is_em_carencia
    from source
)

select * from renamed
