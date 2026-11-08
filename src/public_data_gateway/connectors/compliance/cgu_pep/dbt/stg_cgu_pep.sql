{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_pep', 'cgu_pep') }}
),

renamed as (
    select
        trim(id_registro_pep) as id_registro_pep,
        trim(cpf_pep) as cpf_pep,
        upper(trim(nome_pep)) as nome_pep,
        upper(trim(sigla_funcao)) as sigla_funcao,
        upper(trim(descricao_funcao)) as descricao_funcao,
        upper(trim(nivel_funcao)) as nivel_funcao,
        upper(trim(nome_orgao)) as nome_orgao,
        coalesce(upper(trim(sigla_uf_orgao)), 'DF') as sigla_uf_orgao,
        cast(data_inicio_exercicio as date) as data_inicio_exercicio,
        cast(data_fim_exercicio as date) as data_fim_exercicio,
        cast(data_fim_carencia as date) as data_fim_carencia,
        cast(is_pep_ativo as boolean) as is_pep_ativo
    from source
)

select * from renamed
