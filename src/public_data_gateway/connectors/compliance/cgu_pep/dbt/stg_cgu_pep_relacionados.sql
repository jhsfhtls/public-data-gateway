{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_pep', 'cgu_pep_relacionados') }}
),

renamed as (
    select
        trim(id_relacionamento) as id_relacionamento,
        trim(cpf_pep) as cpf_pep,
        upper(trim(nome_pep)) as nome_pep,
        upper(trim(tipo_relacao)) as tipo_relacao,
        upper(trim(nome_pessoa_relacionada)) as nome_pessoa_relacionada,
        trim(cpf_pessoa_relacionada) as cpf_pessoa_relacionada,
        trim(cnpj_empresa_vinculada) as cnpj_empresa_vinculada,
        upper(trim(grau_exposicao)) as grau_exposicao
    from source
)

select * from renamed
