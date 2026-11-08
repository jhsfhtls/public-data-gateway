{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_ceis_cnep', 'cgu_cepim') }}
),

renamed as (
    select
        trim(id_impedimento) as id_impedimento,
        trim(cnpj_entidade) as cnpj_entidade,
        nullif(trim(cnpj_basico), '') as cnpj_basico,
        upper(trim(nome_entidade)) as nome_entidade,
        trim(motivo_impedimento) as motivo_impedimento,
        upper(trim(orgao_concedente)) as orgao_concedente,
        trim(numero_convenio) as numero_convenio,
        cast(data_referencia as date) as data_referencia,
        cast(is_ativo as boolean) as is_ativo
    from source
)

select * from renamed
