{{ config(materialized='view') }}

with source as (
    select * from {{ source('pncp_contratacoes', 'pncp_orgaos') }}
),

renamed as (
    select
        lpad(trim(cnpj_orgao), 14, '0') as cnpj_orgao,
        trim(razao_social) as razao_social,
        upper(trim(esfera_id)) as esfera_id,
        upper(trim(poder_id)) as poder_id,
        lpad(trim(id_municipio), 7, '0') as id_municipio,
        trim(nome_municipio) as nome_municipio,
        upper(trim(sigla_uf)) as sigla_uf,
        cast(total_compras_registradas as integer) as total_compras_registradas,
        cast(total_contratos_registrados as integer) as total_contratos_registrados
    from source
)

select * from renamed
