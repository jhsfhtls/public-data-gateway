{{ config(materialized='view') }}

with source as (
    select * from {{ source('cgu_ceis_cnep', 'cgu_ceis') }}
),

renamed as (
    select
        trim(id_sancao) as id_sancao,
        upper(trim(tipo_pessoa)) as tipo_pessoa,
        trim(cpf_cnpj_sancionado) as cpf_cnpj_sancionado,
        nullif(trim(cnpj_basico), '') as cnpj_basico,
        upper(trim(nome_sancionado)) as nome_sancionado,
        trim(tipo_sancao) as tipo_sancao,
        cast(data_inicio_sancao as date) as data_inicio_sancao,
        cast(data_fim_sancao as date) as data_fim_sancao,
        cast(data_publicacao_sancao as date) as data_publicacao_sancao,
        upper(trim(orgao_sancionador)) as orgao_sancionador,
        upper(trim(sigla_uf_orgao)) as sigla_uf_orgao,
        upper(trim(poder_orgao)) as poder_orgao,
        upper(trim(esfera_orgao)) as esfera_orgao,
        trim(fundamentacao_legal) as fundamentacao_legal,
        trim(numero_processo) as numero_processo,
        cast(is_sancao_ativa as boolean) as is_sancao_ativa
    from source
)

select * from renamed
