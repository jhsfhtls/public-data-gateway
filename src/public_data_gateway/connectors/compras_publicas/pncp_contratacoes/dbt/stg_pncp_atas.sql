{{ config(materialized='view') }}

with source as (
    select * from {{ source('pncp_contratacoes', 'pncp_atas') }}
),

renamed as (
    select
        trim(numero_controle_pncp_ata) as numero_controle_pncp_ata,
        trim(numero_controle_pncp_compra) as numero_controle_pncp_compra,
        trim(numero_ata_registro_preco) as numero_ata_registro_preco,
        cast(ano_ata as integer) as ano_ata,
        lpad(trim(cnpj_orgao), 14, '0') as cnpj_orgao,
        trim(nome_orgao) as nome_orgao,
        trim(codigo_unidade_orgao) as codigo_unidade_orgao,
        trim(nome_unidade_orgao) as nome_unidade_orgao,
        trim(objeto_contratacao) as objeto_contratacao,
        vigencia_inicio,
        vigencia_fim,
        data_assinatura,
        data_publicacao_pncp,
        cast(cancelado_flag as boolean) as is_cancelada
    from source
)

select * from renamed
