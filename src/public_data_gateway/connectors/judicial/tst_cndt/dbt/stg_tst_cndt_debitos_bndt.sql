with source as (
    select * from {{ source('tst_cndt', 'tst_cndt_debitos_bndt') }}
),

renamed as (
    select
        cast(id_debito_bndt as varchar) as id_debito_bndt,
        cast(numero_certidao as varchar) as numero_certidao,
        cast(cnpj_ou_cpf as varchar) as cnpj_ou_cpf,
        cast(numero_processo_nup as varchar) as numero_processo_nup,
        cast(tribunal_origem as varchar) as tribunal_origem,
        cast(vara_trabalho as varchar) as vara_trabalho,
        cast(tipo_divida_trabalhista as varchar) as tipo_divida_trabalhista,
        cast(valor_debito as double) as valor_debito,
        cast(situacao_exigibilidade as varchar) as situacao_exigibilidade,
        try_cast(data_inclusao_bndt as date) as data_inclusao_bndt,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source
)

select * from renamed
