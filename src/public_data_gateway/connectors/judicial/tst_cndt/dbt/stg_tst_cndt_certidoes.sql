with source as (
    select * from {{ source('tst_cndt', 'tst_cndt_certidoes') }}
),

renamed as (
    select
        cast(numero_certidao as varchar) as numero_certidao,
        cast(ano_certidao as integer) as ano_certidao,
        cast(cnpj_ou_cpf as varchar) as cnpj_ou_cpf,
        cast(cnpj_raiz as varchar) as cnpj_raiz,
        cast(razao_social_ou_nome as varchar) as razao_social_ou_nome,
        cast(tipo_documento as varchar) as tipo_documento,
        cast(status_certidao as varchar) as status_certidao,
        cast(is_regular as boolean) as is_regular,
        try_cast(data_expedicao as date) as data_expedicao,
        cast(hora_expedicao as varchar) as hora_expedicao,
        try_cast(data_validade as date) as data_validade,
        cast(codigo_autenticidade as varchar) as codigo_autenticidade,
        cast(orgao_emissor as varchar) as orgao_emissor,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source
)

select * from renamed
