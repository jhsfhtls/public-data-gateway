with source as (
    select * from {{ source('suframa_pim_zfm', 'suframa_empresas_cadastradas') }}
),

renamed as (
    select
        cast(numero_inscricao_suframa as varchar) as numero_inscricao_suframa,
        cast(cnpj_14 as varchar) as cnpj_14,
        cast(cnpj_raiz as varchar) as cnpj_raiz,
        cast(razao_social as varchar) as razao_social,
        cast(nome_fantasia as varchar) as nome_fantasia,
        cast(situacao_cadastral as varchar) as situacao_cadastral,
        try_cast(data_inscricao as date) as data_inscricao,
        try_cast(data_validade_cadastro as date) as data_validade_cadastro,
        cast(tipo_estabelecimento as varchar) as tipo_estabelecimento,
        cast(tipo_area_incentivada as varchar) as tipo_area_incentivada,
        cast(codigo_area_suframa as varchar) as codigo_area_suframa,
        cast(id_municipio_ibge as varchar) as id_municipio_ibge,
        cast(sigla_uf as varchar) as sigla_uf,
        cast(subsetor_industrial as varchar) as subsetor_industrial,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source
)

select * from renamed
