with source as (
    select * from {{ source('suframa_pim_zfm', 'suframa_projetos_incentivados') }}
),

renamed as (
    select
        cast(codigo_projeto as varchar) as codigo_projeto,
        cast(numero_resolucao_cas as varchar) as numero_resolucao_cas,
        cast(numero_inscricao_suframa as varchar) as numero_inscricao_suframa,
        cast(cnpj_14 as varchar) as cnpj_14,
        cast(titulo_projeto as varchar) as titulo_projeto,
        cast(subsetor_industrial as varchar) as subsetor_industrial,
        cast(status_projeto as varchar) as status_projeto,
        try_cast(data_aprovacao_cas as date) as data_aprovacao_cas,
        cast(faturamento_anual_estimado as double) as faturamento_anual_estimado,
        cast(empregos_diretos_estimados as integer) as empregos_diretos_estimados,
        cast(cumpre_ppb as boolean) as cumpre_ppb,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp
    from source
)

select * from renamed
