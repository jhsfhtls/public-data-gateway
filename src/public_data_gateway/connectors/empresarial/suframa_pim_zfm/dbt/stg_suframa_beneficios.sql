with source as (
    select * from {{ source('suframa_pim_zfm', 'suframa_beneficios_fiscais') }}
),

renamed as (
    select
        cast(codigo_beneficio as varchar) as codigo_beneficio,
        cast(tipo_tributo as varchar) as tipo_tributo,
        cast(descricao_beneficio as varchar) as descricao_beneficio,
        cast(base_legal as varchar) as base_legal,
        cast(tipo_incentivo as varchar) as tipo_incentivo,
        try_cast(vigencia_limite as date) as vigencia_limite,
        cast(exige_ppb as boolean) as exige_ppb
    from source
)

select * from renamed
