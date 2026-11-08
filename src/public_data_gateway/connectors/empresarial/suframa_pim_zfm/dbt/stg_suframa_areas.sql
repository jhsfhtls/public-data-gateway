with source as (
    select * from {{ source('suframa_pim_zfm', 'suframa_areas_livre_comercio') }}
),

renamed as (
    select
        cast(codigo_area_suframa as varchar) as codigo_area_suframa,
        cast(nome_area as varchar) as nome_area,
        cast(tipo_area as varchar) as tipo_area,
        cast(id_municipio_ibge as varchar) as id_municipio_ibge,
        cast(nome_municipio as varchar) as nome_municipio,
        cast(sigla_uf as varchar) as sigla_uf,
        cast(base_legal as varchar) as base_legal
    from source
)

select * from renamed
