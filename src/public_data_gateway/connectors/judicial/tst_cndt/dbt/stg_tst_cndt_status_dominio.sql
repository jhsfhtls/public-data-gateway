with source as (
    select * from {{ source('tst_cndt', 'tst_cndt_status_dominio') }}
),

renamed as (
    select
        cast(codigo_status as varchar) as codigo_status,
        cast(descricao_status as varchar) as descricao_status,
        cast(permite_licitar as boolean) as permite_licitar,
        cast(nivel_risco_trabalhista as varchar) as nivel_risco_trabalhista
    from source
)

select * from renamed
