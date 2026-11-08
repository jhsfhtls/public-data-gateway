with source as (
    select * from {{ source('tst_cndt', 'tst_cndt_tribunais_regionais') }}
),

renamed as (
    select
        cast(sigla_trt as varchar) as sigla_trt,
        cast(nome_tribunal as varchar) as nome_tribunal,
        cast(jurisdicao_uf as varchar) as jurisdicao_uf,
        cast(sede_municipio as varchar) as sede_municipio,
        cast(regiao_brasil as varchar) as regiao_brasil
    from source
)

select * from renamed
