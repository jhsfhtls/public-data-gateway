with source as (
    select * from {{ source('mdic_comexstat', 'comex_ncm_importacao') }}
)

select
    cast(ano as varchar) as ano,
    cast(mes as varchar) as mes,
    cast(ncm_8 as varchar) as ncm_8,
    cast(codigo_unidade as varchar) as codigo_unidade,
    cast(codigo_pais as varchar) as codigo_pais,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(codigo_via as varchar) as codigo_via,
    cast(codigo_urf as varchar) as codigo_urf,
    cast(quantidade_estatistica as double) as quantidade_estatistica,
    cast(peso_liquido_kg as double) as peso_liquido_kg,
    cast(valor_fob_usd as double) as valor_fob_usd,
    cast(valor_frete_usd as double) as valor_frete_usd,
    cast(valor_seguro_usd as double) as valor_seguro_usd
from source
