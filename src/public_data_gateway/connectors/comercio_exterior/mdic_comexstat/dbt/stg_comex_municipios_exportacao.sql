with source as (
    select * from {{ source('mdic_comexstat', 'comex_municipios_exportacao') }}
)

select
    cast(ano as varchar) as ano,
    cast(mes as varchar) as mes,
    cast(sh4 as varchar) as sh4,
    cast(codigo_pais as varchar) as codigo_pais,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_municipio_7 as varchar) as id_municipio_7,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(peso_liquido_kg as double) as peso_liquido_kg,
    cast(valor_fob_usd as double) as valor_fob_usd
from source
