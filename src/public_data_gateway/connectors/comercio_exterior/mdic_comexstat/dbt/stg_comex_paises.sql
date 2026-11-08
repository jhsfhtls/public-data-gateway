with source as (
    select * from {{ source('mdic_comexstat', 'comex_paises') }}
)

select
    cast(codigo_pais as varchar) as codigo_pais,
    cast(iso_alpha3 as varchar) as iso_alpha3,
    cast(nome_pais as varchar) as nome_pais,
    cast(bloco_economico as varchar) as bloco_economico
from source
