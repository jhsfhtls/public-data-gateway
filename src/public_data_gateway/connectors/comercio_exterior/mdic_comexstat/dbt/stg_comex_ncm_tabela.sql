with source as (
    select * from {{ source('mdic_comexstat', 'comex_ncm_tabela') }}
)

select
    cast(ncm_8 as varchar) as ncm_8,
    cast(codigo_unidade as varchar) as codigo_unidade,
    cast(sh6 as varchar) as sh6,
    cast(descricao_ncm as varchar) as descricao_ncm,
    cast(descricao_ncm_ing as varchar) as descricao_ncm_ing,
    cast(descricao_ncm_esp as varchar) as descricao_ncm_esp
from source
