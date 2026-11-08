with raw_pas as (
    select * from {{ source('bcb_ifdata', 'bcb_ifdata_passivo') }}
)

select
    cast(cnpj_14_lider as varchar) as cnpj_14_lider,
    cast(cod_inst as varchar) as cod_inst,
    cast(data_base as varchar) as data_base,
    cast(depositos_vista as double) as depositos_vista,
    cast(depositos_prazo as double) as depositos_prazo,
    cast(depositos_poupanca as double) as depositos_poupanca,
    cast(depositos_totais as double) as depositos_totais,
    cast(captacoes_mercado_aberto as double) as captacoes_mercado_aberto,
    cast(patrimonio_liquido as double) as patrimonio_liquido,
    cast(patrimonio_referencia as double) as patrimonio_referencia,
    cast(passivo_total_exigivel as double) as passivo_total_exigivel
from raw_pas
