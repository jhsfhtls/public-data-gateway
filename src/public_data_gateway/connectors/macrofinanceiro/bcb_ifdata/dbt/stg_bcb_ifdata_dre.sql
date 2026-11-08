with raw_dre as (
    select * from {{ source('bcb_ifdata', 'bcb_ifdata_dre') }}
)

select
    cast(cnpj_14_lider as varchar) as cnpj_14_lider,
    cast(cod_inst as varchar) as cod_inst,
    cast(data_base as varchar) as data_base,
    cast(receitas_intermediacao_financeira as double) as receitas_intermediacao_financeira,
    cast(despesas_intermediacao_financeira as double) as despesas_intermediacao_financeira,
    cast(margem_financeira_bruta as double) as margem_financeira_bruta,
    cast(despesas_provisao_pdd as double) as despesas_provisao_pdd,
    cast(resultado_operacional as double) as resultado_operacional,
    cast(lucro_liquido_trimestral as double) as lucro_liquido_trimestral
from raw_dre
