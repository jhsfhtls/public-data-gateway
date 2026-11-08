with raw_atv as (
    select * from {{ source('bcb_ifdata', 'bcb_ifdata_ativo') }}
)

select
    cast(cnpj_14_lider as varchar) as cnpj_14_lider,
    cast(cod_inst as varchar) as cod_inst,
    cast(data_base as varchar) as data_base,
    cast(disponibilidades as double) as disponibilidades,
    cast(aplicacoes_interfinanceiras as double) as aplicacoes_interfinanceiras,
    cast(titulos_valores_mobiliarios as double) as titulos_valores_mobiliarios,
    cast(carteira_credito_classificada as double) as carteira_credito_classificada,
    cast(outros_creditos_operacionais as double) as outros_creditos_operacionais,
    cast(ativo_permanente as double) as ativo_permanente,
    cast(ativo_total as double) as ativo_total
from raw_atv
