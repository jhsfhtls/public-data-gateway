with raw_ind as (
    select * from {{ source('bcb_ifdata', 'bcb_ifdata_indicadores') }}
)

select
    cast(cnpj_14_lider as varchar) as cnpj_14_lider,
    cast(cod_inst as varchar) as cod_inst,
    cast(data_base as varchar) as data_base,
    cast(indice_basileia_pct as double) as indice_basileia_pct,
    cast(indice_imobilizacao_pct as double) as indice_imobilizacao_pct,
    cast(razao_alavancagem_pct as double) as razao_alavancagem_pct,
    cast(roe_bancario_pct as double) as roe_bancario_pct,
    cast(roa_bancario_pct as double) as roa_bancario_pct,
    cast(taxa_inadimplencia_pct as double) as taxa_inadimplencia_pct
from raw_ind
