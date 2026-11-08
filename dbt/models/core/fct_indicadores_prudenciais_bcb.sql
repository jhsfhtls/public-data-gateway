with ind as (
    select * from {{ ref('stg_bcb_ifdata_indicadores') }}
),
cad as (
    select
        cod_inst,
        nome_instituicao,
        segmento_prudencial
    from {{ ref('stg_bcb_ifdata_cadastro') }}
)

select
    ind.cnpj_14_lider,
    ind.cod_inst,
    cad.nome_instituicao,
    cad.segmento_prudencial,
    ind.data_base,
    ind.indice_basileia_pct,
    ind.indice_imobilizacao_pct,
    ind.razao_alavancagem_pct,
    ind.roe_bancario_pct,
    ind.roa_bancario_pct,
    ind.taxa_inadimplencia_pct
from ind
left join cad on ind.cod_inst = cad.cod_inst
