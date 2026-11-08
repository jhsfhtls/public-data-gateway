with dre as (
    select * from {{ ref('stg_bcb_ifdata_dre') }}
),
cad as (
    select
        cod_inst,
        nome_instituicao,
        segmento_prudencial
    from {{ ref('stg_bcb_ifdata_cadastro') }}
)

select
    dre.cnpj_14_lider,
    dre.cod_inst,
    cad.nome_instituicao,
    cad.segmento_prudencial,
    dre.data_base,
    dre.receitas_intermediacao_financeira,
    dre.despesas_intermediacao_financeira,
    dre.margem_financeira_bruta,
    dre.despesas_provisao_pdd,
    dre.resultado_operacional,
    dre.lucro_liquido_trimestral
from dre
left join cad on dre.cod_inst = cad.cod_inst
