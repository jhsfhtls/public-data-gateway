with atv as (
    select * from {{ ref('stg_bcb_ifdata_ativo') }}
),
pas as (
    select * from {{ ref('stg_bcb_ifdata_passivo') }}
),
cad as (
    select
        cod_inst,
        nome_instituicao,
        segmento_prudencial
    from {{ ref('stg_bcb_ifdata_cadastro') }}
)

select
    atv.cnpj_14_lider,
    atv.cod_inst,
    cad.nome_instituicao,
    cad.segmento_prudencial,
    atv.data_base,
    atv.disponibilidades,
    atv.aplicacoes_interfinanceiras,
    atv.titulos_valores_mobiliarios,
    atv.carteira_credito_classificada,
    atv.outros_creditos_operacionais,
    atv.ativo_permanente,
    atv.ativo_total,
    pas.depositos_vista,
    pas.depositos_prazo,
    pas.depositos_poupanca,
    pas.depositos_totais,
    pas.captacoes_mercado_aberto,
    pas.patrimonio_liquido,
    pas.patrimonio_referencia,
    pas.passivo_total_exigivel
from atv
inner join pas on atv.cod_inst = pas.cod_inst and atv.data_base = pas.data_base
left join cad on atv.cod_inst = cad.cod_inst
