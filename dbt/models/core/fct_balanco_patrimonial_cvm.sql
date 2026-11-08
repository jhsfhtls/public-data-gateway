with bpa as (
    select * from {{ ref('stg_cvm_balanco_ativo') }}
),

bpp as (
    select * from {{ ref('stg_cvm_balanco_passivo') }}
)

select
    bpa.id_demonstracao,
    bpa.codigo_cvm,
    bpa.cnpj_14,
    bpa.ano_exercicio,
    bpa.data_referencia,
    bpa.tipo_demonstracao,
    bpa.versao_documento,
    bpa.ativo_total,
    bpa.ativo_circulante,
    bpa.caixa_equivalentes,
    bpa.aplicacoes_financeiras,
    bpa.contas_a_receber,
    bpa.estoques,
    bpa.ativo_nao_circulante,
    bpa.imobilizado,
    bpa.intangivel,
    coalesce(bpp.passivo_total, bpa.ativo_total) as passivo_total,
    coalesce(bpp.passivo_circulante, 0.0) as passivo_circulante,
    coalesce(bpp.fornecedores, 0.0) as fornecedores,
    coalesce(bpp.emprestimos_financiamentos_cp, 0.0) as emprestimos_financiamentos_cp,
    coalesce(bpp.passivo_nao_circulante, 0.0) as passivo_nao_circulante,
    coalesce(bpp.emprestimos_financiamentos_lp, 0.0) as emprestimos_financiamentos_lp,
    coalesce(bpp.patrimonio_liquido, 0.0) as patrimonio_liquido,
    coalesce(bpp.capital_social_integralizado, 0.0) as capital_social_integralizado,
    coalesce(bpp.reservas_lucros, 0.0) as reservas_lucros,
    bpa.ingestion_timestamp
from bpa
left join bpp
  on bpa.codigo_cvm = bpp.codigo_cvm
 and bpa.ano_exercicio = bpp.ano_exercicio
 and bpa.tipo_demonstracao = bpp.tipo_demonstracao
