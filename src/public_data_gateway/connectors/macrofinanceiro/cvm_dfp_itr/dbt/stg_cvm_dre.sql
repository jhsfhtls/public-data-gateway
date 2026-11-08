with source as (
    select * from {{ source('cvm_dfp_itr', 'cvm_dfp_dre') }}
)

select
    id_demonstracao,
    codigo_cvm,
    cnpj_14,
    cast(ano_exercicio as integer) as ano_exercicio,
    cast(data_referencia as date) as data_referencia,
    tipo_demonstracao,
    cast(versao_documento as integer) as versao_documento,
    cast(receita_liquida_vendas as double) as receita_liquida_vendas,
    cast(custo_bens_servicos_vendidos as double) as custo_bens_servicos_vendidos,
    cast(lucro_bruto as double) as lucro_bruto,
    cast(despesas_operacionais as double) as despesas_operacionais,
    cast(despesas_vendas_gerais_adm as double) as despesas_vendas_gerais_adm,
    cast(ebitda_calculado as double) as ebitda_calculado,
    cast(resultado_financeiro_liquido as double) as resultado_financeiro_liquido,
    cast(lucro_prejuizo_antes_tributos as double) as lucro_prejuizo_antes_tributos,
    cast(imposto_renda_contribuicao_social as double) as imposto_renda_contribuicao_social,
    cast(lucro_liquido_exercicio as double) as lucro_liquido_exercicio,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp
from source
