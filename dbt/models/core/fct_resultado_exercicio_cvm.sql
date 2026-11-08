with source as (
    select * from {{ ref('stg_cvm_dre') }}
)

select
    id_demonstracao,
    codigo_cvm,
    cnpj_14,
    ano_exercicio,
    data_referencia,
    tipo_demonstracao,
    versao_documento,
    receita_liquida_vendas,
    custo_bens_servicos_vendidos,
    lucro_bruto,
    despesas_operacionais,
    despesas_vendas_gerais_adm,
    ebitda_calculado,
    resultado_financeiro_liquido,
    lucro_prejuizo_antes_tributos,
    imposto_renda_contribuicao_social,
    lucro_liquido_exercicio,
    case
        when lucro_liquido_exercicio > 0 then true
        else false
    end as is_exercicio_lucrativo,
    ingestion_timestamp
from source
