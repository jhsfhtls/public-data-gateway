with source as (
    select * from {{ source('cvm_dfp_itr', 'cvm_dfp_fluxo_caixa') }}
)

select
    id_demonstracao,
    codigo_cvm,
    cnpj_14,
    cast(ano_exercicio as integer) as ano_exercicio,
    cast(data_referencia as date) as data_referencia,
    tipo_demonstracao,
    cast(fluxo_caixa_operacional as double) as fluxo_caixa_operacional,
    cast(fluxo_caixa_investimento as double) as fluxo_caixa_investimento,
    cast(fluxo_caixa_financiamento as double) as fluxo_caixa_financiamento,
    cast(variacao_liquida_caixa as double) as variacao_liquida_caixa,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp
from source
