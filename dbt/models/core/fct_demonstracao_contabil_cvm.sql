with source as (
    select * from {{ ref('stg_cvm_fluxo_caixa') }}
)

select
    id_demonstracao,
    codigo_cvm,
    cnpj_14,
    ano_exercicio,
    data_referencia,
    tipo_demonstracao,
    fluxo_caixa_operacional,
    fluxo_caixa_investimento,
    fluxo_caixa_financiamento,
    variacao_liquida_caixa,
    case
        when fluxo_caixa_operacional > 0 then true
        else false
    end as is_fco_positivo,
    ingestion_timestamp
from source
