with source as (
    select * from {{ source('cvm_dfp_itr', 'cvm_dfp_balanco_passivo') }}
)

select
    id_demonstracao,
    codigo_cvm,
    cnpj_14,
    cast(ano_exercicio as integer) as ano_exercicio,
    cast(data_referencia as date) as data_referencia,
    tipo_demonstracao,
    cast(versao_documento as integer) as versao_documento,
    cast(passivo_total as double) as passivo_total,
    cast(passivo_circulante as double) as passivo_circulante,
    cast(fornecedores as double) as fornecedores,
    cast(emprestimos_financiamentos_cp as double) as emprestimos_financiamentos_cp,
    cast(passivo_nao_circulante as double) as passivo_nao_circulante,
    cast(emprestimos_financiamentos_lp as double) as emprestimos_financiamentos_lp,
    cast(patrimonio_liquido as double) as patrimonio_liquido,
    cast(capital_social_integralizado as double) as capital_social_integralizado,
    cast(reservas_lucros as double) as reservas_lucros,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp
from source
