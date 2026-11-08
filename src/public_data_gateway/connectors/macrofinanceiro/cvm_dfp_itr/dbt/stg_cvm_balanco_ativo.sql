with source as (
    select * from {{ source('cvm_dfp_itr', 'cvm_dfp_balanco_ativo') }}
)

select
    id_demonstracao,
    codigo_cvm,
    cnpj_14,
    cast(ano_exercicio as integer) as ano_exercicio,
    cast(data_referencia as date) as data_referencia,
    tipo_demonstracao,
    cast(versao_documento as integer) as versao_documento,
    cast(ativo_total as double) as ativo_total,
    cast(ativo_circulante as double) as ativo_circulante,
    cast(caixa_equivalentes as double) as caixa_equivalentes,
    cast(aplicacoes_financeiras as double) as aplicacoes_financeiras,
    cast(contas_a_receber as double) as contas_a_receber,
    cast(estoques as double) as estoques,
    cast(ativo_nao_circulante as double) as ativo_nao_circulante,
    cast(imobilizado as double) as imobilizado,
    cast(intangivel as double) as intangivel,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp
from source
