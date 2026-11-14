{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_exercicio_fiscal as bigint) as ano_exercicio_fiscal,
    cast(segmento_imovel_itbi as varchar) as segmento_imovel_itbi,
    cast(categoria_uso_imovel as varchar) as categoria_uso_imovel,
    cast(quantidade_transacoes_itbi_ano as bigint) as quantidade_transacoes_itbi_ano,
    cast(valor_transacionado_itbi_milhoes_brl as double) as valor_transacionado_itbi_milhoes_brl,
    cast(valor_medio_transacao_mil_brl as double) as valor_medio_transacao_mil_brl,
    cast(aliquota_nominal_itbi_pct as double) as aliquota_nominal_itbi_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('capitais_iptu_itbi_silver', 'itbi_transacoes_imobiliarias_padrao') }}
