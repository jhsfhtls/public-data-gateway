{{ config(materialized='table') }}

with tx as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        regiao,
        ano_exercicio_fiscal,
        segmento_imovel_itbi,
        categoria_uso_imovel,
        quantidade_transacoes_itbi_ano,
        valor_transacionado_itbi_milhoes_brl,
        valor_medio_transacao_mil_brl,
        aliquota_nominal_itbi_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_itbi_transacoes_imobiliarias_padrao') }}
),

municipios as (
    select
        id_municipio,
        nome_uf
    from {{ ref('dim_municipio') }}
)

select
    t.id_municipio,
    t.nome_municipio,
    t.sigla_uf,
    t.id_uf,
    m.nome_uf,
    t.regiao,
    t.ano_exercicio_fiscal,
    t.segmento_imovel_itbi,
    t.categoria_uso_imovel,
    t.quantidade_transacoes_itbi_ano,
    t.valor_transacionado_itbi_milhoes_brl,
    t.valor_medio_transacao_mil_brl,
    t.aliquota_nominal_itbi_pct,
    t.reference_date,
    t.extracted_at
from tx t
inner join municipios m on t.id_municipio = m.id_municipio
