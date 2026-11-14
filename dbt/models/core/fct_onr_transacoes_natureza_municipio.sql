{{ config(materialized='table') }}

with transacoes as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        id_uf,
        cep_8,
        competencia_referencia,
        natureza_ato_registral,
        categoria_operacao,
        quantidade_atos_registrados_ano,
        volume_financeiro_declarado_milhoes_brl,
        ticket_medio_ato_brl,
        proporcao_protocolo_eletronico_srei_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_onr_transacoes_atos_natureza') }}
),

municipios as (
    select
        id_municipio,
        nome_uf,
        regiao
    from {{ ref('dim_municipio') }}
)

select
    t.id_municipio,
    t.nome_municipio,
    t.sigla_uf,
    t.id_uf,
    coalesce(m.nome_uf, t.sigla_uf) as nome_uf,
    coalesce(m.regiao, 'Brasil') as regiao,
    t.cep_8,
    t.competencia_referencia,
    t.natureza_ato_registral,
    t.categoria_operacao,
    t.quantidade_atos_registrados_ano,
    t.volume_financeiro_declarado_milhoes_brl,
    t.ticket_medio_ato_brl,
    t.proporcao_protocolo_eletronico_srei_pct,
    t.reference_date,
    t.extracted_at
from transacoes t
left join municipios m on t.id_municipio = m.id_municipio
