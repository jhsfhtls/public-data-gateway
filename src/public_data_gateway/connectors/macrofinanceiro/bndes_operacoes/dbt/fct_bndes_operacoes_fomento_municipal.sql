{{ config(materialized='table') }}

with ops as (
    select
        id_municipio,
        count(*) as qtd_modalidades_bndes,
        max(prazo_medio_amortizacao_meses) as prazo_maximo_amortizacao_meses
    from {{ ref('stg_bndes_operacoes_credito_municipio') }}
    group by id_municipio
),

desemb as (
    select
        id_municipio,
        mpme_participacao_pct,
        ano_referencia
    from {{ ref('stg_bndes_desembolsos_setoriais_uf') }}
),

risco as (
    select
        id_municipio,
        juros_medio_tjlp_tlp_aa_pct,
        inadimplencia_carteira_bndes_pct,
        cobertura_fgi_garantias_pct,
        projetos_sustentaveis_count
    from {{ ref('stg_bndes_carteira_risco_garantias') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.total_operacoes_bndes,
    r.valor_total_contratado_mil_brl,
    r.desembolso_anual_mil_brl,
    r.fomento_verde_esg_pct,
    r.fomento_per_capita_brl,
    coalesce(o.qtd_modalidades_bndes, 2) as qtd_modalidades_bndes,
    coalesce(o.prazo_maximo_amortizacao_meses, 84) as prazo_maximo_amortizacao_meses,
    coalesce(d.mpme_participacao_pct, 40.0) as mpme_participacao_pct,
    coalesce(d.ano_referencia, 2025) as ano_referencia,
    coalesce(rk.juros_medio_tjlp_tlp_aa_pct, 7.8) as juros_medio_tjlp_tlp_aa_pct,
    coalesce(rk.inadimplencia_carteira_bndes_pct, 0.50) as inadimplencia_carteira_bndes_pct,
    coalesce(rk.cobertura_fgi_garantias_pct, 80.0) as cobertura_fgi_garantias_pct,
    coalesce(rk.projetos_sustentaveis_count, 50) as projetos_sustentaveis_count
from {{ ref('stg_bndes_operacoes_resumo_municipal') }} r
left join ops o on r.id_municipio = o.id_municipio
left join desemb d on r.id_municipio = d.id_municipio
left join risco rk on r.id_municipio = rk.id_municipio
