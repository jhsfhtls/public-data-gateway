{{ config(materialized='table') }}

with precos as (
    select
        id_municipio,
        count(*) as qtd_produtos_coletados_anp,
        round(avg(margem_media_revenda_brl), 3) as margem_media_revenda_brl,
        sum(qtd_postos_pesquisados) as qtd_amostras_postos_anp
    from {{ ref('stg_anp_precos_revenda_municipio') }}
    group by id_municipio
),

vendas as (
    select
        id_municipio,
        round(sum(case when categoria_combustivel = 'Etanol_Biodiesel_B14' then volume_vendido_m3 else 0.0 end), 2) as volume_biocombustivel_m3,
        round(sum(volume_vendido_m3), 2) as volume_total_comercializado_m3
    from {{ ref('stg_anp_vendas_derivados_estado') }}
    group by id_municipio
),

postos as (
    select
        id_municipio,
        qtd_postos_ativos_anp,
        participacao_bandeira_branca_pct,
        taxa_conformidade_pmqc_pct,
        capacidade_armazenagem_m3
    from {{ ref('stg_anp_postos_revendedores_resumo') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.preco_gasolina_brl,
    r.preco_etanol_brl,
    r.preco_diesel_s10_brl,
    r.razao_etanol_gasolina_pct,
    r.participacao_biocombustivel_pct,
    r.volume_diesel_m3,
    coalesce(p.qtd_postos_ativos_anp, 0) as qtd_postos_ativos_anp,
    coalesce(p.participacao_bandeira_branca_pct, 40.0) as participacao_bandeira_branca_pct,
    coalesce(p.taxa_conformidade_pmqc_pct, 96.0) as taxa_conformidade_pmqc_pct,
    coalesce(p.capacidade_armazenagem_m3, 0.0) as capacidade_armazenagem_m3,
    coalesce(v.volume_biocombustivel_m3, 0.0) as volume_biocombustivel_m3,
    coalesce(v.volume_total_comercializado_m3, 0.0) as volume_total_comercializado_m3,
    coalesce(pr.qtd_produtos_coletados_anp, 0) as qtd_produtos_coletados_anp,
    coalesce(pr.margem_media_revenda_brl, 0.0) as margem_media_revenda_brl
from {{ ref('stg_anp_combustiveis_resumo_municipal') }} r
left join postos p on r.id_municipio = p.id_municipio
left join vendas v on r.id_municipio = v.id_municipio
left join precos pr on r.id_municipio = pr.id_municipio
