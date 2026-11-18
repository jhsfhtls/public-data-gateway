{{ config(materialized='table') }}

with emp as (
    select
        id_municipio,
        count(*) as qtd_empreendimentos_amostra_mmgd,
        round(sum(potencia_instalada_kw), 2) as potencia_amostra_kw,
        sum(qtd_unidades_beneficiarias) as ucs_beneficiarias_amostra
    from {{ ref('stg_aneel_empreendimentos_mmgd') }}
    group by id_municipio
),

fontes as (
    select
        id_municipio,
        round(sum(geracao_estimada_mwh_ano), 2) as geracao_estimada_mwh_ano,
        round(sum(case when fonte_geracao = 'Solar_Fotovoltaica' then potencia_instalada_kw else 0.0 end), 2) as potencia_solar_fontes_kw,
        round(sum(case when fonte_geracao = 'Biogas_Biomassa_Agro' then potencia_instalada_kw else 0.0 end), 2) as potencia_bioenergia_kw
    from {{ ref('stg_aneel_geracao_fonte_municipio') }}
    group by id_municipio
),

rede as (
    select
        id_municipio,
        prazo_medio_vistoria_dias,
        taxa_aprovacao_parecer_pct,
        inversao_fluxo_critica_pct,
        ucs_beneficiarias_total
    from {{ ref('stg_aneel_distribuidoras_rede_mmgd') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.distribuidora_principal,
    r.qtd_usinas_mmgd,
    r.potencia_instalada_total_kw,
    r.potencia_solar_ufv_kw,
    r.potencia_rural_agro_kw,
    r.ucs_com_credito_energia,
    r.participacao_solar_pct,
    coalesce(rd.prazo_medio_vistoria_dias, 12.0) as prazo_medio_vistoria_dias,
    coalesce(rd.taxa_aprovacao_parecer_pct, 92.0) as taxa_aprovacao_parecer_pct,
    coalesce(rd.inversao_fluxo_critica_pct, 12.0) as inversao_fluxo_critica_pct,
    coalesce(rd.ucs_beneficiarias_total, r.ucs_com_credito_energia) as ucs_beneficiarias_total,
    coalesce(f.geracao_estimada_mwh_ano, 0.0) as geracao_estimada_mwh_ano,
    coalesce(f.potencia_bioenergia_kw, 0.0) as potencia_bioenergia_kw,
    coalesce(e.qtd_empreendimentos_amostra_mmgd, 0) as qtd_empreendimentos_amostra_mmgd,
    coalesce(e.potencia_amostra_kw, 0.0) as potencia_amostra_kw
from {{ ref('stg_aneel_mmgd_resumo_municipal') }} r
left join rede rd on r.id_municipio = rd.id_municipio
left join fontes f on r.id_municipio = f.id_municipio
left join emp e on r.id_municipio = e.id_municipio
