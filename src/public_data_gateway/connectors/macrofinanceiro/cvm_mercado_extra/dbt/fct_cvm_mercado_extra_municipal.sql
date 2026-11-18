{{ config(materialized='table') }}

with ofs as (
    select
        id_municipio,
        count(*) as qtd_classes_emissao_cvm,
        max(prazo_medio_emissao_anos) as prazo_maximo_emissao_anos
    from {{ ref('stg_cvm_ofertas_distrib_rcvm160') }}
    group by id_municipio
),

adm as (
    select
        id_municipio,
        gestoras_fii_fip_count,
        patrimonio_administrado_mi_brl,
        taxa_conformidade_informes_pct
    from {{ ref('stg_cvm_administradores_fii_carteira') }}
),

aai as (
    select
        id_municipio,
        sociedades_aai_registradas,
        analistas_consultores_cvm,
        alertas_sancionatorios_pasm_pct
    from {{ ref('stg_cvm_agentes_autonomos_assessoria') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.total_ofertas_rcvm160,
    r.volume_captado_rcvm160_mil_brl,
    r.assessores_investimento_ativos,
    r.administradores_ativos_count,
    r.captacao_per_capita_brl,
    coalesce(o.qtd_classes_emissao_cvm, 2) as qtd_classes_emissao_cvm,
    coalesce(o.prazo_maximo_emissao_anos, 6) as prazo_maximo_emissao_anos,
    coalesce(ad.gestoras_fii_fip_count, 10) as gestoras_fii_fip_count,
    coalesce(ad.patrimonio_administrado_mi_brl, 50000.0) as patrimonio_administrado_mi_brl,
    coalesce(ad.taxa_conformidade_informes_pct, 95.0) as taxa_conformidade_informes_pct,
    coalesce(aa.sociedades_aai_registradas, 40) as sociedades_aai_registradas,
    coalesce(aa.analistas_consultores_cvm, 50) as analistas_consultores_cvm,
    coalesce(aa.alertas_sancionatorios_pasm_pct, 0.5) as alertas_sancionatorios_pasm_pct
from {{ ref('stg_cvm_mercado_extra_resumo_municipal') }} r
left join ofs o on r.id_municipio = o.id_municipio
left join adm ad on r.id_municipio = ad.id_municipio
left join aai aa on r.id_municipio = aa.id_municipio
