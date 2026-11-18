{{ config(materialized='table') }}

with lic as (
    select
        id_municipio,
        count(*) as qtd_temas_indexados_okbr
    from {{ ref('stg_okbr_diarios_licitacoes_dispensas') }}
    group by id_municipio
),

exc as (
    select
        id_municipio,
        mencoes_educacao_saude_12m,
        mencoes_esg_clima_12m
    from {{ ref('stg_okbr_diarios_excertos_tematicos') }}
),

trp as (
    select
        id_municipio,
        cobertura_historica_anos,
        formato_aberto_pdf_txt_pct,
        regularidade_publicacao_pct
    from {{ ref('stg_okbr_diarios_transparencia_ativa') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.edicoes_indexadas_12m,
    r.qtd_atos_licitatorios_12m,
    r.valor_extratos_citados_mil_brl,
    r.taxa_ocr_legivel_pct,
    r.indice_transparencia_diario_pct,
    coalesce(l.qtd_temas_indexados_okbr, 2) as qtd_temas_indexados_okbr,
    coalesce(e.mencoes_educacao_saude_12m, 500) as mencoes_educacao_saude_12m,
    coalesce(e.mencoes_esg_clima_12m, 200) as mencoes_esg_clima_12m,
    coalesce(t.cobertura_historica_anos, 8) as cobertura_historica_anos,
    coalesce(t.formato_aberto_pdf_txt_pct, 90.0) as formato_aberto_pdf_txt_pct,
    coalesce(t.regularidade_publicacao_pct, 95.0) as regularidade_publicacao_pct
from {{ ref('stg_okbr_querido_diario_resumo_municipal') }} r
left join lic l on r.id_municipio = l.id_municipio
left join exc e on r.id_municipio = e.id_municipio
left join trp t on r.id_municipio = t.id_municipio
