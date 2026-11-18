{{ config(materialized='table') }}

with muni as (
    select * from {{ ref('stg_capag_avaliacao_municipal') }}
),

est as (
    select * from {{ ref('stg_capag_avaliacao_estadual') }}
),

ifgf as (
    select * from {{ ref('stg_capag_ifgf_gestao_fiscal') }}
),

gar as (
    select * from {{ ref('stg_capag_garantias_operacoes') }}
)

select
    m.id_municipio,
    m.sigla_uf,
    m.nome_municipio,
    m.ano_referencia,
    m.nota_capag_oficial,
    m.indicador_1_endividamento_dcl_rcl_pct,
    m.nota_ind_1,
    m.indicador_2_poupanca_corrente_pct,
    m.nota_ind_2,
    m.indicador_3_liquidez_relativa_pct,
    m.nota_ind_3,
    m.elegivel_garantia_uniao,
    m.qualidade_informacao_ranking_icf,
    e.nota_capag_uf,
    e.elegivel_garantia_uniao_uf,
    e.espaco_fiscal_estimado_brl,
    i.ifgf_geral,
    i.ifgf_autonomia,
    i.ifgf_gastos_pessoal,
    i.ifgf_liquidez,
    i.ifgf_investimentos,
    i.conceito_ifgf,
    g.estoque_garantias_uniao_brl,
    g.operacoes_contratadas_com_garantia,
    g.custo_medio_ponderado_aa_pct,
    g.honras_aval_acumuladas_brl,
    g.status_adimplencia_garantias
from muni m
left join est e on m.id_municipio = e.id_municipio
left join ifgf i on m.id_municipio = i.id_municipio
left join gar g on m.id_municipio = g.id_municipio
