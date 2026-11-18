{{ config(materialized='table') }}

with entes as (
    select * from {{ ref('stg_siconfi_entes') }}
),

dca as (
    select * from {{ ref('stg_siconfi_dca_balanco') }}
    qualify row_number() over (partition by id_municipio order by ano_exercicio desc) = 1
),

rreo as (
    select * from {{ ref('stg_siconfi_rreo_execucao') }}
    qualify row_number() over (partition by id_municipio order by ano_exercicio desc, bimestre desc) = 1
),

rgf as (
    select * from {{ ref('stg_siconfi_rgf_limites') }}
    qualify row_number() over (partition by id_municipio order by ano_exercicio desc, quadrimestre desc) = 1
),

extratos as (
    select
        id_municipio,
        count(*) as total_entregas_monitoradas,
        sum(case when status_entrega = 'HOMOLOGADO' then 1 else 0 end) as entregas_homologadas
    from {{ ref('stg_siconfi_extratos_entregas') }}
    group by id_municipio
)

select
    e.id_municipio,
    e.sigla_uf,
    e.nome_ente,
    e.esfera,
    e.regiao,
    e.populacao,
    e.cnpj_14,
    e.ano_exercicio,
    e.is_capital,
    d.ativo_total_brl,
    d.passivo_total_brl,
    d.patrimonio_liquido_brl,
    d.receita_orcamentaria_realizada_brl,
    d.despesa_orcamentaria_empenhada_brl,
    d.resultado_orcamentario_brl,
    r.receita_corrente_liquida_brl,
    r.receita_tributaria_propria_brl,
    r.transferencias_correntes_brl,
    r.despesa_capital_investimento_brl,
    r.resultado_primario_brl,
    g.despesa_total_pessoal_brl,
    g.despesa_pessoal_rcl_pct,
    g.limite_prudencial_pessoal_pct,
    g.limite_maximo_pessoal_pct,
    g.divida_consolidada_liquida_brl,
    g.status_alerta_lrf,
    round(
        case
            when coalesce(r.receita_corrente_liquida_brl, 0.0) > 0
            then (r.receita_tributaria_propria_brl / r.receita_corrente_liquida_brl) * 100.0
            else 0.0
        end,
        2
    ) as autonomia_tributaria_pct,
    round(
        case
            when coalesce(r.receita_corrente_liquida_brl, 0.0) > 0
            then (r.despesa_capital_investimento_brl / r.receita_corrente_liquida_brl) * 100.0
            else 0.0
        end,
        2
    ) as taxa_investimento_rcl_pct,
    coalesce(x.total_entregas_monitoradas, 0) as total_entregas_monitoradas,
    coalesce(x.entregas_homologadas, 0) as entregas_homologadas,
    round(
        case
            when coalesce(x.total_entregas_monitoradas, 0) > 0
            then (cast(x.entregas_homologadas as double) / x.total_entregas_monitoradas) * 100.0
            else 0.0
        end,
        2
    ) as indice_adimplencia_siconfi_pct
from entes e
left join dca d on e.id_municipio = d.id_municipio
left join rreo r on e.id_municipio = r.id_municipio
left join rgf g on e.id_municipio = g.id_municipio
left join extratos x on e.id_municipio = x.id_municipio
