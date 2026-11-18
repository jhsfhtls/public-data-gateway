{{ config(materialized='table') }}

with cand as (
    select
        id_municipio,
        count(*) as qtd_cargos_apurados_tse,
        max(partidos_competitivos_count) as max_partidos_competitivos,
        round(avg(competicao_eleitoral_pct), 2) as competicao_eleitoral_media_pct
    from {{ ref('stg_tse_candidaturas_partidos_municipio') }}
    group by id_municipio
),

vot as (
    select
        id_municipio,
        abstencao_pct,
        votos_validos_pct,
        ano_eleicao
    from {{ ref('stg_tse_votacao_comparecimento_secao') }}
),

contas as (
    select
        id_municipio,
        despesa_contratada_mil_brl,
        contas_aprovadas_pct,
        doadores_cpf_cnpj_distintos
    from {{ ref('stg_tse_prestacao_contas_campanha') }}
)

select
    r.id_municipio,
    r.sigla_uf,
    r.nome_municipio,
    r.regiao,
    r.eleitorado_apto_total,
    r.total_candidaturas_aptas,
    r.comparecimento_pct,
    r.receita_total_campanha_mil_brl,
    r.custo_por_eleitor_brl,
    coalesce(c.qtd_cargos_apurados_tse, 2) as qtd_cargos_apurados_tse,
    coalesce(c.max_partidos_competitivos, 15) as max_partidos_competitivos,
    coalesce(c.competicao_eleitoral_media_pct, 75.0) as competicao_eleitoral_media_pct,
    coalesce(v.abstencao_pct, 25.0) as abstencao_pct,
    coalesce(v.votos_validos_pct, 90.0) as votos_validos_pct,
    coalesce(v.ano_eleicao, 2024) as ano_eleicao,
    coalesce(ct.despesa_contratada_mil_brl, 0.0) as despesa_contratada_mil_brl,
    coalesce(ct.contas_aprovadas_pct, 88.0) as contas_aprovadas_pct,
    coalesce(ct.doadores_cpf_cnpj_distintos, 0) as doadores_cpf_cnpj_distintos
from {{ ref('stg_tse_eleicoes_resumo_municipal') }} r
left join cand c on r.id_municipio = c.id_municipio
left join vot v on r.id_municipio = v.id_municipio
left join contas ct on r.id_municipio = ct.id_municipio
