{{ config(materialized='table') }}

with mercado as (
    select * from {{ ref('stg_pnad_mercado_trabalho_trimestral') }}
),

rendimento as (
    select * from {{ ref('stg_pnad_rendimento_trabalho') }}
),

subutilizacao as (
    select * from {{ ref('stg_pnad_subutilizacao_forca') }}
)

select
    m.id_uf,
    m.sigla_uf,
    m.nome_uf,
    m.ano_trimestre,
    m.populacao_idade_trabalhar_mil,
    m.forca_trabalho_total_mil,
    m.populacao_ocupada_mil,
    m.populacao_desocupada_mil,
    m.populacao_fora_forca_mil,
    m.taxa_desocupacao_pct,
    m.taxa_participacao_forca_pct,
    m.taxa_nivel_ocupacao_pct,
    r.rendimento_medio_real_habitual_reais,
    r.rendimento_medio_real_efetivo_reais,
    r.massa_rendimento_real_habitual_milhoes_reais,
    s.taxa_composta_subutilizacao_pct,
    s.subocupados_por_insuficiencia_horas_mil,
    s.populacao_desalentada_mil,
    s.taxa_desalento_pct,
    m.reference_date,
    m.extracted_at
from mercado m
left join rendimento r
    on m.id_uf = r.id_uf
    and m.ano_trimestre = r.ano_trimestre
left join subutilizacao s
    on m.id_uf = s.id_uf
    and m.ano_trimestre = s.ano_trimestre
