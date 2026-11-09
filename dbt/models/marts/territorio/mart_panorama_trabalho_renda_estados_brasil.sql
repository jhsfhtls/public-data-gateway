{{ config(materialized='table') }}

with mercado as (
    select * from {{ ref('fct_pnad_mercado_trabalho_uf') }}
),

posicao as (
    select * from {{ ref('fct_pnad_posicao_informalidade_uf') }}
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
    m.taxa_desocupacao_pct,
    m.taxa_participacao_forca_pct,
    m.taxa_nivel_ocupacao_pct,
    m.rendimento_medio_real_habitual_reais,
    m.rendimento_medio_real_efetivo_reais,
    m.massa_rendimento_real_habitual_milhoes_reais,
    m.taxa_composta_subutilizacao_pct,
    m.populacao_desalentada_mil,
    m.taxa_desalento_pct,
    p.empregado_com_carteira_clt_mil,
    p.empregado_sem_carteira_mil,
    p.taxa_informalidade_pct,
    p.taxa_desocupacao_homens_pct,
    p.taxa_desocupacao_mulheres_pct,
    p.taxa_desocupacao_jovens_18_24_pct,
    p.razao_rendimento_mulher_homem,
    round(
        least(
            100.0,
            greatest(
                0.0,
                ((15.0 - least(15.0, m.taxa_desocupacao_pct)) * 3.5)
                + ((m.rendimento_medio_real_habitual_reais / 5000.0) * 35.0)
                + ((65.0 - least(65.0, p.taxa_informalidade_pct)) * 0.45)
            )
        ),
        2
    ) as score_resiliencia_mercado_trabalho,
    case
        when m.taxa_desocupacao_pct <= 5.0 and p.taxa_informalidade_pct <= 32.0 and m.rendimento_medio_real_habitual_reais >= 3500.0
            then 'MERCADO_AQUECIDO_ALTA_RENDA'
        when m.taxa_desocupacao_pct <= 7.0 and p.taxa_informalidade_pct <= 38.0
            then 'MERCADO_RESILIENTE'
        when p.taxa_informalidade_pct >= 50.0 or m.taxa_desocupacao_pct >= 10.0
            then 'ALTA_VULNERABILIDADE_INFORMAL'
        else 'MERCADO_EQUILIBRADO'
    end as classificacao_mercado_trabalho_uf,
    m.reference_date,
    m.extracted_at
from mercado m
left join posicao p
    on m.id_uf = p.id_uf
    and m.ano_trimestre = p.ano_trimestre
