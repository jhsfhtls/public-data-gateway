{{ config(materialized='table') }}

with posicao as (
    select * from {{ ref('stg_pnad_posicao_ocupacao') }}
),

demografia as (
    select * from {{ ref('stg_pnad_demografia_educacao_trabalho') }}
)

select
    p.id_uf,
    p.sigla_uf,
    p.nome_uf,
    p.ano_trimestre,
    p.empregado_com_carteira_clt_mil,
    p.empregado_sem_carteira_mil,
    p.trabalhador_domestico_mil,
    p.empregado_setor_publico_mil,
    p.empregador_mil,
    p.conta_propria_mil,
    p.taxa_informalidade_pct,
    d.taxa_desocupacao_homens_pct,
    d.taxa_desocupacao_mulheres_pct,
    d.taxa_desocupacao_jovens_18_24_pct,
    d.taxa_desocupacao_ensino_superior_pct,
    d.razao_rendimento_mulher_homem,
    p.reference_date,
    p.extracted_at
from posicao p
left join demografia d
    on p.id_uf = d.id_uf
    and p.ano_trimestre = d.ano_trimestre
