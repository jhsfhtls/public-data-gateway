{{ config(materialized='table') }}

with etapas as (
    select
        id_municipio,
        nome_municipio,
        sigla_uf,
        etapa_ensino,
        matriculas_totais_etapa,
        matriculas_rede_publica_etapa,
        matriculas_tempo_integral_etapa,
        proporcao_tempo_integral_pct,
        participacao_etapa_total_pct,
        media_alunos_por_turma,
        taxa_distorcao_idade_serie_pct,
        reference_date,
        extracted_at
    from {{ ref('stg_censo_escolar_matriculas_etapas') }}
)

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    etapa_ensino,
    matriculas_totais_etapa,
    matriculas_rede_publica_etapa,
    round((matriculas_rede_publica_etapa * 100.0) / nullif(matriculas_totais_etapa, 0), 2) as proporcao_rede_publica_etapa_pct,
    matriculas_tempo_integral_etapa,
    proporcao_tempo_integral_pct,
    participacao_etapa_total_pct,
    media_alunos_por_turma,
    taxa_distorcao_idade_serie_pct,
    reference_date,
    extracted_at
from etapas
