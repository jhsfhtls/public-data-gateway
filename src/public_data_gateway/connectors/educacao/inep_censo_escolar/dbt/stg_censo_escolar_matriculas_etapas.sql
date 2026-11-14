{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(etapa_ensino as varchar) as etapa_ensino,
    cast(matriculas_totais_etapa as integer) as matriculas_totais_etapa,
    cast(matriculas_rede_publica_etapa as integer) as matriculas_rede_publica_etapa,
    cast(matriculas_tempo_integral_etapa as integer) as matriculas_tempo_integral_etapa,
    cast(proporcao_tempo_integral_pct as double) as proporcao_tempo_integral_pct,
    cast(participacao_etapa_total_pct as double) as participacao_etapa_total_pct,
    cast(media_alunos_por_turma as double) as media_alunos_por_turma,
    cast(taxa_distorcao_idade_serie_pct as double) as taxa_distorcao_idade_serie_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_escolar_silver', 'censo_escolar_matriculas_etapas') }}
