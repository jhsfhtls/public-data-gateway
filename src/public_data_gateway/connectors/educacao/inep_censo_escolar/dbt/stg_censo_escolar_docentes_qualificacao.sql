{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(total_docentes_exercicio as integer) as total_docentes_exercicio,
    cast(docentes_ensino_superior_completo as integer) as docentes_ensino_superior_completo,
    cast(proporcao_docentes_superior_pct as double) as proporcao_docentes_superior_pct,
    cast(docentes_pos_graduacao_pct as double) as docentes_pos_graduacao_pct,
    cast(indicador_adequacao_formacao_docente_afd_pct as double) as indicador_adequacao_formacao_docente_afd_pct,
    cast(razao_alunos_por_docente as double) as razao_alunos_por_docente,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_escolar_silver', 'censo_escolar_docentes_qualificacao') }}
