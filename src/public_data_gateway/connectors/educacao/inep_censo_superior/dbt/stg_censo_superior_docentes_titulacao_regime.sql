{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(total_docentes_exercicio_ies as integer) as total_docentes_exercicio_ies,
    cast(docentes_doutores as integer) as docentes_doutores,
    cast(docentes_mestres as integer) as docentes_mestres,
    cast(proporcao_doutores_pct as double) as proporcao_doutores_pct,
    cast(proporcao_mestres_doutores_stricto_sensu_pct as double) as proporcao_mestres_doutores_stricto_sensu_pct,
    cast(proporcao_regime_tempo_integral_de_pct as double) as proporcao_regime_tempo_integral_de_pct,
    cast(razao_alunos_por_docente_equivalente as double) as razao_alunos_por_docente_equivalente,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_superior_silver', 'censo_superior_docentes_titulacao_regime') }}
