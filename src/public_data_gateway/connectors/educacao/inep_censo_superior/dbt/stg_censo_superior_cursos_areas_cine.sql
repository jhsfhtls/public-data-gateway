{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(area_cine_rotulo as varchar) as area_cine_rotulo,
    cast(cursos_graduacao_ativos as integer) as cursos_graduacao_ativos,
    cast(matriculas_ativas_area as integer) as matriculas_ativas_area,
    cast(concluintes_anuais_area as integer) as concluintes_anuais_area,
    cast(participacao_matriculas_area_pct as double) as participacao_matriculas_area_pct,
    cast(proporcao_modalidade_ead_area_pct as double) as proporcao_modalidade_ead_area_pct,
    cast(conceito_enade_medio_area as double) as conceito_enade_medio_area,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_superior_silver', 'censo_superior_cursos_areas_cine') }}
