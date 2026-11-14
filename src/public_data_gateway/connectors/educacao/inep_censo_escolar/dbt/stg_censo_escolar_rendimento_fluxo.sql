{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(total_matriculas_educacao_basica as integer) as total_matriculas_educacao_basica,
    cast(matriculas_tempo_integral_total as integer) as matriculas_tempo_integral_total,
    cast(proporcao_tempo_integral_total_pct as double) as proporcao_tempo_integral_total_pct,
    cast(taxa_aprovacao_media_pct as double) as taxa_aprovacao_media_pct,
    cast(taxa_reprovacao_media_pct as double) as taxa_reprovacao_media_pct,
    cast(taxa_abandono_escolar_pct as double) as taxa_abandono_escolar_pct,
    cast(taxa_distorcao_idade_serie_media_pct as double) as taxa_distorcao_idade_serie_media_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_escolar_silver', 'censo_escolar_rendimento_fluxo') }}
