{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(total_internacoes_aih as integer) as total_internacoes_aih,
    cast(internacoes_carater_urgencia as integer) as internacoes_carater_urgencia,
    cast(internacoes_carater_eletivo as integer) as internacoes_carater_eletivo,
    cast(proporcao_urgencia_pct as double) as proporcao_urgencia_pct,
    cast(diarias_hospitalares_totais as integer) as diarias_hospitalares_totais,
    cast(tempo_medio_permanencia_dias as double) as tempo_medio_permanencia_dias,
    cast(internacoes_por_1000_hab_ano as double) as internacoes_por_1000_hab_ano,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sih_silver', 'sih_internacoes_aih_municipio') }}
