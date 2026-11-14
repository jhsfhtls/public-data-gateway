{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(total_matriculas_graduacao as integer) as total_matriculas_graduacao,
    cast(matriculas_presenciais as integer) as matriculas_presenciais,
    cast(matriculas_ead_polo as integer) as matriculas_ead_polo,
    cast(proporcao_matriculas_ead_pct as double) as proporcao_matriculas_ead_pct,
    cast(ingressantes_anuais_total as integer) as ingressantes_anuais_total,
    cast(concluintes_anuais_total as integer) as concluintes_anuais_total,
    cast(taxa_conclusao_acumulada_tcg_pct as double) as taxa_conclusao_acumulada_tcg_pct,
    cast(taxa_desistencia_anual_tda_pct as double) as taxa_desistencia_anual_tda_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_superior_silver', 'censo_superior_matriculas_ingressantes_concluintes') }}
