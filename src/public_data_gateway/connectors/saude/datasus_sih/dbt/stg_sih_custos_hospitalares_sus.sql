{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(valor_total_aih_brl as double) as valor_total_aih_brl,
    cast(valor_servicos_hospitalares_brl as double) as valor_servicos_hospitalares_brl,
    cast(valor_servicos_profissionais_brl as double) as valor_servicos_profissionais_brl,
    cast(valor_uti_alta_complexidade_brl as double) as valor_uti_alta_complexidade_brl,
    cast(ticket_medio_aih_brl as double) as ticket_medio_aih_brl,
    cast(custo_medio_diaria_brl as double) as custo_medio_diaria_brl,
    cast(proporcao_custo_uti_pct as double) as proporcao_custo_uti_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sih_silver', 'sih_custos_hospitalares_sus') }}
