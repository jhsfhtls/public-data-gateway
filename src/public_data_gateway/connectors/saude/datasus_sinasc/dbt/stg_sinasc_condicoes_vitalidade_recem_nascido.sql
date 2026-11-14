{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(nascidos_baixo_peso_menor_2500g as integer) as nascidos_baixo_peso_menor_2500g,
    cast(proporcao_baixo_peso_nascer_pct as double) as proporcao_baixo_peso_nascer_pct,
    cast(nascidos_prematuros_menor_37_sem as integer) as nascidos_prematuros_menor_37_sem,
    cast(proporcao_prematuridade_pct as double) as proporcao_prematuridade_pct,
    cast(proporcao_apgar_5min_adequado_7_10_pct as double) as proporcao_apgar_5min_adequado_7_10_pct,
    cast(taxa_anomalias_congenitas_10k_nv as double) as taxa_anomalias_congenitas_10k_nv,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sinasc_silver', 'sinasc_condicoes_vitalidade_recem_nascido') }}
