{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_referencia as varchar) as ano_mes_referencia,
    cast(total_erbs_licenciadas as integer) as total_erbs_licenciadas,
    cast(erbs_5g_nr as integer) as erbs_5g_nr,
    cast(erbs_4g_lte as integer) as erbs_4g_lte,
    cast(erbs_3g_wcdma as integer) as erbs_3g_wcdma,
    cast(erbs_2g_gsm as integer) as erbs_2g_gsm,
    cast(operadoras_ativas_smp as integer) as operadoras_ativas_smp,
    cast(operadora_lider_erbs as varchar) as operadora_lider_erbs,
    cast(densidade_erbs_por_10k_hab as double) as densidade_erbs_por_10k_hab,
    cast(densidade_erbs_por_100km2 as double) as densidade_erbs_por_100km2,
    cast(cobertura_urbana_5g_pct as double) as cobertura_urbana_5g_pct,
    cast(cobertura_urbana_4g_pct as double) as cobertura_urbana_4g_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anatel_mosaico_silver', 'mosaico_estacoes_smp') }}
