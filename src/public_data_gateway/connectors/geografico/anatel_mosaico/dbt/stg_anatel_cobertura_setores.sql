{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_mes_referencia as varchar) as ano_mes_referencia,
    cast(populacao_coberta_4g_pct as double) as populacao_coberta_4g_pct,
    cast(populacao_coberta_5g_pct as double) as populacao_coberta_5g_pct,
    cast(area_geografica_coberta_movel_pct as double) as area_geografica_coberta_movel_pct,
    cast(cobertura_rodovias_federais_pct as double) as cobertura_rodovias_federais_pct,
    cast(localidades_rurais_conectadas_pct as double) as localidades_rurais_conectadas_pct,
    cast(escolas_publicas_conectadas_pct as double) as escolas_publicas_conectadas_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anatel_mosaico_silver', 'mosaico_cobertura_setores') }}
