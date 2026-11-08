{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(populacao_indigena as bigint) as populacao_indigena,
    cast(populacao_quilombola as bigint) as populacao_quilombola,
    cast(percentual_indigena_pct as double) as percentual_indigena_pct,
    cast(percentual_quilombola_pct as double) as percentual_quilombola_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_censo2022/silver/censo_diversidade_quilombola_indigena.parquet')
