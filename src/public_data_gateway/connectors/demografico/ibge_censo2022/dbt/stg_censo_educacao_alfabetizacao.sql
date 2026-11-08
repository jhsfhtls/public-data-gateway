{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(populacao_15_anos_ou_mais as bigint) as populacao_15_anos_ou_mais,
    cast(populacao_alfabetizada as bigint) as populacao_alfabetizada,
    cast(taxa_alfabetizacao_pct as double) as taxa_alfabetizacao_pct,
    cast(taxa_analfabetismo_pct as double) as taxa_analfabetismo_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_censo2022/silver/censo_educacao_alfabetizacao.parquet')
