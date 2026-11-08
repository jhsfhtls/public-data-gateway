{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(populacao_urbana as bigint) as populacao_urbana,
    cast(populacao_rural as bigint) as populacao_rural,
    cast(taxa_urbanizacao_pct as double) as taxa_urbanizacao_pct,
    cast(predominancia_territorial as varchar) as predominancia_territorial,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_censo2022/silver/censo_urbanizacao_situacao.parquet')
