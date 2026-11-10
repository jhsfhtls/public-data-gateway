{{ config(materialized='view') }}

select
    cast(ano as integer) as ano,
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nivel_escolaridade as varchar) as nivel_escolaridade,
    cast(sexo_trabalhador as varchar) as sexo_trabalhador,
    cast(total_vinculos as integer) as total_vinculos,
    cast(remuneracao_media_reais as double) as remuneracao_media_reais,
    cast(remuneracao_media_sm as double) as remuneracao_media_sm,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/mte_rais/silver/rais_remuneracao_media.parquet')
