{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_censo as integer) as ano_censo,
    cast(idade_mediana_anos as double) as idade_mediana_anos,
    cast(indice_envelhecimento_razao as double) as indice_envelhecimento_razao,
    cast(razao_sexo_homens_por_100_mulheres as double) as razao_sexo_homens_por_100_mulheres,
    cast(populacao_homens as bigint) as populacao_homens,
    cast(populacao_mulheres as bigint) as populacao_mulheres,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_censo2022/silver/censo_estrutura_etaria.parquet')
