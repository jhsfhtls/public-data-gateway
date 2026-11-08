{{ config(materialized='view') }}

select
    cast(id_uf as varchar) as id_uf,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_uf as varchar) as nome_uf,
    cast(ano as integer) as ano,
    cast(indice_gini_rendimento as double) as indice_gini_rendimento,
    cast(rendimento_medio_domiciliar_reais as double) as rendimento_medio_domiciliar_reais,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_sidra/silver/sidra_gini_rendimento_uf.parquet')
