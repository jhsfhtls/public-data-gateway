{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano as integer) as ano,
    cast(quantidade_produzida_toneladas as double) as quantidade_produzida_toneladas,
    cast(valor_producao_mil_reais as double) as valor_producao_mil_reais,
    cast(area_plantada_hectares as double) as area_plantada_hectares,
    cast(area_colhida_hectares as double) as area_colhida_hectares,
    cast(principal_cultura as varchar) as principal_cultura,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_sidra/silver/sidra_agropecuaria_pam.parquet')
