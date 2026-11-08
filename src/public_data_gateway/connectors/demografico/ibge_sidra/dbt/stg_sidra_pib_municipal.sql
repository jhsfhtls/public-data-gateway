{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano as integer) as ano,
    cast(pib_mil_reais as double) as pib_mil_reais,
    cast(vab_total_mil_reais as double) as vab_total_mil_reais,
    cast(vab_agropecuaria_mil_reais as double) as vab_agropecuaria_mil_reais,
    cast(vab_industria_mil_reais as double) as vab_industria_mil_reais,
    cast(vab_servicos_mil_reais as double) as vab_servicos_mil_reais,
    cast(vab_administracao_publica_mil_reais as double) as vab_administracao_publica_mil_reais,
    cast(impostos_liquidos_mil_reais as double) as impostos_liquidos_mil_reais,
    cast(participacao_pib_brasil_pct as double) as participacao_pib_brasil_pct,
    cast(participacao_pib_uf_pct as double) as participacao_pib_uf_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from read_parquet('data/ibge_sidra/silver/sidra_pib_municipal.parquet')
