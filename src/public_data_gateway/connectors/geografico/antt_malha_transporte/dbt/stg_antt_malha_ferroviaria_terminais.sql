{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(possui_conexao_ferroviaria as boolean) as possui_conexao_ferroviaria,
    cast(concessionarias_ferroviarias as varchar) as concessionarias_ferroviarias,
    cast(extensao_ferroviaria_km as double) as extensao_ferroviaria_km,
    cast(terminais_carga_intermodal as integer) as terminais_carga_intermodal,
    cast(movimentacao_ferroviaria_anual_tu as double) as movimentacao_ferroviaria_anual_tu,
    cast(principais_mercadorias_ferrovia as varchar) as principais_mercadorias_ferrovia,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('antt_malha_transporte_silver', 'antt_malha_ferroviaria_terminais') }}
