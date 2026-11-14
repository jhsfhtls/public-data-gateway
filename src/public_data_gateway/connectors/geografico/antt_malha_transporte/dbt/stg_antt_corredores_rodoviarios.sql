{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(rodovias_federais_principais as varchar) as rodovias_federais_principais,
    cast(extensao_malha_federal_km as double) as extensao_malha_federal_km,
    cast(trechos_concedidos_antt_km as double) as trechos_concedidos_antt_km,
    cast(proporcao_malha_concedida_pct as double) as proporcao_malha_concedida_pct,
    cast(pracas_pedagio_ativas as integer) as pracas_pedagio_ativas,
    cast(volume_medio_diario_vmd_pesados as integer) as volume_medio_diario_vmd_pesados,
    cast(fluxo_anual_cargas_estimado_ton as double) as fluxo_anual_cargas_estimado_ton,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('antt_malha_transporte_silver', 'antt_corredores_rodoviarios') }}
