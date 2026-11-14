{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(total_transportadores_rntrc as integer) as total_transportadores_rntrc,
    cast(transportadores_etc_empresas as integer) as transportadores_etc_empresas,
    cast(transportadores_tac_autonomos as integer) as transportadores_tac_autonomos,
    cast(transportadores_ctc_cooperativas as integer) as transportadores_ctc_cooperativas,
    cast(frota_veiculos_habilitados as integer) as frota_veiculos_habilitados,
    cast(capacidade_estatica_total_ton as double) as capacidade_estatica_total_ton,
    cast(idade_media_frota_anos as double) as idade_media_frota_anos,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('antt_malha_transporte_silver', 'antt_frota_rntrc_municipio') }}
