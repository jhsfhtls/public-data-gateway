{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(faixa_frequencia_mhz as varchar) as faixa_frequencia_mhz,
    cast(geracao_tecnologica as varchar) as geracao_tecnologica,
    cast(largura_banda_agregada_mhz as double) as largura_banda_agregada_mhz,
    cast(estacoes_licenciadas_faixa as integer) as estacoes_licenciadas_faixa,
    cast(potencia_media_eirp_dbm as double) as potencia_media_eirp_dbm,
    cast(status_limpeza_faixa as varchar) as status_limpeza_faixa,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anatel_mosaico_silver', 'mosaico_espectro_radiofrequencia') }}
