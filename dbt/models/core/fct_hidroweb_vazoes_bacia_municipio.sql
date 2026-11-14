{{ config(materialized='table') }}

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    regiao_hidrografica,
    tipo_monitoramento_hidrologico,
    vazao_media_m3s,
    cota_media_cm,
    cota_maxima_anual_cm,
    cota_minima_anual_cm,
    taxa_eventos_criticos_pct,
    capacidade_regularizacao_pct,
    reference_date
from {{ ref('stg_ana_hidroweb_cotas_vazoes_bacia') }}
