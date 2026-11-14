{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(regiao_hidrografica as varchar) as regiao_hidrografica,
    cast(tipo_monitoramento_hidrologico as varchar) as tipo_monitoramento_hidrologico,
    cast(vazao_media_m3s as double) as vazao_media_m3s,
    cast(cota_media_cm as double) as cota_media_cm,
    cast(cota_maxima_anual_cm as double) as cota_maxima_anual_cm,
    cast(cota_minima_anual_cm as double) as cota_minima_anual_cm,
    cast(taxa_eventos_criticos_pct as double) as taxa_eventos_criticos_pct,
    cast(capacidade_regularizacao_pct as double) as capacidade_regularizacao_pct,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/ana_hidroweb_telemetria/silver/hidroweb_cotas_vazoes_bacia.parquet')
