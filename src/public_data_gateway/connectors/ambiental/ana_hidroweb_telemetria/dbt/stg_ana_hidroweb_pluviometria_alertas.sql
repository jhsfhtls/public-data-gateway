{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(regiao_hidrografica as varchar) as regiao_hidrografica,
    cast(precipitacao_acumulada_anual_mm as double) as precipitacao_acumulada_anual_mm,
    cast(anomalia_precipitacao_pct as double) as anomalia_precipitacao_pct,
    cast(dias_consecutivos_sem_chuva as bigint) as dias_consecutivos_sem_chuva,
    cast(eventos_alerta_cheia as bigint) as eventos_alerta_cheia,
    cast(eventos_alerta_estiagem as bigint) as eventos_alerta_estiagem,
    cast(indice_equilibrio_pluviometrico as double) as indice_equilibrio_pluviometrico,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/ana_hidroweb_telemetria/silver/hidroweb_pluviometria_estiagem_cheias.parquet')
