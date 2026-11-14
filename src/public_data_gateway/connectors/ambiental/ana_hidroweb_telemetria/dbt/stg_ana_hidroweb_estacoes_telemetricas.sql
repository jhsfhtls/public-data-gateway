{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(regiao_hidrografica as varchar) as regiao_hidrografica,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(qtd_estacoes_telemetricas_ativas as bigint) as qtd_estacoes_telemetricas_ativas,
    cast(qtd_estacoes_fluviometricas as bigint) as qtd_estacoes_fluviometricas,
    cast(qtd_estacoes_pluviometricas as bigint) as qtd_estacoes_pluviometricas,
    cast(disponibilidade_telemetria_pct as double) as disponibilidade_telemetria_pct,
    cast(densidade_rede_estacoes_1000km2 as double) as densidade_rede_estacoes_1000km2,
    cast(indice_cobertura_rede_hidroweb as double) as indice_cobertura_rede_hidroweb,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/ana_hidroweb_telemetria/silver/hidroweb_estacoes_telemetricas_rede.parquet')
