{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(regiao_hidrografica as varchar) as regiao_hidrografica,
    cast(indice_qualidade_agua_iqa as double) as indice_qualidade_agua_iqa,
    cast(indice_seguranca_hidrica_ish as double) as indice_seguranca_hidrica_ish,
    cast(comprometimento_balanco_hidrico_pct as double) as comprometimento_balanco_hidrico_pct,
    cast(cobertura_outorga_regularizada_pct as double) as cobertura_outorga_regularizada_pct,
    cast(confiabilidade_serie_historica_pct as double) as confiabilidade_serie_historica_pct,
    cast(indice_resiliencia_hidrica as double) as indice_resiliencia_hidrica,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/ana_hidroweb_telemetria/silver/hidroweb_qualidade_seguranca_hidrica.parquet')
