{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_alertas_mapbiomas_ano as bigint) as qtd_alertas_mapbiomas_ano,
    cast(area_desmatada_validada_ha as double) as area_desmatada_validada_ha,
    cast(alertas_com_embargo_ou_autuacao_pct as double) as alertas_com_embargo_ou_autuacao_pct,
    cast(alertas_em_area_protegida_pct as double) as alertas_em_area_protegida_pct,
    cast(velocidade_regeneracao_liquida_pct as double) as velocidade_regeneracao_liquida_pct,
    cast(indice_integridade_territorial as double) as indice_integridade_territorial,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapbiomas_cobertura/silver/mapbiomas_alerta_desmatamento_validado.parquet')
