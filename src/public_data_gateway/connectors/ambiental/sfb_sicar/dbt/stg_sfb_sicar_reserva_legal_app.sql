{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(categoria_imovel_car as varchar) as categoria_imovel_car,
    cast(qtd_imoveis_categoria as bigint) as qtd_imoveis_categoria,
    cast(area_reserva_legal_declarada_ha as double) as area_reserva_legal_declarada_ha,
    cast(area_app_preservada_ha as double) as area_app_preservada_ha,
    cast(taxa_passivo_ambiental_pct as double) as taxa_passivo_ambiental_pct,
    cast(conformidade_reserva_legal_pct as double) as conformidade_reserva_legal_pct,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/sfb_sicar/silver/sicar_reserva_legal_app_situacao.parquet')
