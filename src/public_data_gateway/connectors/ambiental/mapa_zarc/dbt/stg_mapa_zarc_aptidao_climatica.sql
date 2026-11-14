select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(ano_safra as varchar) as ano_safra,
    cast(qtd_culturas_zoneadas as integer) as qtd_culturas_zoneadas,
    cast(decendios_aptos_media as integer) as decendios_aptos_media,
    cast(indice_seguranca_hidrica_safra as double) as indice_seguranca_hidrica_safra,
    cast(risco_geada_granizo_pct as double) as risco_geada_granizo_pct,
    cast(indice_aptidao_climatica_zarc as double) as indice_aptidao_climatica_zarc,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapa_zarc/silver/zarc_aptidao_climatica_municipio.parquet')
