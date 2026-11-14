select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(cultura_zarc as varchar) as cultura_zarc,
    cast(grupo_solo_ad as varchar) as grupo_solo_ad,
    cast(risco_climatico_nivel_pct as integer) as risco_climatico_nivel_pct,
    cast(decendios_aptos_plantio as integer) as decendios_aptos_plantio,
    cast(janela_plantio_inicio_mes as integer) as janela_plantio_inicio_mes,
    cast(janela_plantio_fim_mes as integer) as janela_plantio_fim_mes,
    cast(elegivel_proagro_psr_pct as double) as elegivel_proagro_psr_pct,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapa_zarc/silver/zarc_janelas_plantio_cultura.parquet')
