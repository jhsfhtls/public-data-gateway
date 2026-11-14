select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(capacidade_retencao_agua_solo_pct as double) as capacidade_retencao_agua_solo_pct,
    cast(adocao_plantio_direto_zarc_pct as double) as adocao_plantio_direto_zarc_pct,
    cast(estabilidade_pluviometrica_safra_pct as double) as estabilidade_pluviometrica_safra_pct,
    cast(indice_resiliencia_solo_ad as double) as indice_resiliencia_solo_ad,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/mapa_zarc/silver/zarc_risco_solo_ad_biomassa.parquet')
