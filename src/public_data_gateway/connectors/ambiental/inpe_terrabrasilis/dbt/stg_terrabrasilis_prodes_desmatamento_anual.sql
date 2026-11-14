with source_data as (
    select *
    from read_parquet('data/inpe_terrabrasilis/silver/terrabrasilis_prodes_desmatamento_anual.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(ano_prodes as bigint) as ano_prodes,
    cast(area_desmatada_prodes_km2 as double) as area_desmatada_prodes_km2,
    cast(desmatamento_acumulado_km2 as double) as desmatamento_acumulado_km2,
    cast(cobertura_florestal_remanescente_pct as double) as cobertura_florestal_remanescente_pct,
    cast(variacao_anual_prodes_pct as double) as variacao_anual_prodes_pct,
    cast(taxa_desmatamento_relativa_area_pct as double) as taxa_desmatamento_relativa_area_pct,
    cast(focos_calor_associados_ano as bigint) as focos_calor_associados_ano,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
