with source_data as (
    select *
    from read_parquet('data/inpe_terrabrasilis/silver/terrabrasilis_cobertura_uso_terra_bioma.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(ano_prodes as bigint) as ano_prodes,
    cast(area_vegetacao_nativa_km2 as double) as area_vegetacao_nativa_km2,
    cast(area_uso_agropecuario_km2 as double) as area_uso_agropecuario_km2,
    cast(area_regeneracao_secundaria_km2 as double) as area_regeneracao_secundaria_km2,
    cast(area_hidrografia_km2 as double) as area_hidrografia_km2,
    cast(indice_pressao_antropica_pct as double) as indice_pressao_antropica_pct,
    cast(conformidade_codigo_florestal_pct as double) as conformidade_codigo_florestal_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
