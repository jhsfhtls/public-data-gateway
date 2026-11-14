with source_data as (
    select *
    from read_parquet('data/inpe_terrabrasilis/silver/terrabrasilis_deter_alertas_classe.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_prodes as bigint) as ano_prodes,
    cast(classe_alerta_deter as varchar) as classe_alerta_deter,
    cast(num_alertas_deter_ano as bigint) as num_alertas_deter_ano,
    cast(area_alertas_deter_km2 as double) as area_alertas_deter_km2,
    cast(area_media_poligono_ha as double) as area_media_poligono_ha,
    cast(alertas_em_uc_ti_pct as double) as alertas_em_uc_ti_pct,
    cast(dias_medios_emissao_alerta as double) as dias_medios_emissao_alerta,
    cast(proporcao_classe_sobre_total_pct as double) as proporcao_classe_sobre_total_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
