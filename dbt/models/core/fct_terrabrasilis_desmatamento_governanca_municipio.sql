with prodes as (
    select * from {{ ref('stg_terrabrasilis_prodes_desmatamento_anual') }}
),

cobertura as (
    select * from {{ ref('stg_terrabrasilis_cobertura_uso_terra_bioma') }}
),

governanca as (
    select * from {{ ref('stg_terrabrasilis_governanca_fiscalizacao_esg') }}
),

deter_agg as (
    select
        id_municipio,
        sum(num_alertas_deter_ano) as total_alertas_deter_ano,
        round(sum(area_alertas_deter_km2), 2) as area_total_alertas_deter_km2
    from {{ ref('stg_terrabrasilis_deter_alertas_classe') }}
    group by id_municipio
)

select
    p.id_municipio,
    p.nome_municipio,
    p.sigla_uf,
    p.id_uf,
    p.regiao,
    p.bioma_predominante,
    p.ano_prodes,
    p.area_desmatada_prodes_km2,
    p.desmatamento_acumulado_km2,
    p.cobertura_florestal_remanescente_pct,
    p.variacao_anual_prodes_pct,
    p.taxa_desmatamento_relativa_area_pct,
    p.focos_calor_associados_ano,
    da.total_alertas_deter_ano,
    da.area_total_alertas_deter_km2,
    c.area_vegetacao_nativa_km2,
    c.area_uso_agropecuario_km2,
    c.area_regeneracao_secundaria_km2,
    c.area_hidrografia_km2,
    c.indice_pressao_antropica_pct,
    c.conformidade_codigo_florestal_pct,
    g.embargos_ibama_ativos_ano,
    g.autos_infracao_ambiental_ano,
    g.propriedades_car_regularizadas_pct,
    g.cobertura_monitoramento_sar_optico_pct,
    g.elegibilidade_credito_rural_esg_pct,
    g.risco_desconformidade_eudr_pct,
    g.indice_prontidao_fiscalizacao,
    p.reference_date,
    p.extracted_at
from prodes p
inner join deter_agg da
    on p.id_municipio = da.id_municipio
inner join cobertura c
    on p.id_municipio = c.id_municipio
inner join governanca g
    on p.id_municipio = g.id_municipio
