with source_data as (
    select *
    from read_parquet('data/inpe_terrabrasilis/silver/terrabrasilis_governanca_fiscalizacao_esg.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_prodes as bigint) as ano_prodes,
    cast(embargos_ibama_ativos_ano as bigint) as embargos_ibama_ativos_ano,
    cast(autos_infracao_ambiental_ano as bigint) as autos_infracao_ambiental_ano,
    cast(propriedades_car_regularizadas_pct as double) as propriedades_car_regularizadas_pct,
    cast(cobertura_monitoramento_sar_optico_pct as double) as cobertura_monitoramento_sar_optico_pct,
    cast(elegibilidade_credito_rural_esg_pct as double) as elegibilidade_credito_rural_esg_pct,
    cast(risco_desconformidade_eudr_pct as double) as risco_desconformidade_eudr_pct,
    cast(indice_prontidao_fiscalizacao as double) as indice_prontidao_fiscalizacao,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from source_data
