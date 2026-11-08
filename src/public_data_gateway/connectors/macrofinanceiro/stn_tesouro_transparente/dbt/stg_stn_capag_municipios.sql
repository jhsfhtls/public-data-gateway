{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_tesouro_transparente/silver/stn_capag_municipios.parquet')
)

select
    cast(id_municipio_ibge_7d as varchar) as id_municipio_ibge_7d,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_ente as varchar) as nome_ente,
    cast(ano_exercicio as bigint) as ano_exercicio,
    cast(indicador_1_endividamento_pct as double) as indicador_1_endividamento_pct,
    cast(indicador_2_poupanca_corrente_pct as double) as indicador_2_poupanca_corrente_pct,
    cast(indicador_3_liquidez_relativa_pct as double) as indicador_3_liquidez_relativa_pct,
    cast(nota_indicador_1 as varchar) as nota_indicador_1,
    cast(nota_indicador_2 as varchar) as nota_indicador_2,
    cast(nota_indicador_3 as varchar) as nota_indicador_3,
    cast(nota_capag_final as varchar) as nota_capag_final
from source
