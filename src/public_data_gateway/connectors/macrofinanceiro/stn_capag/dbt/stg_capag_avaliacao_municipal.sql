{{ config(materialized='view') }}

with source as (
    select * from read_parquet('data/stn_capag/silver/capag_avaliacao_municipal.parquet')
)

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(ano_referencia as bigint) as ano_referencia,
    cast(nota_capag_oficial as varchar) as nota_capag_oficial,
    cast(indicador_1_endividamento_dcl_rcl_pct as double) as indicador_1_endividamento_dcl_rcl_pct,
    cast(nota_ind_1 as varchar) as nota_ind_1,
    cast(indicador_2_poupanca_corrente_pct as double) as indicador_2_poupanca_corrente_pct,
    cast(nota_ind_2 as varchar) as nota_ind_2,
    cast(indicador_3_liquidez_relativa_pct as double) as indicador_3_liquidez_relativa_pct,
    cast(nota_ind_3 as varchar) as nota_ind_3,
    cast(elegivel_garantia_uniao as boolean) as elegivel_garantia_uniao,
    cast(qualidade_informacao_ranking_icf as varchar) as qualidade_informacao_ranking_icf
from source
