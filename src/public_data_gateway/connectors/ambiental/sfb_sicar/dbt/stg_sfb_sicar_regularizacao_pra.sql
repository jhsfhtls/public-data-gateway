{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(qtd_imoveis_aderidos_pra as bigint) as qtd_imoveis_aderidos_pra,
    cast(imoveis_sem_sobreposicao_ti_uc_pct as double) as imoveis_sem_sobreposicao_ti_uc_pct,
    cast(imoveis_livres_embargo_ibama_pct as double) as imoveis_livres_embargo_ibama_pct,
    cast(elegibilidade_credito_rural_car_pct as double) as elegibilidade_credito_rural_car_pct,
    cast(velocidade_homologacao_car_pct as double) as velocidade_homologacao_car_pct,
    cast(indice_governanca_pra as double) as indice_governanca_pra,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/sfb_sicar/silver/sicar_regularizacao_pra_embargos.parquet')
