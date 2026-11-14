{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(bioma_predominante as varchar) as bioma_predominante,
    cast(poligonos_sem_sobreposicao_pct as double) as poligonos_sem_sobreposicao_pct,
    cast(integracao_car_sigef_incra_pct as double) as integracao_car_sigef_incra_pct,
    cast(precisao_georreferenciamento_pct as double) as precisao_georreferenciamento_pct,
    cast(atualizacao_retificacao_anual_pct as double) as atualizacao_retificacao_anual_pct,
    cast(indice_qualidade_geoespacial_car as double) as indice_qualidade_geoespacial_car,
    cast(reference_date as varchar) as reference_date
from read_parquet('data/sfb_sicar/silver/sicar_geometria_validacao_sigef.parquet')
