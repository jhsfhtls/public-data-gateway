{{ config(materialized='table') }}

select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    bioma_predominante,
    categoria_imovel_car,
    qtd_imoveis_categoria,
    area_reserva_legal_declarada_ha,
    area_app_preservada_ha,
    taxa_passivo_ambiental_pct,
    conformidade_reserva_legal_pct,
    reference_date
from {{ ref('stg_sfb_sicar_reserva_legal_app') }}
