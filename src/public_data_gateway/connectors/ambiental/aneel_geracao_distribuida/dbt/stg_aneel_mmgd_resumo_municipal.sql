{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(distribuidora_principal as varchar) as distribuidora_principal,
    cast(qtd_usinas_mmgd as integer) as qtd_usinas_mmgd,
    cast(potencia_instalada_total_kw as double) as potencia_instalada_total_kw,
    cast(potencia_solar_ufv_kw as double) as potencia_solar_ufv_kw,
    cast(potencia_rural_agro_kw as double) as potencia_rural_agro_kw,
    cast(ucs_com_credito_energia as integer) as ucs_com_credito_energia,
    cast(participacao_solar_pct as double) as participacao_solar_pct
from {{ source('aneel_geracao_distribuida', 'aneel_mmgd_resumo_municipal') }}
where id_municipio is not null
