{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_mes_referencia as varchar) as ano_mes_referencia,
    cast(possui_backhaul_fibra as boolean) as possui_backhaul_fibra,
    cast(total_acessos_scm as integer) as total_acessos_scm,
    cast(acessos_fibra_ftth as integer) as acessos_fibra_ftth,
    cast(acessos_cabo_coaxial as integer) as acessos_cabo_coaxial,
    cast(acessos_radio_satelite as integer) as acessos_radio_satelite,
    cast(participacao_fibra_pct as double) as participacao_fibra_pct,
    cast(velocidade_mediana_mbps as double) as velocidade_mediana_mbps,
    cast(prestadoras_scm_ativas as integer) as prestadoras_scm_ativas,
    cast(indice_hhi_concentracao_scm as double) as indice_hhi_concentracao_scm,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('anatel_mosaico_silver', 'mosaico_banda_larga_scm') }}
