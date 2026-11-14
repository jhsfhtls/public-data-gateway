{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(total_leitos_hospitalares as integer) as total_leitos_hospitalares,
    cast(leitos_sus as integer) as leitos_sus,
    cast(leitos_nao_sus as integer) as leitos_nao_sus,
    cast(total_leitos_uti as integer) as total_leitos_uti,
    cast(leitos_uti_adulto as integer) as leitos_uti_adulto,
    cast(leitos_uti_pediatrico_neonatal as integer) as leitos_uti_pediatrico_neonatal,
    cast(leitos_uti_sus as integer) as leitos_uti_sus,
    cast(leitos_por_1000_hab as double) as leitos_por_1000_hab,
    cast(leitos_uti_por_10k_hab as double) as leitos_uti_por_10k_hab,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_cnes_silver', 'cnes_leitos_hospitalares_uti') }}
