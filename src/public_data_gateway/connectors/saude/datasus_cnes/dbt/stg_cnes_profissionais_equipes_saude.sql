{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(categoria_profissional as varchar) as categoria_profissional,
    cast(total_vinculos_ativos as integer) as total_vinculos_ativos,
    cast(vinculos_atendimento_sus as integer) as vinculos_atendimento_sus,
    cast(carga_horaria_semanal_media as double) as carga_horaria_semanal_media,
    cast(densidade_por_10k_hab as double) as densidade_por_10k_hab,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_cnes_silver', 'cnes_profissionais_equipes_saude') }}
