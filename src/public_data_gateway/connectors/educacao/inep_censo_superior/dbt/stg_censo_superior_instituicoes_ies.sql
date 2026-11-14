{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(ano_censo as integer) as ano_censo,
    cast(total_ies_ativas as integer) as total_ies_ativas,
    cast(universidades_e_centros_universitarios as integer) as universidades_e_centros_universitarios,
    cast(ies_rede_publica as integer) as ies_rede_publica,
    cast(ies_rede_privada as integer) as ies_rede_privada,
    cast(proporcao_ies_publicas_pct as double) as proporcao_ies_publicas_pct,
    cast(igc_medio_continuo_municipio as double) as igc_medio_continuo_municipio,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_superior_silver', 'censo_superior_instituicoes_ies') }}
