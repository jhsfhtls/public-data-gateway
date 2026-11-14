{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(cep_8 as varchar) as cep_8,
    cast(ano_censo as integer) as ano_censo,
    cast(total_escolas_ativas as integer) as total_escolas_ativas,
    cast(escolas_rede_publica as integer) as escolas_rede_publica,
    cast(escolas_rede_privada as integer) as escolas_rede_privada,
    cast(proporcao_rede_publica_pct as double) as proporcao_rede_publica_pct,
    cast(escolas_com_internet_banda_larga_pct as double) as escolas_com_internet_banda_larga_pct,
    cast(escolas_com_laboratorio_informatica_pct as double) as escolas_com_laboratorio_informatica_pct,
    cast(escolas_com_laboratorio_ciencias_pct as double) as escolas_com_laboratorio_ciencias_pct,
    cast(escolas_com_biblioteca_pct as double) as escolas_com_biblioteca_pct,
    cast(escolas_com_quadra_esportiva_pct as double) as escolas_com_quadra_esportiva_pct,
    cast(escolas_acessibilidade_pcd_pct as double) as escolas_acessibilidade_pcd_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_censo_escolar_silver', 'censo_escolar_escolas_infraestrutura') }}
