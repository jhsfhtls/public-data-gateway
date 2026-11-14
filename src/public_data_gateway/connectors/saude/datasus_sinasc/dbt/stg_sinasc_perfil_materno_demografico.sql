{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(idade_media_materna_anos as double) as idade_media_materna_anos,
    cast(maes_adolescentes_menores_20_anos as integer) as maes_adolescentes_menores_20_anos,
    cast(proporcao_gravidez_adolescencia_pct as double) as proporcao_gravidez_adolescencia_pct,
    cast(maes_35_anos_ou_mais as integer) as maes_35_anos_ou_mais,
    cast(proporcao_gestacao_tardia_35_mais_pct as double) as proporcao_gestacao_tardia_35_mais_pct,
    cast(proporcao_escolaridade_materna_12_anos_mais_pct as double) as proporcao_escolaridade_materna_12_anos_mais_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sinasc_silver', 'sinasc_perfil_materno_demografico') }}
