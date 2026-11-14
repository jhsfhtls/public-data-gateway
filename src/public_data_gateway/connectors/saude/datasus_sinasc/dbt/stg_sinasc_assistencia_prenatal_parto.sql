{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_referencia as integer) as ano_referencia,
    cast(faixa_assistencia_prenatal as varchar) as faixa_assistencia_prenatal,
    cast(nascidos_vivos_faixa as integer) as nascidos_vivos_faixa,
    cast(participacao_faixa_prenatal_pct as double) as participacao_faixa_prenatal_pct,
    cast(inicio_prenatal_primeiro_trimestre_pct as double) as inicio_prenatal_primeiro_trimestre_pct,
    cast(proporcao_cesarea_faixa_pct as double) as proporcao_cesarea_faixa_pct,
    cast(idade_media_materna_faixa_anos as double) as idade_media_materna_faixa_anos,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sinasc_silver', 'sinasc_assistencia_prenatal_parto') }}
