{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(altas_hospitalares as integer) as altas_hospitalares,
    cast(obitos_hospitalares as integer) as obitos_hospitalares,
    cast(taxa_mortalidade_hospitalar_pct as double) as taxa_mortalidade_hospitalar_pct,
    cast(internacoes_com_uti as integer) as internacoes_com_uti,
    cast(taxa_utilizacao_uti_pct as double) as taxa_utilizacao_uti_pct,
    cast(internacoes_icsap_evitaveis as integer) as internacoes_icsap_evitaveis,
    cast(proporcao_icsap_pct as double) as proporcao_icsap_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_sih_silver', 'sih_desfechos_mortalidade_hospitalar') }}
