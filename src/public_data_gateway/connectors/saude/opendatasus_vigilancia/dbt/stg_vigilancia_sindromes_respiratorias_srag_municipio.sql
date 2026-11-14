{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_epidemiologico as bigint) as ano_epidemiologico,
    cast(notificacoes_sindrome_gripal_esus_ano as bigint) as notificacoes_sindrome_gripal_esus_ano,
    cast(internacoes_srag_sivep_ano as bigint) as internacoes_srag_sivep_ano,
    cast(taxa_incidencia_srag_100k_hab as double) as taxa_incidencia_srag_100k_hab,
    cast(internacoes_srag_uti_ano as bigint) as internacoes_srag_uti_ano,
    cast(proporcao_srag_uti_pct as double) as proporcao_srag_uti_pct,
    cast(obitos_srag_confirmados_ano as bigint) as obitos_srag_confirmados_ano,
    cast(letalidade_hospitalar_srag_pct as double) as letalidade_hospitalar_srag_pct,
    cast(confirmacao_laboratorial_rt_pcr_srag_pct as double) as confirmacao_laboratorial_rt_pcr_srag_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('opendatasus_vigilancia_silver', 'vigilancia_sindromes_respiratorias_srag_municipio') }}
