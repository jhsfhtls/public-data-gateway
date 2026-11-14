{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_epidemiologico as bigint) as ano_epidemiologico,
    cast(unidades_sentinela_vigilancia_ativas as bigint) as unidades_sentinela_vigilancia_ativas,
    cast(notificacoes_imediatas_24h_cievs_pct as double) as notificacoes_imediatas_24h_cievs_pct,
    cast(investigacoes_epidemiologicas_48h_pct as double) as investigacoes_epidemiologicas_48h_pct,
    cast(tempo_medio_liberacao_gal_lacen_horas as double) as tempo_medio_liberacao_gal_lacen_horas,
    cast(indice_bloqueio_vetorial_arboviroses_pct as double) as indice_bloqueio_vetorial_arboviroses_pct,
    cast(surtos_investigados_encerrados_pct as double) as surtos_investigados_encerrados_pct,
    cast(integracao_rnds_esus_sinan_pct as double) as integracao_rnds_esus_sinan_pct,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('opendatasus_vigilancia_silver', 'vigilancia_capacidade_resposta_cievs_lacen') }}
