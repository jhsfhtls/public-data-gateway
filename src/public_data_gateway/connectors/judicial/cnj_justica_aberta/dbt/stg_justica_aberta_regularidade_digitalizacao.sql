{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(semestre_referencia as varchar) as semestre_referencia,
    cast(acervo_historico_digitalizado_pct as double) as acervo_historico_digitalizado_pct,
    cast(serventias_integradas_srei_onr_crc_pct as double) as serventias_integradas_srei_onr_crc_pct,
    cast(indice_transparencia_prestacao_contas_cnj_pct as double) as indice_transparencia_prestacao_contas_cnj_pct,
    cast(inspecoes_corregedoria_em_dia_pct as double) as inspecoes_corregedoria_em_dia_pct,
    cast(reclamacoes_disciplinares_por_100k_atos as double) as reclamacoes_disciplinares_por_100k_atos,
    cast(forca_trabalho_prepostos_escreventes as integer) as forca_trabalho_prepostos_escreventes,
    cast(atos_por_escrevente_semestre as double) as atos_por_escrevente_semestre,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('cnj_justica_aberta_silver', 'justica_aberta_regularidade_digitalizacao') }}
