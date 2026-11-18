{{ config(materialized='view') }}

select
    cast(cod_idss_ans as varchar) as cod_idss_ans,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(operadoras_atuantes_count as bigint) as operadoras_atuantes_count,
    cast(idss_medio_ponderado as double) as idss_medio_ponderado,
    cast(ticket_medio_mensal_brl as double) as ticket_medio_mensal_brl,
    cast(reclamacoes_nip_10k_benef as double) as reclamacoes_nip_10k_benef
from {{ source('ans_saude_suplementar', 'ans_operadoras_idss_qualidade') }}
