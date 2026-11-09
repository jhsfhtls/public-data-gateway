{{ config(materialized='view') }}

select
    id_uf,
    sigla_uf,
    nome_uf,
    ano_trimestre,
    empregado_com_carteira_clt_mil,
    empregado_sem_carteira_mil,
    trabalhador_domestico_mil,
    empregado_setor_publico_mil,
    empregador_mil,
    conta_propria_mil,
    taxa_informalidade_pct,
    reference_date,
    extracted_at
from {{ source('ibge_pnad_continua_silver', 'pnad_posicao_ocupacao') }}
