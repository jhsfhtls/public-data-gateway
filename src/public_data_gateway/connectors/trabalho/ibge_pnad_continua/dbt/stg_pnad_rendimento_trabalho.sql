{{ config(materialized='view') }}

select
    id_uf,
    sigla_uf,
    nome_uf,
    ano_trimestre,
    rendimento_medio_real_habitual_reais,
    rendimento_medio_real_efetivo_reais,
    massa_rendimento_real_habitual_milhoes_reais,
    variacao_rendimento_ano_anterior_pct,
    reference_date,
    extracted_at
from {{ source('ibge_pnad_continua_silver', 'pnad_rendimento_trabalho') }}
