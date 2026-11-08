{{ config(materialized='table') }}

select
    subsetor_industrial,
    nome_subsetor,
    is_industrial,
    relevancia_pim
from {{ ref('stg_suframa_subsetores') }}
