{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(pct_colisao_frontal_traseira as double) as pct_colisao_frontal_traseira,
    cast(pct_envolvimento_caminhao_onibus as double) as pct_envolvimento_caminhao_onibus,
    cast(pct_condicao_clima_adverso as double) as pct_condicao_clima_adverso,
    cast(alcoolemia_autuacoes_ano as integer) as alcoolemia_autuacoes_ano
from {{ source('prf_acidentes', 'prf_perfil_causas_condicoes') }}
where id_municipio is not null
