{{ config(materialized='view') }}

select
    cast(cod_registro_susep as varchar) as cod_registro_susep,
    cast(grupo_ramo_seguro as varchar) as grupo_ramo_seguro,
    cast(porte_mercado_segurador as varchar) as porte_mercado_segurador,
    cast(situacao_solvencia_pl as varchar) as situacao_solvencia_pl,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(premio_direto_anual_mil_brl as double) as premio_direto_anual_mil_brl,
    cast(sinistro_retido_anual_mil_brl as double) as sinistro_retido_anual_mil_brl,
    cast(indice_sinistralidade_pct as double) as indice_sinistralidade_pct
from {{ source('susep_seguros', 'susep_premios_sinistros_ramo') }}
where cod_registro_susep is not null
