{{ config(materialized='view') }}

select
    cast(cod_lotacao_siape as varchar) as cod_lotacao_siape,
    cast(orgao_exercicio_codigo as varchar) as orgao_exercicio_codigo,
    cast(regime_juridico_siape as varchar) as regime_juridico_siape,
    cast(situacao_vinculo as varchar) as situacao_vinculo,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(qtd_servidores_lotados as integer) as qtd_servidores_lotados,
    cast(massa_salarial_bruta_mil_brl as double) as massa_salarial_bruta_mil_brl,
    cast(competencia_siape as varchar) as competencia_siape
from {{ source('cgu_servidores_federais', 'siape_servidores_ativos_municipio') }}
where cod_lotacao_siape is not null
