{{ config(materialized='view') }}

select
    cast(cod_empreendimento_mmgd as varchar) as cod_empreendimento_mmgd,
    cast(sig_tipo_geracao as varchar) as sig_tipo_geracao,
    cast(classe_consumo as varchar) as classe_consumo,
    cast(modalidade_mmgd as varchar) as modalidade_mmgd,
    cast(distribuidora as varchar) as distribuidora,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(potencia_instalada_kw as double) as potencia_instalada_kw,
    cast(qtd_modulos_instalados as integer) as qtd_modulos_instalados,
    cast(qtd_unidades_beneficiarias as integer) as qtd_unidades_beneficiarias,
    cast(data_conexao as varchar) as data_conexao,
    cast(tipo_titular as varchar) as tipo_titular
from {{ source('aneel_geracao_distribuida', 'aneel_empreendimentos_mmgd') }}
where cod_empreendimento_mmgd is not null
  and id_municipio is not null
