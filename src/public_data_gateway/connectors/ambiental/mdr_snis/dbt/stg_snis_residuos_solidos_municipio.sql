{{ config(materialized='view') }}

select
    cast(cod_snis_rs as varchar) as cod_snis_rs,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(cobertura_coleta_rdo_pct as double) as cobertura_coleta_rdo_pct,
    cast(coleta_seletiva_ativa as boolean) as coleta_seletiva_ativa,
    cast(destinacao_adequada_aterro_pct as double) as destinacao_adequada_aterro_pct,
    cast(massa_recuperada_reciclagem_t as double) as massa_recuperada_reciclagem_t,
    cast(ano_referencia as integer) as ano_referencia
from {{ source('mdr_snis', 'snis_residuos_solidos_municipio') }}
where cod_snis_rs is not null
  and id_municipio is not null
