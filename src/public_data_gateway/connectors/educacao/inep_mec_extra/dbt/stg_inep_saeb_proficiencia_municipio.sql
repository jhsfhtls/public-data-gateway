{{ config(materialized='view') }}

select
    cast(cod_avaliacao_inep as varchar) as cod_avaliacao_inep,
    cast(etapa_saeb_mec as varchar) as etapa_saeb_mec,
    cast(programa_acesso_fomento as varchar) as programa_acesso_fomento,
    cast(situacao_equidade_saeb as varchar) as situacao_equidade_saeb,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(proficiencia_media_saeb_lp_mat as double) as proficiencia_media_saeb_lp_mat,
    cast(bolsas_prouni_sisu_12m as bigint) as bolsas_prouni_sisu_12m,
    cast(repasses_fundeb_cnpq_mil_brl as double) as repasses_fundeb_cnpq_mil_brl
from {{ source('inep_mec_extra', 'inep_saeb_proficiencia_municipio') }}
where cod_avaliacao_inep is not null
