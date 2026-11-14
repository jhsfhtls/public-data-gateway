{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_edicao_enem as bigint) as ano_edicao_enem,
    cast(dependencia_administrativa_escola as varchar) as dependencia_administrativa_escola,
    cast(categoria_escola_enem as varchar) as categoria_escola_enem,
    cast(concluintes_participantes_rede as bigint) as concluintes_participantes_rede,
    cast(nota_media_objetivas_rede as double) as nota_media_objetivas_rede,
    cast(nota_media_matematica_rede as double) as nota_media_matematica_rede,
    cast(nota_media_redacao_rede as double) as nota_media_redacao_rede,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_enem_silver', 'enem_desempenho_rede_ensino') }}
