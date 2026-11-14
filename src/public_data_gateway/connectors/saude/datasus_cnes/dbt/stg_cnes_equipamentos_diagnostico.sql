{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(tomografos_computadorizados as integer) as tomografos_computadorizados,
    cast(ressonancia_magnetica as integer) as ressonancia_magnetica,
    cast(mamografos_em_uso as integer) as mamografos_em_uso,
    cast(aparelhos_hemodialise as integer) as aparelhos_hemodialise,
    cast(aparelhos_raio_x as integer) as aparelhos_raio_x,
    cast(proporcao_equipamentos_sus_pct as double) as proporcao_equipamentos_sus_pct,
    cast(indice_disponibilidade_diagnostica as double) as indice_disponibilidade_diagnostica,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_cnes_silver', 'cnes_equipamentos_diagnostico') }}
