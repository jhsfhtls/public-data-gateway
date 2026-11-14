{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(semestre_referencia as varchar) as semestre_referencia,
    cast(atribuicao_cartorial as varchar) as atribuicao_cartorial,
    cast(serventias_atribuicao as integer) as serventias_atribuicao,
    cast(atos_praticados_semestre as bigint) as atos_praticados_semestre,
    cast(certidoes_expedidas_semestre as bigint) as certidoes_expedidas_semestre,
    cast(participacao_atos_municipio_pct as double) as participacao_atos_municipio_pct,
    cast(proporcao_atos_eletronicos_e_notariado_pct as double) as proporcao_atos_eletronicos_e_notariado_pct,
    cast(prazo_medio_emissao_dias as double) as prazo_medio_emissao_dias,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('cnj_justica_aberta_silver', 'justica_aberta_produtividade_atribuicao') }}
