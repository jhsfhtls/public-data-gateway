{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(ano_mes_competencia as varchar) as ano_mes_competencia,
    cast(codigo_cnes_referencia as varchar) as codigo_cnes_referencia,
    cast(cnpj_14 as varchar) as cnpj_14,
    cast(cep_8 as varchar) as cep_8,
    cast(estabelecimento_referencia_nome as varchar) as estabelecimento_referencia_nome,
    cast(total_estabelecimentos_ativos as integer) as total_estabelecimentos_ativos,
    cast(hospitais_gerais_especializados as integer) as hospitais_gerais_especializados,
    cast(unidades_basicas_saude_ubs as integer) as unidades_basicas_saude_ubs,
    cast(unidades_pronto_atendimento_upa as integer) as unidades_pronto_atendimento_upa,
    cast(estabelecimentos_atendimento_sus as integer) as estabelecimentos_atendimento_sus,
    cast(proporcao_rede_sus_pct as double) as proporcao_rede_sus_pct,
    cast(source_url as varchar) as source_url,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('datasus_cnes_silver', 'cnes_estabelecimentos_saude') }}
