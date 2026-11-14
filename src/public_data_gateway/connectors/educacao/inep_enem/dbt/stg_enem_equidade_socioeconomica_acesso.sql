{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(id_uf as varchar) as id_uf,
    cast(regiao as varchar) as regiao,
    cast(ano_edicao_enem as bigint) as ano_edicao_enem,
    cast(proporcao_isencao_taxa_inscricao_pct as double) as proporcao_isencao_taxa_inscricao_pct,
    cast(proporcao_concluintes_escola_publica_pct as double) as proporcao_concluintes_escola_publica_pct,
    cast(acesso_internet_domiciliar_q025_pct as double) as acesso_internet_domiciliar_q025_pct,
    cast(gap_nota_objetiva_privada_vs_publica_pts as double) as gap_nota_objetiva_privada_vs_publica_pts,
    cast(razao_equidade_publica_privada_enem as double) as razao_equidade_publica_privada_enem,
    cast(aptos_sisu_prouni_fies_nota_450_pct as double) as aptos_sisu_prouni_fies_nota_450_pct,
    cast(indice_paridade_socioeconomica_qse as double) as indice_paridade_socioeconomica_qse,
    cast(reference_date as varchar) as reference_date,
    cast(extracted_at as varchar) as extracted_at
from {{ source('inep_enem_silver', 'enem_equidade_socioeconomica_acesso') }}
