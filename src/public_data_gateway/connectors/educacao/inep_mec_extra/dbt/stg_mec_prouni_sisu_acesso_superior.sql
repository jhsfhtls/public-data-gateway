{{ config(materialized='view') }}

select
    cast(cod_acesso_mec as varchar) as cod_acesso_mec,
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(bolsas_prouni_ativas as bigint) as bolsas_prouni_ativas,
    cast(vagas_ofertadas_sisu as bigint) as vagas_ofertadas_sisu,
    cast(nota_corte_media_sisu as double) as nota_corte_media_sisu,
    cast(taxa_ocupacao_vagas_pct as double) as taxa_ocupacao_vagas_pct
from {{ source('inep_mec_extra', 'mec_prouni_sisu_acesso_superior') }}
where cod_acesso_mec is not null
