{{ config(materialized='view') }}

select
    cast(id_municipio as varchar) as id_municipio,
    cast(sigla_uf as varchar) as sigla_uf,
    cast(nome_municipio as varchar) as nome_municipio,
    cast(regiao as varchar) as regiao,
    cast(frota_total_veiculos as bigint) as frota_total_veiculos,
    cast(qtd_automoveis as bigint) as qtd_automoveis,
    cast(qtd_motocicletas as bigint) as qtd_motocicletas,
    cast(qtd_caminhoes_utilitarios as bigint) as qtd_caminhoes_utilitarios,
    cast(qtd_eletrificados_bevs_hevs as bigint) as qtd_eletrificados_bevs_hevs,
    cast(veiculos_novos_emplacados_ano as bigint) as veiculos_novos_emplacados_ano
from {{ source('senatran_frota', 'senatran_frota_resumo_municipal') }}
where id_municipio is not null
