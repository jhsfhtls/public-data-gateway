{{ config(materialized='table') }}

select
    e.numero_inscricao_suframa,
    e.cnpj_14,
    e.cnpj_raiz,
    e.razao_social,
    e.nome_fantasia,
    e.situacao_cadastral,
    e.data_inscricao,
    e.data_validade_cadastro,
    e.tipo_estabelecimento,
    e.tipo_area_incentivada,
    e.codigo_area_suframa,
    e.id_municipio_ibge,
    e.sigla_uf,
    e.subsetor_industrial,
    s.nome_subsetor,
    s.is_industrial as is_subsetor_manufatureiro,
    s.relevancia_pim,
    case
        when e.situacao_cadastral = 'ATIVA' and e.data_validade_cadastro >= current_date then true
        else false
    end as is_cadastro_regular_vigente,
    e.ingestion_timestamp
from {{ ref('stg_suframa_empresas') }} e
left join {{ ref('stg_suframa_subsetores') }} s
    on e.subsetor_industrial = s.subsetor_industrial
