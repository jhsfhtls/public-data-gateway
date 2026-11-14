select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    ano_base_sucupira,
    grande_area_conhecimento,
    matriculados_mestrado_area,
    matriculados_doutorado_area,
    (matriculados_mestrado_area + matriculados_doutorado_area) as total_matriculados_area,
    titulados_mestres_ano,
    titulados_doutores_ano,
    (titulados_mestres_ano + titulados_doutores_ano) as total_titulados_area,
    bolsistas_capes_area,
    taxa_titulacao_doutorado_pct,
    reference_date,
    extracted_at
from {{ ref('stg_sucupira_discentes_titulados_area') }}
