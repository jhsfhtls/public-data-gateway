select
    id_municipio,
    nome_municipio,
    sigla_uf,
    id_uf,
    regiao,
    ano_prodes,
    classe_alerta_deter,
    num_alertas_deter_ano,
    area_alertas_deter_km2,
    area_media_poligono_ha,
    alertas_em_uc_ti_pct,
    dias_medios_emissao_alerta,
    proporcao_classe_sobre_total_pct,
    reference_date,
    extracted_at
from {{ ref('stg_terrabrasilis_deter_alertas_classe') }}
