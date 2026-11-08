with source as (
    select * from {{ source('suframa_pim_zfm', 'suframa_subsetores_industriais') }}
),

renamed as (
    select
        cast(subsetor_industrial as varchar) as subsetor_industrial,
        cast(nome_subsetor as varchar) as nome_subsetor,
        cast(is_industrial as boolean) as is_industrial,
        cast(relevancia_pim as varchar) as relevancia_pim
    from source
)

select * from renamed
