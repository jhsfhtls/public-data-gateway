{{ config(materialized='table') }}

SELECT
    model_id,
    input_hash,
    task_type,
    top_label,
    top_score,
    extracted_at
FROM {{ ref('stg_huggingface_inference_runs') }}
