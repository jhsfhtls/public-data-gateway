{{ config(materialized='view') }}

SELECT
    CAST(model_id AS VARCHAR) AS model_id,
    CAST(input_hash AS VARCHAR) AS input_hash,
    CAST(task_type AS VARCHAR) AS task_type,
    CAST(top_label AS VARCHAR) AS top_label,
    CAST(top_score AS DOUBLE) AS top_score,
    CAST(extracted_at AS VARCHAR) AS extracted_at
FROM {{ source('huggingface_inference', 'hf_inference_runs') }}
WHERE model_id IS NOT NULL
  AND input_hash IS NOT NULL
