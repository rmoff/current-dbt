WITH      source_data AS (
          -- Spec #6: Create a unique ID for each session 
          SELECT    md5(session_name)  AS session_id,
                    session_name
          FROM      {{ ref('stg_ratings') }}
          UNION 
          SELECT    md5(session_name)  AS session_id,
                    session_name
          FROM      {{ ref('stg_scans') }}
          )

SELECT    *
FROM      source_data
