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

SELECT    src.session_id,
          src.session_name,
          sc.speakers,
          sc.track,
          SUM(sc.scans) AS scans,
          SUM(sc.rating_ct) AS rating_ct
FROM      src.source_data src
          LEFT OUTER JOIN
          {{ ref('stg_scans') }} sc
          ON src.session_name = sc.session_name
WHERE     src.session_name IS NOT NULL
GROUP BY  src.session_id,
          src.session_name,
          sc.speakers,
          sc.track