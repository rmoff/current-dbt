{% set rating_areas = ['overall_experience','presenter', 'content'] %}
{% set rating_types = ['rating','comment'] %}

WITH ratings_agg AS (
  SELECT session_id, 
         {% for a in rating_areas -%}
          {% for r in rating_types -%}
            LIST_SORT(
              LIST({{a}}_{{r}}),
              'DESC') AS {{a}}_{{r}},
          {% endfor -%}
         {% endfor -%}
    FROM {{ ref('session_ratings_detail')}}
  GROUP BY session_id
)

SELECT s.session_id, 
       s.session_name, 
       s.speakers, 
       s.track, 
       s.scans,
        {% for a in rating_areas -%}
          LIST_FILTER({{a}}_rating,x->x IS NOT NULL) AS {{a}}_rating_detail,
          LIST_MEDIAN({{a}}_rating) AS {{a}}_rating_median,
          LIST_FILTER({{a}}_comment,x->x IS NOT NULL) AS {{a}}_comments,
        {% endfor -%}
       s.rating_ct
  FROM  {{ ref('stg_session')}} s
          LEFT JOIN 
          ratings_agg r
          ON s.session_id = r.session_id