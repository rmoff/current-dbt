{% set rating_types = ['Overall Experience','Presenter', 'Content'] %}

WITH      source_data AS (
          -- Spec #4: Rename fields to remove spaces etc
          SELECT    TRIM(title)      AS session_name,
                    -- Spec #5: Pivot rating type into individual columns
                    {% for r in rating_types -%}
                      CASE WHEN "Rating Type" = '{{ r }}' THEN rating END AS {{ r.lower().replace(' ','_') }}_rating,
                      CASE WHEN "Rating Type" = '{{ r }}' THEN "comment" END AS {{ r.lower().replace(' ','_') }}_comment,
                    {% endfor -%}
                    -- Spec #7 Create a new field showing if attendee was in-person or not
                    CASE WHEN "Attendee Type" = 'Virtual' THEN 1 ELSE 0 END AS virtual_attendee
                    -- Spec #3: Remove PII data of those who left ratings
          FROM      {{ ref('rating_detail') }}
          )

SELECT    *
FROM      source_data
-- Spec #8: Exclude irrelevant sessions
WHERE     session_name NOT IN ('Breakfast', 'Lunch', 'Registration')
