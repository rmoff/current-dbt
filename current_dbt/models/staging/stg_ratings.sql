WITH      source_data AS (
          -- Spec #4: Rename fields to remove spaces etc
          SELECT    title           AS session_name,
                    "Rating Type"   AS rating_type,
                    rating,
                    "comment"       AS rating_comment,
                    "Attendee Type" AS attendee_type,
                    -- Spec #7 Create a new field showing if attendee was in-person or not
                    CASE WHEN "Attendee Type" = 'Virtual' THEN 1 ELSE 0 END AS virtual_attendee
                    -- Spec #3: Remove PII data of those who left ratings
          FROM      {{ ref('rating_detail') }}
          )

SELECT    *
FROM      source_data
-- Spec #8: Exclude irrelevant sessions
WHERE     session_name NOT IN ('Breakfast', 'Lunch', 'Registration')
