SELECT s.session_id, 
       s.session_name, 
       s.speakers, 
       r.virtual_attendee,
       r.overall_experience_rating, 
       r.presenter_rating,
       r.content_rating,
       r.overall_experience_comment, 
       r.presenter_comment,
       r.content_comment
  FROM  {{ ref('stg_ratings')}} r
          LEFT JOIN 
          {{ ref('stg_session') }} s
          ON s.session_name = r.session_name