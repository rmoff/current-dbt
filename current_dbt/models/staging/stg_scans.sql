-- {% set tracks = ['Architectures You\'ve Always Wondered About','Case Studies','Data Development Life Cycle','Developing Real-Time Applications','Event Streaming in Academia and Beyond','Fun and Geeky','Kafka Summit','Modern Data Flow','Operations and Observability','Panel','People & Culture','Real Time Analytics','Sponsored Session','Streaming Technologies'] %}
{% set tracks = ['Case Studies','Data Development Life Cycle','Developing Real-Time Applications','Event Streaming in Academia and Beyond','Fun and Geeky','Kafka Summit','Modern Data Flow','Operations and Observability','Panel','People & Culture','Real Time Analytics','Sponsored Session','Streaming Technologies'] %}

WITH      source_data AS (
          -- Spec #4: Rename fields to remove spaces etc
          SELECT    TRIM(name)             AS session_name,
                    Speakers               AS speakers,
                    TRY_CAST(scans AS INT) AS scans,
                    "# Survey Responses"   AS rating_ct,
                    -- Spec #9 Combine all track fields into a single summary
                    {% for t in tracks -%}
                    CASE WHEN "{{ t }}" IS NOT NULL THEN ['{{ t }}'] END 
                                           AS F{{ loop.index }},
                    {% endfor -%}
          FROM      {{ ref('session_scans') }}
          )
SELECT    session_name,
          speakers,
          scans,
          rating_ct,
          -- LIST_CONCAT takes two parameters, so we're going to stack them. 
          -- Write a nested LIST_CONCAT for all but one occurance of the tracks
          {% for x in range((tracks|length -1)) -%}
            LIST_CONCAT(
          {% endfor -%}
          -- For every track…
          {% for t in tracks -%}
            -- Write out the field number
            F{{ loop.index }} 
            -- Unless it's the first one, add a close parenthesis
            {% if loop.index !=1  %}) {% endif %} 
            -- Unless it's the last one, add a comma
            {% if loop.index < tracks|length %}, {% endif %}
          {% endfor -%} 
          AS track 
FROM      source_data