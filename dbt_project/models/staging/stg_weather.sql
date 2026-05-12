WITH source as (
    select * from raw.weather_raw
)
SELECT
    city,
    country,
    temp as temp_c,
    feels_like as feels_like_c,
    humidity,
    wind_speed,
    weather_description,
    timestamp,
    ingested_at
from source
where temp is not null and city is not null