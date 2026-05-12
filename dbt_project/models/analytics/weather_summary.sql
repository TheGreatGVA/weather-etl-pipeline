with staging as (
    select * from {{ ref('stg_weather') }}
)
select
    city,
    country,
    DATE(timestamp) as weather_date,
    round(avg(temp_c)::numeric,2) as avg_temp_c,
    max(temp_c) as max_temp_c,
    min(temp_c) as min_temp_c,
    round(avg(humidity)::numeric,2) as avg_humidity,
    max(wind_speed) as max_wind_speed
from staging 
group by city, country, weather_date
order by weather_date desc, city