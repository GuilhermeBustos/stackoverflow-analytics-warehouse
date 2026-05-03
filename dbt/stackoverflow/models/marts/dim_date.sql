{{
    config(
        cluster_by=['date_day']
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2008-01-01' as date)",
        end_date="cast('2022-12-31' as date)"
    ) }}
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter_of_year,
    EXTRACT(MONTH FROM date_day) AS month_of_year,
    FORMAT_DATE('%B', date_day) AS month_name,
    FORMAT_DATE('%b', date_day) AS month_name_short,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%a', date_day) AS day_name_short,
    DATE_TRUNC(date_day, WEEK) AS week_start_date,
    DATE_TRUNC(date_day, MONTH) AS month_start_date,
    {{ last_day('date_day', 'month') }} AS month_end_date,
    DATE_TRUNC(date_day, QUARTER)    AS quarter_start_date,
    {{ last_day('date_day', 'quarter') }} AS quarter_end_date,
    DATE_TRUNC(date_day, YEAR) AS year_start_date,
    {{ last_day('date_day', 'year') }}  AS year_end_date
FROM date_spine