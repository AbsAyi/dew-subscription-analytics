with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-12-01' as date)",
        end_date="cast('2026-06-30' as date)"
    ) }}
),

dates as (
    select
        date_day                                    as date_key,
        date_day                                    as full_date,
        extract(year from date_day)                 as year,
        extract(month from date_day)                as month_number,
        extract(day from date_day)                  as day_of_month,
        dayname(date_day)                           as day_name,
        monthname(date_day)                         as month_name,
        date_trunc('month', date_day)               as month_start,
        last_day(date_day)                          as month_end,
        extract(quarter from date_day)              as quarter_number,
        year(date_day) || '-Q' || extract(quarter from date_day) as year_quarter,
        year(date_day) || '-' || lpad(extract(month from date_day), 2, '0') as year_month,
        case
            when extract(dayofweek from date_day) in (0, 6) then true
            else false
        end as is_weekend
    from date_spine
)

select * from dates