with stg_data as (

    select * from {{ ref('stg_top_pageviews') }}

),

history_stats as (

    select
        project,
        pageview_date,
        article,
        views,
        rank,
        -- Rata-rata views 28 hari ke belakang (tidak termasuk hari ini)
        avg(views) over (
            partition by project, article
            order by pageview_date
            rows between 28 preceding and 1 preceding
        ) as avg_views_28d,
        -- Standar deviasi views 28 hari ke belakang
        stddev(views) over (
            partition by project, article
            order by pageview_date
            rows between 28 preceding and 1 preceding
        ) as stddev_views_28d,
        -- Jumlah kemunculan artikel dalam 28 hari terakhir
        count(views) over (
            partition by project, article
            order by pageview_date
            rows between 28 preceding and 1 preceding
        ) as appearances_last_28d
    from stg_data

),

trending_metrics as (

    select
        project,
        pageview_date,
        article,
        views,
        rank,
        coalesce(avg_views_28d, 0) as avg_views_28d,
        coalesce(stddev_views_28d, 0) as stddev_views_28d,
        appearances_last_28d,
        -- Z-Score calculation
        case 
            when stddev_views_28d is null or stddev_views_28d = 0 then 0
            else round((views - avg_views_28d) / stddev_views_28d, 2)
        end as z_score,
        -- Flag pendatang baru (tidak pernah muncul dalam 28 hari terakhir)
        case 
            when appearances_last_28d = 0 then true 
            else false 
        end as is_new_entrant
    from history_stats

)

select * from trending_metrics