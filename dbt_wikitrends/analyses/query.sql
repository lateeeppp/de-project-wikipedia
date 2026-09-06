select distinct article from (
    select
        article
    from
        {{ source('raw', 'top_pageviews') }}
    order by article asc
)