select
    *
from
    {{ source('raw', 'top_pageviews') }}