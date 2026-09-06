with source as (
    select
        *
    from
        {{ source('raw', 'top_pageviews') }}
),
renamed_and_filtered as (
    select
        project,
        pageview_date,
        article,
        views,
        rank,
        _extracted_at,
        _source_url
    from
        source
    where
        article not in ('Halaman_Utama', 'Istimewa: Pencarian')
        and article not like 'Istimewa:%'
        and article not like 'Special:%'
        and article not like 'Berkas:%'
        and article not like 'Wikipedia:%'
        and article not like 'Portal:%'
        and article not like 'Kategori:%'
        and article not like 'Templat:%'
        and article not like 'Bantuan:%'
        and article not like 'Pengguna:%'
        and article not like 'Pembicaraan:%'
        and article not like 'Modul:%'
)
select * from renamed_and_filtered
