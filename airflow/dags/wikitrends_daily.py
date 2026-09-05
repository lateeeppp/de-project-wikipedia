"""DAG harian: Wikimedia API -> GCS -> BigQuery raw."""

import datetime as dt
import os

import pendulum
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import dag, task

# Skema ini HARUS sama dengan tabel yang sudah dibuat di Console.
SCHEMA_FIELDS = [
    {"name": "project", "type": "STRING", "mode": "REQUIRED"},
    {"name": "access", "type": "STRING", "mode": "REQUIRED"},
    {"name": "pageview_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "article", "type": "STRING", "mode": "REQUIRED"},
    {"name": "views", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "rank", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "_extracted_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "_source_url", "type": "STRING", "mode": "REQUIRED"},
]

def target_date_for(data_interval_start) -> dt.date:
    """Tanggal data yang dikerjakan run ini.

    Wikimedia menerbitkan data pageviews beberapa jam SETELAH harinya selesai,
    jadi run hari ini tidak boleh meminta data hari ini -- pasti 404.
    EXTRACT_LAG_DAYS (=2) memberi jarak aman.
    """
    lag = int(os.environ["EXTRACT_LAG_DAYS"])
    return data_interval_start.date() - dt.timedelta(days=lag)

@dag(
    dag_id="wikitrends_daily",
    # Mulai dari tanggal yang datanya sudah kita buktikan ada.
    start_date=pendulum.datetime(2026, 9, 4, tz="UTC"),
    schedule="@daily",
    # Jangan mengejar semua tanggal yang terlewat begitu DAG dinyalakan.
    catchup=False,
    # Satu run per waktu. Melindungi tabel raw dari dua penulis bersamaan.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": dt.timedelta(minutes=5),
    },
    tags=["wikitrends", "elt"],
)
def wikitrends_daily():
    @task
    def extract(data_interval_start=None) -> str:
        import requests
        from airflow.sdk.exceptions import AirflowFailException

        from pipelines.extract import run

        target_date = target_date_for(data_interval_start)
        try:
            return run(target_date)
        except requests.exceptions.HTTPError as err:
            if err.response is not None and err.response.status_code == 404:
                raise AirflowFailException(
                    f"Data {target_date} belum tersedia di Wikimedia (404)."
                ) from err
            raise

    @task
    def object_path(data_interval_start=None) -> str:
        from pipelines.extract import build_object_path

        return build_object_path(
            os.environ["WIKIMEDIA_PROJECT"], target_date_for(data_interval_start)
        )

    gcs_uri = extract()
    source_object = object_path()

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=os.environ["GCS_BUCKET"],
        source_objects=[source_object],
        # $YYYYMMDD = tulis HANYA ke partisi tanggal itu.
        destination_project_dataset_table=(
            f"{os.environ['GCP_PROJECT_ID']}"
            f".{os.environ['BQ_DATASET_RAW']}"
            ".top_pageviews${{ macros.ds_format("
            "macros.ds_add(ds, -2), '%Y-%m-%d', '%Y%m%d') }}"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        compression="GZIP",
        schema_fields=SCHEMA_FIELDS,
        # Dua baris ini WAJIB cocok dengan tabel yang sudah dibuat di Console.
        # Kalau tidak, BigQuery menolak: "Incompatible table partitioning".
        time_partitioning={"type": "DAY", "field": "pageview_date"},
        cluster_fields=["article"],
        # Hapus-lalu-tulis partisi itu saja. Ini yang membuat run ulang aman.
        write_disposition="WRITE_TRUNCATE",
        # 0 baris rusak ditoleransi: lebih baik gagal keras daripada salah diam.
        max_bad_records=0,
        gcp_conn_id="google_cloud_default",
    )

    @task
    def check_row_count(data_interval_start=None) -> int:
        """Pastikan partisi terisi dan kunci (project, date, article) unik."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        target_date = target_date_for(data_interval_start)
        sql = f"""
            SELECT COUNT(*) AS baris, COUNT(DISTINCT article) AS unik
            FROM `{os.environ['GCP_PROJECT_ID']}.{os.environ['BQ_DATASET_RAW']}.top_pageviews`
            WHERE pageview_date = DATE '{target_date.isoformat()}'
        """
        hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
        baris, unik = hook.get_first(sql)

        # Yang diperiksa adalah yang SELALU benar, bukan angka 993 yang
        # kebetulan benar untuk satu tanggal.
        if baris == 0:
            raise ValueError(f"Partisi {target_date} kosong.")
        if baris != unik:
            raise ValueError(
                f"Artikel duplikat di {target_date}: {baris} baris, {unik} unik."
            )

        print(f"{target_date}: {baris} baris, semua artikel unik.")
        return baris

    gcs_uri >> load_to_bigquery >> check_row_count()


wikitrends_daily()
