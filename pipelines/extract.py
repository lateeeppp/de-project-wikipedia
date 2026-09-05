"""Langkah 4: extract dengan retry, siap dipanggil Airflow dengan tanggal apa pun."""

import datetime as dt
import gzip
import json
import os
import sys

import requests
from dotenv import load_dotenv
from google.cloud import storage
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

load_dotenv()

BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top"
ACCESS = "all-access"

# Kegagalan SEMENTARA: server sibuk atau sedang bermasalah, layak dicoba lagi.
# 404 dan 403 SENGAJA tidak di sini -- lihat komentar di fetch_item().
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RetryableHTTPError(Exception):
    """Penanda bahwa kegagalan ini sementara, jadi boleh dicoba ulang."""


def build_url(project: str, target_date: dt.date) -> str:
    """Susun URL API. :02d memaksa 2 digit -> "09", bukan "9"; API menolak "9"."""
    return (
        f"{BASE_URL}/{project}/{ACCESS}"
        f"/{target_date.year:04d}/{target_date.month:02d}/{target_date.day:02d}"
    )


@retry(
    retry=retry_if_exception_type(RetryableHTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_item(url: str, user_agent: str) -> dict:
    """Ambil satu hari data. Hanya kegagalan sementara yang diulang.

    404 berarti tanggalnya memang tidak ada -- diulang 100x hasilnya tetap 404,
    jadi langsung gagal supaya kita tahu masalahnya sekarang, bukan 20 menit lagi.
    """
    response = requests.get(url, headers={"User-Agent": user_agent}, timeout=30)
    if response.status_code in RETRYABLE_STATUS:
        raise RetryableHTTPError(f"HTTP {response.status_code} dari {url}")
    response.raise_for_status()
    return response.json()["items"][0]


def flatten(item: dict, target_date: dt.date, source_url: str) -> list[dict]:
    """Dari 1 objek bersarang -> banyak baris datar, seperti baris tabel.

    extracted_at dihitung SEKALI di sini, bukan per baris, supaya seluruh baris
    hasil satu pengambilan punya penanda batch yang sama.
    """
    extracted_at = dt.datetime.now(dt.UTC).isoformat()
    return [
        {
            "project": item["project"],
            "access": item["access"],
            "pageview_date": target_date.isoformat(),
            "article": article["article"],
            "views": article["views"],
            "rank": article["rank"],
            # Dua kolom audit: kapan diambil, dan dari URL mana.
            "_extracted_at": extracted_at,
            "_source_url": source_url,
        }
        for article in item["articles"]
    ]


def to_jsonl_gz(rows: list[dict]) -> bytes:
    """JSONL (satu objek per baris) lalu gzip.

    ensure_ascii=False supaya huruf non-ASCII utuh. Gzip mengecilkan ~10x dan
    BigQuery mendekompresinya sendiri saat load, jadi tidak ada kerugian.
    """
    body = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    return gzip.compress(body.encode("utf-8"))


def build_object_path(project: str, target_date: dt.date) -> str:
    """Path bergaya partisi Hive: satu tanggal = satu folder.

    Nama file tetap, jadi menjalankan ulang tanggal yang sama MENIMPA,
    bukan menumpuk. Inilah yang membuat backfill aman diulang.
    """
    return (
        f"raw/top_pageviews/project={project}"
        f"/dt={target_date.isoformat()}/top_pageviews.jsonl.gz"
    )


def upload_to_gcs(bucket_name: str, object_path: str, payload: bytes) -> None:
    """Kirim ke GCS.

    storage.Client() tanpa argumen: kredensial dicari lewat
    GOOGLE_APPLICATION_CREDENTIALS, yang baru ada di os.environ SETELAH
    load_dotenv() jalan. Urutannya penting.
    """
    bucket = storage.Client().bucket(bucket_name)
    bucket.blob(object_path).upload_from_string(
        payload, content_type="application/gzip"
    )


def default_target_date() -> dt.date:
    """Tanggal aman untuk dijalankan manual: hari ini dikurangi lag.

    Airflow TIDAK memakai ini -- dia mengisi target_date secara eksplisit dari
    data_interval_start miliknya. Ini cuma kemudahan saat mengetes dari terminal.
    """
    lag = int(os.environ["EXTRACT_LAG_DAYS"])
    return dt.datetime.now(dt.UTC).date() - dt.timedelta(days=lag)


def run(target_date: dt.date, project: str | None = None) -> str:
    """Rangkai semuanya: ambil -> ratakan -> kompres -> unggah."""
    project = project or os.environ["WIKIMEDIA_PROJECT"]
    url = build_url(project, target_date)

    item = fetch_item(url, os.environ["WIKIMEDIA_USER_AGENT"])
    rows = flatten(item, target_date, url)

    object_path = build_object_path(project, target_date)
    upload_to_gcs(os.environ["GCS_BUCKET"], object_path, to_jsonl_gz(rows))

    gcs_uri = f"gs://{os.environ['GCS_BUCKET']}/{object_path}"
    print(f"{len(rows)} baris -> {gcs_uri}")
    return gcs_uri


if __name__ == "__main__":
    # Argumen opsional: tanggal YYYY-MM-DD. Tanpa argumen -> pakai lag dari .env.
    target = (
        dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1
        else default_target_date()
    )
    run(target)
