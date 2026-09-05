"""Test offline untuk pipelines/extract.py -- tanpa jaringan, tanpa kredensial."""

import datetime as dt

import pytest
import requests
import responses
from tenacity import wait_none

from pipelines.extract import build_url, fetch_item, flatten

TARGET_DATE = dt.date(2026, 9, 4)
URL = build_url("id.wikipedia", TARGET_DATE)

# Tiruan respons API. Dua artikel saja -- cukup untuk membuktikan perataan,
# dan JANGAN 993, karena jumlah baris memang berubah tiap tanggal.
ITEM = {
    "project": "id.wikipedia",
    "access": "all-access",
    "articles": [
        {"article": "Istimewa:Pencarian", "views": 21910, "rank": 1},
        {"article": "Halaman_Utama", "views": 16981, "rank": 2},
    ],
}

# Versi tanpa jeda. Tanpa ini, test retry benar-benar menunggu 2+4+8 detik.
fetch_item_cepat = fetch_item.retry_with(wait=wait_none())


def test_build_url_memaksa_dua_digit():
    """API menolak "/2026/9/4"; bulan dan tanggal wajib 2 digit."""
    assert build_url("id.wikipedia", dt.date(2026, 9, 4)).endswith("/2026/09/04")


def test_flatten_meratakan_dan_mengisi_kolom_audit():
    rows = flatten(ITEM, TARGET_DATE, URL)

    assert len(rows) == len(ITEM["articles"])
    assert rows[0]["article"] == "Istimewa:Pencarian"
    assert rows[0]["pageview_date"] == "2026-09-04"
    assert rows[0]["_source_url"] == URL
    # Satu pengambilan = satu penanda batch, jadi nilainya sama di semua baris.
    assert len({row["_extracted_at"] for row in rows}) == 1


@responses.activate
def test_retry_saat_kegagalan_sementara():
    """503 = server sedang bermasalah. Coba lagi, dan kali kedua berhasil."""
    responses.add(responses.GET, URL, status=503)
    responses.add(responses.GET, URL, json={"items": [ITEM]}, status=200)

    item = fetch_item_cepat(URL, "test-agent/0.1")

    assert item["project"] == "id.wikipedia"
    assert len(responses.calls) == 2


@responses.activate
def test_tidak_retry_saat_404():
    """404 = tanggalnya memang tidak ada. Diulang 100x hasilnya tetap 404."""
    responses.add(responses.GET, URL, status=404)

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_item_cepat(URL, "test-agent/0.1")

    # Inti test ini: TEPAT satu panggilan. Kalau jadi 5, retry-nya salah sasaran.
    assert len(responses.calls) == 1
