"""Langkah 2: ratakan data mentah jadi baris rapi, lalu simpan sebagai file JSONL."""

import datetime as dt
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

PROJECT = "id.wikipedia"
TARGET_DATE = dt.date(2026, 9, 4)

# :02d memaksa 2 digit -> "09", bukan "9". API menolak kalau tidak 2 digit.
url = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/top"
    f"/{PROJECT}/all-access"
    f"/{TARGET_DATE.year:04d}/{TARGET_DATE.month:02d}/{TARGET_DATE.day:02d}"
)

response = requests.get(
    url,
    headers={"User-Agent": os.environ["WIKIMEDIA_USER_AGENT"]},
    timeout=30,
)
response.raise_for_status()

item = response.json()["items"][0]
extracted_at = dt.datetime.now(dt.UTC).isoformat()

# Meratakan: dari 1 objek bersarang -> banyak baris datar, seperti baris tabel.
# Semua baris disimpan apa adanya, termasuk Istimewa:Pencarian dan Halaman_Utama.
rows = [
    {
        "project": item["project"],
        "access": item["access"],
        "pageview_date": TARGET_DATE.isoformat(),
        "article": article["article"],
        "views": article["views"],
        "rank": article["rank"],
        # Dua kolom audit: kapan diambil, dan dari URL mana.
        "_extracted_at": extracted_at,
        "_source_url": url,
    }
    for article in item["articles"]
]

out_dir = Path("data")
out_dir.mkdir(exist_ok=True)  # exist_ok=True -> aman dijalankan berulang kali
out_path = out_dir / f"top_pageviews_{TARGET_DATE.isoformat()}.jsonl"

# JSONL: satu objek JSON per baris. ensure_ascii=False supaya huruf non-ASCII utuh.
out_path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows))

print(f"{len(rows)} baris -> {out_path}")
