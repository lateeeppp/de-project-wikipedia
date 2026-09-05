"""Langkah 1: lihat sendiri bentuk data dari sumbernya."""

import os

import requests
from dotenv import load_dotenv

load_dotenv()

URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/top"
    "/id.wikipedia/all-access/2026/09/04"
)

response = requests.get(
    URL,
    headers={"User-Agent": os.environ["WIKIMEDIA_USER_AGENT"]},
    timeout=30,
)
response.raise_for_status()

articles = response.json()["items"][0]["articles"]

print("jumlah baris:", len(articles))
for article in articles[:10]:
    print(article["rank"], article["views"], article["article"])
