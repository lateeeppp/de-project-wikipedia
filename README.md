# WikiTrends Indonesia

Pipeline ELT harian untuk mendeteksi artikel Wikipedia Indonesia yang mengalami lonjakan pageviews tidak biasa.

## Pertanyaan Bisnis

Artikel apa yang sedang melonjak tidak wajar di Wikipedia Indonesia hari ini dibandingkan kebiasaan historisnya?

## Arsitektur

![Data Pipeline](./data/pipeline-wikipedia.png)

## Nilai Bisnis

Pipeline ini mengubah data pageviews mentah menjadi sinyal tren yang dapat digunakan oleh:

- Data analyst untuk menganalisis pola perhatian publik.
- Tim editorial untuk menemukan topik yang sedang ramai.
- Content strategist untuk menentukan topik yang perlu diperbarui.

## Data Model

### Raw

Raw Menyimpan data mentah dari Wikimedia API. Tabel dipartisi berdasarkan pageview_date dan di-cluster berdasarkan article.

### Staging

Membersihkan judul artikel dari kata kunci prefix bawaan wikipedia, seperti:

- Istimewa:\*
- Halaman_Utama
- Portal:\*
- Dan sebagainya

### Marts/Analytics

Menyediakan metrik tambahan seperti:

- average views 28 hari terakhir
- standar deviasi views 28 hari terakhir
- Z-score 28 hari terakhir
- Total views 28 hari terakhir
- Status Artikel Baru/Lama
