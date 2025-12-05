# Pipeline ETL Airflow Data Transformasi STT

Pipeline ini merupakan ETL (Extract, Transform, Load) menggunakan Apache Airflow yang memproses dua file CSV (`STT1.csv` dan `STT2.csv`), melakukan pembersihan data, penggabungan, pengklasifikasian perhitungan debit/credit dan menghasilkan file final hasil transformasi.

---

## 📁 Struktur Folder
**note : Bucket (storage) dibuat di local.**

project/

│── script_stt_transformation.py

│── STT1.csv

│── STT2.csv

│── bucket/ (auto-generated)

│ ├── stt1_raw.csv

│ ├── stt2_raw.csv

│ ├── stt1_clean.csv

│ └── stt2_clean.csv

│── result/ (auto-generated)

│ └── result_transform_stt.csv

Folder `bucket` dan `result` akan dibuat secara otomatis oleh Airflow.

Pada Git terlampir untuk hasil transformasi dengan nama file result_transform_stt.csv.

---

## Alur Kerja Program

### 1. **Load Data**
- Membaca file sumber (`STT1.csv`, `STT2.csv`)
- Menyimpannya ke folder `bucket/` sebagai raw data

### 2. **Cleansing Data**
Membersihkan data berdasarkan:
- Menghapus row dengan kolom kosong
- Konversi kolom date menjadi tipe `datetime`
- Konversi kolom `amount` menjadi numeric
- Menghapus row invalid (`date` atau `amount` bernilai NaT/NaN)

### 3. **Transformasi Data**
- Merge STT1 + STT2
- Remove duplicate berdasarkan `number`
- Membuat kolom:
  - `Debit` → jika `client_type = 'C'`
  - `Credit` → jika `client_type = 'V'`
- Grouping berdasarkan:
  - `date`
  - `client_code`
- Menghasilkan:
  - total debit
  - total credit
  - jumlah transaksi harian per client

### 4. **Output Final**
File hasil transformasi disimpan di: 

result/result_transform_stt.csv

---





