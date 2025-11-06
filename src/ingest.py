# src/ingest.py

import pandas as pd
import os
import logging

# Setup logging dasar untuk melihat proses apa yang sedang berjalan
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Tentukan path (lokasi) folder data kita
RAW_DATA_PATH = 'data/raw'
PROCESSED_DATA_PATH = 'data/processed'

def clean_col_names(df):
    """Membersihkan nama kolom pada DataFrame."""
    cols = df.columns
    new_cols = []
    for col in cols:
        new_col = col.replace(' ', '_').lower() # Ganti spasi dengan underscore, buat jadi lowercase
        new_cols.append(new_col)
    df.columns = new_cols
    return df

def ingest_data():
    """
    Fungsi utama untuk membaca semua file CSV, membersihkan nama kolom,
    dan menyimpannya sebagai file Parquet.
    """
    logging.info("Memulai proses ingestion data...")
    
    # Buat folder processed jika belum ada
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    
    # Dapatkan daftar semua file di folder raw
    files = os.listdir(RAW_DATA_PATH)
    csv_files = [f for f in files if f.endswith('.csv')]
    
    for file_name in csv_files:
        try:
            raw_file_path = os.path.join(RAW_DATA_PATH, file_name)
            logging.info(f"Membaca file: {file_name}")
            
            df = pd.read_csv(raw_file_path)
            
            # Bersihkan nama kolom
            df = clean_col_names(df)
            
            # Ubah nama file dari .csv menjadi .parquet
            processed_file_name = file_name.replace('.csv', '.parquet')
            processed_file_path = os.path.join(PROCESSED_DATA_PATH, processed_file_name)
            
            # Simpan sebagai Parquet
            df.to_parquet(processed_file_path, index=False)
            logging.info(f"Berhasil menyimpan {processed_file_name} di {PROCESSED_DATA_PATH}")

        except Exception as e:
            logging.error(f"Gagal memproses file {file_name}: {e}")
    
    logging.info("Proses ingestion data selesai.")

if __name__ == '__main__':
    ingest_data()