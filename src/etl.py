# src/etl.py

import pandas as pd
import os

def clean_and_merge_data():
    """
    Fungsi ini membaca data mentah, membersihkan, menggabungkan semua tabel
    menjadi satu master table, dan menyimpannya ke /data/processed.
    """
    print("Memulai proses ETL...")

    # Tentukan path
    RAW_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
    PROCESSED_DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

    # Buat folder processed jika belum ada
    if not os.path.exists(PROCESSED_DATA_PATH):
        os.makedirs(PROCESSED_DATA_PATH)

    # 1. Baca semua dataset yang diperlukan
    print("Membaca semua dataset mentah...")
    customers = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_customers_dataset.csv"))
    orders = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_orders_dataset.csv"))
    order_items = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_order_items_dataset.csv"))
    products = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_products_dataset.csv"))
    payments = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_order_payments_dataset.csv"))
    reviews = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_order_reviews_dataset.csv"))
    sellers = pd.read_csv(os.path.join(RAW_DATA_PATH, "olist_sellers_dataset.csv"))
    translation = pd.read_csv(os.path.join(RAW_DATA_PATH, "product_category_name_translation.csv"))

    # 2. Lakukan pembersihan dan konversi tipe data
    print("Membersihkan dan mengubah tipe data...")
    # Mengubah kolom tanggal menjadi datetime
    for df in [orders]:
        for col in df.columns:
            if 'timestamp' in col or '_date' in col:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    # Mengisi nilai kosong di produk
    products['product_category_name'].fillna('unknown', inplace=True)

    # 3. Gabungkan semua tabel menjadi satu "Master Table"
    print("Menggabungkan tabel menjadi Master Table...")
    # Mulai dari orders, gabungkan dengan informasi lainnya
    df = orders.merge(customers, on='customer_id')
    df = df.merge(order_items, on='order_id')
    df = df.merge(products, on='product_id')
    df = df.merge(payments, on='order_id')
    df = df.merge(reviews, on='order_id')
    df = df.merge(sellers, on='seller_id')
    df = df.merge(translation, on='product_category_name', how='left') # 'left' join untuk jaga semua baris

    # 4. Simpan Master Table
    print("Menyimpan Master Table...")
    df.to_parquet(os.path.join(PROCESSED_DATA_PATH, 'master_table.parquet'))

    print("Proses ETL selesai. master_table.parquet disimpan di /data/processed/")

if __name__ == '__main__':
    clean_and_merge_data()