# src/dashboard/app.py

import streamlit as st
import pandas as pd
import os

# Atur judul halaman
st.set_page_config(page_title="Insightify Market Suite", layout="wide")

# Fungsi untuk memuat data (dengan caching agar lebih cepat)
@st.cache_data
def load_data():
    path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'processed', 'master_table.parquet')
    df = pd.read_parquet(path)
    return df

# Muat data
df = load_data()

# --- Mulai membuat tampilan dashboard ---

st.title("🚀 Insightify Market Suite")
st.markdown("Dashboard Analisis E-Commerce End-to-End")

# --- KPI Utama ---
st.header("📈 KPI Utama")

# Hitung KPI
total_revenue = df['payment_value'].sum()
total_orders = df['order_id'].nunique()
total_customers = df['customer_unique_id'].nunique()
avg_revenue_per_order = total_revenue / total_orders

# Tampilkan KPI dalam kolom
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Pendapatan (R$)", f"{total_revenue:,.2f}")
col2.metric("Total Pesanan", f"{total_orders:,}")
col3.metric("Total Pelanggan", f"{total_customers:,}")
col4.metric("Pendapatan/Pesanan (R$)", f"{avg_revenue_per_order:,.2f}")

# --- Tambahkan visualisasi dari EDA (Contoh) ---
st.header("📊 Analisis Penjualan")

# Tren penjualan bulanan
st.subheader("Tren Penjualan Bulanan")
df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
monthly_revenue = df.set_index('order_purchase_timestamp').groupby(pd.Grouper(freq='M'))['payment_value'].sum()
st.line_chart(monthly_revenue)

# Produk terlaris
st.subheader("Top 10 Kategori Produk Terlaris")
top_10_categories = df['product_category_name_english'].value_counts().head(10)
st.bar_chart(top_10_categories)