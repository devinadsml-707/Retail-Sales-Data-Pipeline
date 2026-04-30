"""
load_to_postgres.py
====================
Script to create table_m3 in PostgreSQL and load the raw CSV data.
Run this ONCE after starting docker-compose to seed the source database.

Usage:
    python scripts/load_to_postgres.py

Requirements:
    pip install pandas psycopg2-binary sqlalchemy
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

# ── Connection Config ─────────────────────────────────────────
DB_CONFIG = {
    "user":     "airflow",
    "password": "airflow",
    "host":     "localhost",
    "port":     "5434",          # mapped port from docker-compose
    "database": "milestone3",
}

RAW_CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "retail_transactions_raw.csv")


def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)


def create_database_if_not_exists():
    """Create the milestone3 database if it doesn't exist."""
    config = {**DB_CONFIG, "database": "airflow"}
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'milestone3'")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE milestone3")
            print("✅ Database 'milestone3' created")
        else:
            print("ℹ️  Database 'milestone3' already exists")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️  Could not create database: {e}")


def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS table_m3")
    cursor.execute("""
        CREATE TABLE table_m3 (
            "Invoice no"      VARCHAR(50) PRIMARY KEY,
            "Customer id"     VARCHAR(50),
            "Gender"          VARCHAR(10),
            "Age"             DECIMAL(10, 2),
            "Payment method"  VARCHAR(50),
            "Category"        VARCHAR(100),
            "Quantity"        INT,
            "Price"           DECIMAL(10, 2),
            "Invoice date"    DATE,
            "Shopping mall"   VARCHAR(100)
        )
    """)
    conn.commit()
    print("✅ Table 'table_m3' created")
    cursor.close()


def load_data(conn):
    if not os.path.exists(RAW_CSV_PATH):
        print(f"❌ CSV not found at: {RAW_CSV_PATH}")
        sys.exit(1)

    df = pd.read_csv(RAW_CSV_PATH)
    print(f"📂 Loaded CSV: {df.shape[0]} rows × {df.shape[1]} columns")

    cursor = conn.cursor()
    cols = ', '.join([f'"{c}"' for c in df.columns])
    rows = [tuple(r) for r in df.values]

    execute_values(cursor, f'INSERT INTO table_m3 ({cols}) VALUES %s', rows)
    conn.commit()
    print(f"✅ Inserted {len(df)} rows into table_m3")
    cursor.close()


def verify(conn):
    df = pd.read_sql("SELECT COUNT(*) as total FROM table_m3", conn)
    print(f"🔍 Verification — rows in table_m3: {df['total'].iloc[0]}")


if __name__ == "__main__":
    create_database_if_not_exists()
    conn = get_connection()
    create_table(conn)
    load_data(conn)
    verify(conn)
    conn.close()
    print("\n🎉 Done! You can now trigger the Airflow DAG at http://localhost:8080")
