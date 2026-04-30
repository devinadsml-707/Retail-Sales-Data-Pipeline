-- =============================================================
-- db_schema.sql
-- DDL for the retail ETL pipeline source table
-- Database: milestone3 (PostgreSQL)
-- =============================================================

-- Drop if exists (safe re-run)
DROP TABLE IF EXISTS table_m3;

-- Create source table
CREATE TABLE table_m3 (
    "Invoice no"      VARCHAR(50)     PRIMARY KEY,
    "Customer id"     VARCHAR(50),
    "Gender"          VARCHAR(10),
    "Age"             DECIMAL(10, 2),
    "Payment method"  VARCHAR(50),
    "Category"        VARCHAR(100),
    "Quantity"        INT,
    "Price"           DECIMAL(10, 2),
    "Invoice date"    DATE,
    "Shopping mall"   VARCHAR(100)
);

-- =============================================================
-- Optional: merge raw Kaggle CSVs (sales_data + customer_data)
-- Run this if starting from the original Kaggle download
-- =============================================================

CREATE TABLE IF NOT EXISTS sales_data (
    "Invoice no"    VARCHAR(50),
    "Customer id"   VARCHAR(50),
    "Category"      VARCHAR(100),
    "Quantity"      INT,
    "Price"         DECIMAL(10, 2),
    "Invoice date"  DATE,
    "Shopping mall" VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS customer_data (
    "Customer id"    VARCHAR(50),
    "Gender"         VARCHAR(10),
    "Age"            INT,
    "Payment method" VARCHAR(50)
);

-- Merge into raw working table
CREATE TABLE raw_transactions AS
SELECT
    s."Invoice no",
    s."Customer id",
    c."Gender",
    c."Age",
    c."Payment method",
    s."Category",
    s."Quantity",
    s."Price",
    s."Invoice date",
    s."Shopping mall"
FROM sales_data s
LEFT JOIN customer_data c
    ON s."Customer id" = c."Customer id";
