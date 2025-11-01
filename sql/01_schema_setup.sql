-- 01_setup.sql
-- Purpose: Create database schema and import CSV data into SQLite
PRAGMA foreign_keys = ON;
-- Drop tables if they exist
DROP TABLE IF EXISTS marketing_spend;
DROP TABLE IF EXISTS revenue;
DROP TABLE IF EXISTS external_factors;
-- Create tables
CREATE TABLE marketing_spend (
    date TEXT PRIMARY KEY,
    paid_search REAL,
    paid_social REAL,
    display REAL,
    email REAL,
    affiliate REAL,
    tv REAL
);
CREATE TABLE revenue (
    date TEXT PRIMARY KEY,
    revenue REAL,
    transactions INTEGER,
    new_customers INTEGER
);
CREATE TABLE external_factors (
    date TEXT PRIMARY KEY,
    is_weekend INTEGER,
    is_holiday INTEGER,
    promotion_active INTEGER,
    competitor_index REAL,
    seasonality_index REAL
);
-- Import CSV files
.mode csv
.import data/marketing_spend.csv marketing_spend
.import data/revenue.csv revenue
.import data/external_factors.csv external_factors
