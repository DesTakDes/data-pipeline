-- Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Staging: data mentah dari CSV
CREATE TABLE IF NOT EXISTS staging.raw_data (
    name            VARCHAR(255),
    url             TEXT,
    street_address  TEXT,
    city            VARCHAR(100),
    state           VARCHAR(100),
    zip_code        VARCHAR(20),
    country         VARCHAR(100),
    phone_number_1  VARCHAR(50),
    phone_number_2  VARCHAR(50),
    fax_1           VARCHAR(50),
    fax_2           VARCHAR(50),
    email_1         VARCHAR(255),
    email_2         VARCHAR(255),
    open_hours      TEXT,
    latitude        VARCHAR(50),
    longitude       VARCHAR(50),
    facebook        TEXT,
    twitter         TEXT,
    instagram       TEXT
);

-- Staging: setelah transform
CREATE TABLE IF NOT EXISTS staging.transformed_data (
    name            VARCHAR(255),
    url             TEXT,
    street_address  TEXT,
    city            VARCHAR(100),
    state           VARCHAR(100),
    zip_code        VARCHAR(20),
    country         VARCHAR(100),
    phone_number_1  VARCHAR(50),
    phone_number_2  VARCHAR(50),
    fax_1           VARCHAR(50),
    fax_2           VARCHAR(50),
    email_1         VARCHAR(255),
    email_2         VARCHAR(255),
    open_hours      TEXT,
    latitude        NUMERIC,
    longitude       NUMERIC,
    facebook        TEXT,
    twitter         TEXT,
    instagram       TEXT,
    date_partition  DATE,
    processed_at    TIMESTAMP
);

-- Warehouse: fact table
CREATE TABLE IF NOT EXISTS warehouse.fact_table (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255),
    url             TEXT,
    street_address  TEXT,
    city            VARCHAR(100),
    state           VARCHAR(100),
    zip_code        VARCHAR(20),
    country         VARCHAR(100),
    phone_number_1  VARCHAR(50),
    phone_number_2  VARCHAR(50),
    fax_1           VARCHAR(50),
    fax_2           VARCHAR(50),
    email_1         VARCHAR(255),
    email_2         VARCHAR(255),
    open_hours      TEXT,
    latitude        NUMERIC,
    longitude       NUMERIC,
    facebook        TEXT,
    twitter         TEXT,
    instagram       TEXT,
    date_partition  DATE,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- Warehouse: summary per state
CREATE TABLE IF NOT EXISTS warehouse.state_summary (
    state           VARCHAR(100) PRIMARY KEY,
    total_stores    INTEGER,
    total_cities    INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Warehouse: summary per city
CREATE TABLE IF NOT EXISTS warehouse.city_summary (
    city            VARCHAR(100),
    state           VARCHAR(100),
    total_stores    INTEGER,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (city, state)
);

-- Database Metabase
CREATE DATABASE metabase;

-- Index
CREATE INDEX IF NOT EXISTS idx_fact_state   ON warehouse.fact_table (state);
CREATE INDEX IF NOT EXISTS idx_fact_city    ON warehouse.fact_table (city);
CREATE INDEX IF NOT EXISTS idx_fact_country ON warehouse.fact_table (country);
CREATE INDEX IF NOT EXISTS idx_fact_zip     ON warehouse.fact_table (zip_code);