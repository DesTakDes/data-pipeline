from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--date', required=True)
parser.add_argument('--csv',  required=True)
args = parser.parse_args()

# Init Spark session
spark = SparkSession.builder \
    .appName('ETL Transform') \
    .config('spark.jars', '/opt/spark/jars/postgresql-42.6.0.jar') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

JDBC_URL  = 'jdbc:postgresql://postgres:5432/airflow'
JDBC_PROP = {
    'user':     'airflow',
    'password': 'airflow',
    'driver':   'org.postgresql.Driver'
}

print(f'Reading from staging.raw_data...')

# Baca dari staging
df = spark.read.jdbc(JDBC_URL, 'staging.raw_data', properties=JDBC_PROP)
print(f'Raw data: {df.count()} rows, {len(df.columns)} columns')

# ── Transformasi dinamis ──────────────────────────────────────────

# 1. Bersihkan semua kolom string — trim whitespace
string_cols = [f.name for f in df.schema.fields
               if str(f.dataType) == 'StringType()']
for col in string_cols:
    df = df.withColumn(col, F.trim(F.col(col)))

# 2. Replace string kosong dengan null
for col in string_cols:
    df = df.withColumn(col,
        F.when(F.col(col) == '', None).otherwise(F.col(col)))

# 3. Hapus duplikat
df_clean = df.dropDuplicates()
print(f'After dedup: {df_clean.count()} rows')

# 4. Tambah kolom metadata
df_clean = df_clean \
    .withColumn('_date_partition', F.lit(args.date).cast('date')) \
    .withColumn('_processed_at',   F.current_timestamp())

# ── Tulis ke staging.transformed_data ────────────────────────────
df_clean.write.jdbc(
    JDBC_URL,
    'staging.transformed_data',
    mode='overwrite',
    properties=JDBC_PROP
)
print(f'Written to staging.transformed_data')

# ── Agregasi summary (kalau ada kolom yang relevan) ───────────────
cols_lower = [c.lower() for c in df_clean.columns]

if 'state' in cols_lower and 'city' in cols_lower:
    df_state = df_clean.groupBy('state').agg(
        F.count('*').alias('total_stores'),
        F.countDistinct('city').alias('total_cities'),
        F.current_timestamp().alias('updated_at')
    )
    df_state.write.jdbc(
        JDBC_URL,
        'warehouse.state_summary',
        mode='overwrite',
        properties=JDBC_PROP
    )
    print('State summary written')

spark.stop()
print('Spark transform complete')