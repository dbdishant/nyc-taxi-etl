"""Load a small, validated sample of NYC Yellow Taxi Parquet into PostgreSQL."""

import argparse
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from pathlib import Path
from urllib.request import urlopen

import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, inspect, text

load_dotenv()

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2025-01.parquet"
)
PARQUET_PATH = Path("data/yellow_tripdata_2025-01.parquet")
TABLE_NAME = "taxi_trip_data"
BATCH_SIZE = 10000

COLUMN_MAPPING = {
    "VendorID": "vendorid",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "ratecodeid",
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
    "Airport_fee": "airport_fee",
}

SOURCE_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee",
]

TARGET_COLUMNS = [COLUMN_MAPPING.get(c, c) for c in SOURCE_COLUMNS]

INTEGER_COLUMNS = [
    "vendorid", "passenger_count", "ratecodeid",
    "pulocationid", "dolocationid", "payment_type",
]

MONEY_COLUMNS = [
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
]

def get_engine():
    required = ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME")
    missing = [key for key in required if os.getenv(key) is None]
    if missing:
        raise ValueError(f"Missing .env variables: {', '.join(missing)}")
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
    )
    return create_engine(url, connect_args={"connect_timeout": 10})

def download_if_missing():
    if PARQUET_PATH.exists():
        print(f"Using cached file: {PARQUET_PATH}")
        return
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    partial = PARQUET_PATH.with_suffix(".parquet.part")
    print("Downloading Parquet file (the full file is downloaded even in test mode)...")
    try:
        with urlopen(SOURCE_URL, timeout=60) as response, partial.open("wb") as out:
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
        partial.replace(PARQUET_PATH)
    finally:
        partial.unlink(missing_ok=True)
    print(f"Saved to {PARQUET_PATH}")


def validate_schema(parquet_file, engine):
    actual = set(parquet_file.schema_arrow.names)
    missing = set(SOURCE_COLUMNS) - actual
    if missing:
        raise ValueError(f"Parquet is missing columns: {sorted(missing)}")
    inspector = inspect(engine)
    if not inspector.has_table(TABLE_NAME):
        raise RuntimeError(f"Table '{TABLE_NAME}' not found. Run schema.sql first.")
    db_columns = {col["name"] for col in inspector.get_columns(TABLE_NAME)}
    missing_db = set(TARGET_COLUMNS) - db_columns
    if missing_db:
        raise ValueError(f"Database table is missing columns: {sorted(missing_db)}")
    print("Source and destination schema checks passed.")


def clean_data(df):
    df = df.rename(columns=COLUMN_MAPPING)[TARGET_COLUMNS].copy()
    for col in ("pickup_datetime", "dropoff_datetime"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    valid = df["pickup_datetime"].notna() & df["dropoff_datetime"].notna()
    valid &= df["dropoff_datetime"] >= df["pickup_datetime"]

    for col in INTEGER_COLUMNS:
        values = pd.to_numeric(df[col], errors="coerce")
        ok = values.isna() | (
            values.between(-2147483648, 2147483647)
            & (values % 1 == 0)
        )
        valid &= ok
        df[col] = values.astype("Float64")

    distance = pd.to_numeric(df["trip_distance"], errors="coerce")
    valid &= distance.isna() | (distance >= 0)
    df["trip_distance"] = distance
    passengers = pd.to_numeric(df["passenger_count"], errors="coerce")
    valid &= passengers.isna() | (passengers >= 0)

    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype("string")
    valid &= df["store_and_fwd_flag"].isna() | (df["store_and_fwd_flag"].str.len() <= 1)

    for col in MONEY_COLUMNS:
        values = pd.to_numeric(df[col], errors="coerce")
        # NUMERIC(12,2): at most 10 digits before the decimal point.
        valid &= values.isna() | (values.abs() < 10_000_000_000)
        df[col] = values

    df = df.loc[valid.fillna(False)].copy()
    for col in INTEGER_COLUMNS:
        df[col] = df[col].astype("Int64")

    def to_decimal(value):
        if pd.isna(value):
            return None
        try:
            result = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
            return result if abs(result) < Decimal("10000000000") else None
        except (InvalidOperation, ValueError):
            return None

    for col in MONEY_COLUMNS:
        df[col] = df[col].map(to_decimal)
    return df


def load_parquet(limit):
    engine = get_engine()
    try:
        with engine.connect() as conn:
            print("Connected to PostgreSQL:", conn.execute(text("SELECT current_database()" )).scalar())
        download_if_missing()
        parquet_file = pq.ParquetFile(PARQUET_PATH)
        validate_schema(parquet_file, engine)

        loaded = 0
        rejected = 0
        with engine.connect() as conn:
            before = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar_one()
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=SOURCE_COLUMNS):
            source = batch.to_pandas()
            cleaned = clean_data(source)
            rejected += len(source) - len(cleaned)
            if limit is not None:
                cleaned = cleaned.head(limit - loaded)
            if cleaned.empty:
                continue
            with engine.begin() as conn:
                cleaned.to_sql(
                    TABLE_NAME, con=conn, if_exists="append", index=False,
                    method="multi", chunksize=100,
                )
            loaded += len(cleaned)
            print(f"Loaded {loaded} rows; rejected {rejected} invalid rows so far")
            if limit is not None and loaded >= limit:
                break
        with engine.connect() as conn:
            after = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar_one()
        print(f"Done. Inserted: {loaded}; table rows before: {before}; after: {after}")
        print("Note: rerunning appends rows again; this loader does not deduplicate.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NYC Taxi Parquet -> PostgreSQL")
    parser.add_argument("--limit", type=int, default=None,
                        help="Optional number of valid rows to load; default loads all")
    args = parser.parse_args()
    load_parquet(args.limit)