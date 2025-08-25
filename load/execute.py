import os
import sys
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utility.utility import setup_logger

logger = setup_logger("load_phase1")

# ---------------------------
# DB Config
# ---------------------------
DB_NAME = "football"   
DB_USER = "bishnu"
DB_PASS = "bishnu"   
DB_HOST = "localhost"
DB_PORT = "5432"

# ---------------------------
# File path (parquet output)
# ---------------------------
output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "parquet_output")
matches_parquet = os.path.join(output_path, "matches_teams.parquet")

# ---------------------------
# Load data
# ---------------------------
def load_matches(conn):
    if not os.path.exists(matches_parquet):
        logger.error(f"Parquet file not found: {matches_parquet}")
        return
    
    df = pd.read_parquet(matches_parquet)
    logger.info(f"Loaded {len(df)} rows from parquet")

   
    required_cols = ["date", "season", "home_team_long_name", "away_team_long_name", "home_team_goal", "away_team_goal"]
    df = df[required_cols].where(pd.notnull(df), None)

    values = [tuple(x) for x in df.to_numpy()]
    cols = list(df.columns)

    with conn.cursor() as cur:
        query = f"INSERT INTO matches_teams ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING"
        execute_values(cur, query, values, page_size=1000)
        conn.commit()
        logger.info(f"Inserted {len(df)} rows into matches_teams")

# ---------------------------
# Main
# ---------------------------
def main():
    start_time = time.time()
    logger.info("Starting Phase 1 PostgreSQL load...")

    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

    load_matches(conn)
    conn.close()

    elapsed = time.time() - start_time
    logger.info(f"Phase 1 PostgreSQL load completed in {int(elapsed)} seconds")

if __name__ == "__main__":
    main()
