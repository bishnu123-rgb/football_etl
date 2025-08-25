import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import zipfile
import time
import sqlite3
import pandas as pd
from utility.utility import setup_logger

# ---------------------------
# Paths & Logger
# ---------------------------
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "extracted_data")
logger = setup_logger("extract")

# ---------------------------
# Functions
# ---------------------------
def extract_from_zip():
    """Extract soccer.zip into OUTPUT_DIR."""
    zip_path = os.path.join(OUTPUT_DIR, "soccer.zip")
    if not os.path.exists(zip_path):
        logger.error(f"{zip_path} not found!")
        return False
    logger.info(f"Extracting {zip_path}...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(OUTPUT_DIR)
    logger.info("Extraction complete.")
    return True

def create_csv_from_sqlite():
    """Read database.sqlite and export matches.csv and teams.csv."""
    db_path = os.path.join(OUTPUT_DIR, "database.sqlite")
    if not os.path.exists(db_path):
        logger.error("database.sqlite not found! Cannot create CSVs.")
        return False
    logger.info("Reading database.sqlite...")
    conn = sqlite3.connect(db_path)

    matches = pd.read_sql("SELECT * FROM Match", conn)
    matches_path = os.path.join(OUTPUT_DIR, "matches.csv")
    matches.to_csv(matches_path, index=False)
    logger.info(f"matches.csv created! ({len(matches)} rows)")

    teams = pd.read_sql("SELECT * FROM Team", conn)
    teams_path = os.path.join(OUTPUT_DIR, "teams.csv")
    teams.to_csv(teams_path, index=False)
    logger.info(f"teams.csv created! ({len(teams)} rows)")

    conn.close()

   
    for f in [matches_path, teams_path]:
        if not os.path.exists(f):
            logger.error(f"CSV missing: {f}")
            return False
    return True

# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    start_time = time.time()

    if extract_from_zip():
        if create_csv_from_sqlite():
            logger.info("All CSVs verified successfully")

    elapsed = time.time() - start_time
    logger.info(f"Extraction finished in {int(elapsed)} seconds")
