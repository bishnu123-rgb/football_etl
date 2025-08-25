import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, lit, count, sum as _sum, avg
)
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utility.utility import setup_logger

logger = setup_logger("transform")

# Paths
input_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "extracted_data")
output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "parquet_output")
os.makedirs(output_path, exist_ok=True)

# Spark
spark = SparkSession.builder \
    .appName("FootballETL_Transform") \
    .getOrCreate()

start_time = time.time()
logger.info("Starting transformation...")

# ---------------------------
# Load CSVs
# ---------------------------
matches_csv = os.path.join(input_path, "matches.csv")
teams_csv = os.path.join(input_path, "teams.csv")

matches_df = spark.read.option("header", True).csv(matches_csv)
teams_df = spark.read.option("header", True).csv(teams_csv)

logger.info(f"Matches loaded: {matches_df.count()} rows")
logger.info(f"Teams loaded: {teams_df.count()} rows")

# ---------------------------
# Clean column names
# ---------------------------
for col_name in matches_df.columns:
    matches_df = matches_df.withColumnRenamed(col_name, col_name.strip().lower().replace(" ", "_"))
for col_name in teams_df.columns:
    teams_df = teams_df.withColumnRenamed(col_name, col_name.strip().lower().replace(" ", "_"))

# ---------------------------
# Convert date column
# ---------------------------
matches_df = matches_df.withColumn("date", to_date(col("date")))

# ---------------------------
# Drop null IDs
# ---------------------------
matches_df = matches_df.dropna(subset=["match_api_id", "home_team_api_id", "away_team_api_id"])

# ---------------------------
# Join teams
# ---------------------------
teams_df_home = teams_df.select(
    col("team_api_id").alias("home_team_api_id"),
    col("team_long_name").alias("home_team_name")
)

teams_df_away = teams_df.select(
    col("team_api_id").alias("away_team_api_id"),
    col("team_long_name").alias("away_team_name")
)

matches_teams_df = matches_df \
    .join(teams_df_home, "home_team_api_id", "left") \
    .join(teams_df_away, "away_team_api_id", "left")

logger.info(f"Final transformed dataset: {matches_teams_df.count()} rows")

# ---------------------------
# Save cleaned matches
# ---------------------------
matches_output = os.path.join(output_path, "matches_cleaned.parquet")
matches_teams_df.write.mode("overwrite").parquet(matches_output)
logger.info(f"Saved cleaned matches to {matches_output}")

# ==================================================
# AGGREGATIONS
# ==================================================

# ---------------------------
# League Standings
# ---------------------------
league_df = matches_teams_df.withColumn(
    "home_win", when(col("home_team_goal") > col("away_team_goal"), lit(1)).otherwise(lit(0))
).withColumn(
    "away_win", when(col("away_team_goal") > col("home_team_goal"), lit(1)).otherwise(lit(0))
).withColumn(
    "draw", when(col("home_team_goal") == col("away_team_goal"), lit(1)).otherwise(lit(0))
)

# Home side stats
home_stats = league_df.groupBy("season", "league_id", "home_team_api_id").agg(
    count("*").alias("matches_played"),
    _sum("home_win").alias("wins"),
    _sum("draw").alias("draws"),
    _sum("away_win").alias("losses"),
    _sum("home_team_goal").alias("goals_for"),
    _sum("away_team_goal").alias("goals_against")
).withColumnRenamed("home_team_api_id", "team_api_id")

# Away side stats
away_stats = league_df.groupBy("season", "league_id", "away_team_api_id").agg(
    count("*").alias("matches_played"),
    _sum("away_win").alias("wins"),
    _sum("draw").alias("draws"),
    _sum("home_win").alias("losses"),
    _sum("away_team_goal").alias("goals_for"),
    _sum("home_team_goal").alias("goals_against")
).withColumnRenamed("away_team_api_id", "team_api_id")

# Combine home + away
standings_df = home_stats.unionByName(away_stats) \
    .groupBy("season", "league_id", "team_api_id") \
    .agg(
        _sum("matches_played").alias("matches_played"),
        _sum("wins").alias("wins"),
        _sum("draws").alias("draws"),
        _sum("losses").alias("losses"),
        _sum("goals_for").alias("goals_for"),
        _sum("goals_against").alias("goals_against")
    ).withColumn(
        "points", col("wins") * 3 + col("draws") * 1
    )

# Save league standings
standings_output = os.path.join(output_path, "league_standings.parquet")
standings_df.write.mode("overwrite").parquet(standings_output)
logger.info(f"Saved league standings to {standings_output}")

# ---------------------------
# Team Stats
# ---------------------------
team_stats_df = matches_teams_df.select(
    "season",
    "home_team_api_id", "away_team_api_id",
    "home_team_goal", "away_team_goal"
)

# Home team perspective
home_team_stats = team_stats_df.groupBy("season", "home_team_api_id").agg(
    avg("home_team_goal").alias("avg_goals_scored"),
    avg("away_team_goal").alias("avg_goals_conceded")
).withColumnRenamed("home_team_api_id", "team_api_id")

# Away team perspective
away_team_stats = team_stats_df.groupBy("season", "away_team_api_id").agg(
    avg("away_team_goal").alias("avg_goals_scored"),
    avg("home_team_goal").alias("avg_goals_conceded")
).withColumnRenamed("away_team_api_id", "team_api_id")

# Merge both
team_stats_final = home_team_stats.unionByName(away_team_stats) \
    .groupBy("season", "team_api_id") \
    .agg(
        avg("avg_goals_scored").alias("avg_goals_scored"),
        avg("avg_goals_conceded").alias("avg_goals_conceded")
    )

# Save team stats
team_stats_output = os.path.join(output_path, "team_stats.parquet")
team_stats_final.write.mode("overwrite").parquet(team_stats_output)
logger.info(f"Saved team stats to {team_stats_output}")

# ==================================================
# Done
# ==================================================
elapsed = time.time() - start_time
logger.info(f"Phase 1 Transformation completed in {int(elapsed)} seconds")
spark.stop()
