import zipfile
import sqlite3
from pathlib import Path

import pandas as pd


# ---------- Paths & constants ----------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DB_PATH = DATA_PROCESSED / "vbb_gtfs.db"


# ---------- Helpers ----------

def ensure_processed_dir():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)


def load_csv_from_zip(zip_path: Path, filename: str) -> pd.DataFrame:
    """
    Load a GTFS CSV file from the ZIP into a pandas DataFrame.
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(filename) as f:
            df = pd.read_csv(f)
    return df


# ---------- GTFS → SQLite ----------

def load_core_gtfs_to_sqlite(zip_path: Path, db_path: Path):
    """
    Load core GTFS tables (stops, routes, trips, stop_times, calendar, calendar_dates)
    into a SQLite DB.
    """
    ensure_processed_dir()

    conn = sqlite3.connect(db_path)
    try:
        # stops.txt
        stops = load_csv_from_zip(zip_path, "stops.txt")
        stops.to_sql("stops", conn, if_exists="replace", index=False)

        # routes.txt
        routes = load_csv_from_zip(zip_path, "routes.txt")
        routes.to_sql("routes", conn, if_exists="replace", index=False)

        # trips.txt
        trips = load_csv_from_zip(zip_path, "trips.txt")
        trips.to_sql("trips", conn, if_exists="replace", index=False)

        # stop_times.txt
        stop_times = load_csv_from_zip(zip_path, "stop_times.txt")
        stop_times.to_sql("stop_times", conn, if_exists="replace", index=False)

        # calendar.txt (optional)
        try:
            calendar = load_csv_from_zip(zip_path, "calendar.txt")
            calendar.to_sql("calendar", conn, if_exists="replace", index=False)
        except KeyError:
            print("calendar.txt not found in GTFS zip. Skipping.")

        # calendar_dates.txt (optional)
        try:
            calendar_dates = load_csv_from_zip(zip_path, "calendar_dates.txt")
            calendar_dates.to_sql("calendar_dates", conn, if_exists="replace", index=False)
        except KeyError:
            print("calendar_dates.txt not found in GTFS zip. Skipping.")

        print("Core GTFS tables loaded into SQLite.")
    finally:
        conn.close()


# ---------- Basic GTFS KPIs ----------

def basic_kpis(db_path: Path) -> pd.DataFrame:
    """
    Calculate basic schedule-based KPIs:
      - number of routes
      - number of trips
      - number of stops
    """
    conn = sqlite3.connect(db_path)
    try:
        n_routes = pd.read_sql_query(
            "SELECT COUNT(*) AS n_routes FROM routes;", conn
        ).iloc[0, 0]
        n_trips = pd.read_sql_query(
            "SELECT COUNT(*) AS n_trips FROM trips;", conn
        ).iloc[0, 0]
        n_stops = pd.read_sql_query(
            "SELECT COUNT(*) AS n_stops FROM stops;", conn
        ).iloc[0, 0]

        kpis = pd.DataFrame(
            {
                "metric": ["routes", "trips", "stops"],
                "value": [n_routes, n_trips, n_stops],
            }
        )
        return kpis
    finally:
        conn.close()


def compute_headway_stats(db_path: Path, max_rows: int = 100000) -> pd.DataFrame:
    """
    Compute simple headway stats (time between consecutive trips)
    from stop_times for a subset of records (to avoid memory issues).

    Returns a DataFrame with headway metrics per route_id.
    """
    conn = sqlite3.connect(db_path)
    try:
        query = """
        SELECT
            st.stop_id,
            st.trip_id,
            st.arrival_time,
            t.route_id
        FROM stop_times st
        JOIN trips t ON st.trip_id = t.trip_id
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(max_rows,))

        # Convert arrival_time (HH:MM:SS, can exceed 24h) to seconds since "day start"
        def time_to_seconds(t):
            h, m, s = map(int, str(t).split(":"))
            return h * 3600 + m * 60 + s

        df["arrival_sec"] = df["arrival_time"].apply(time_to_seconds)

        # Sort by route, stop, time
        df = df.sort_values(["route_id", "stop_id", "arrival_sec"])

        # Headways in minutes
        df["headway_min"] = df.groupby(["route_id", "stop_id"])["arrival_sec"].diff() / 60.0

        df = df.dropna(subset=["headway_min"])

        headway_stats = (
            df.groupby("route_id")["headway_min"]
            .agg(["count", "mean", "median", "min", "max"])
            .reset_index()
            .rename(
                columns={
                    "count": "n_headways",
                    "mean": "avg_headway_min",
                    "median": "median_headway_min",
                    "min": "min_headway_min",
                    "max": "max_headway_min",
                }
            )
        )

        return headway_stats
    finally:
        conn.close()


def run_sql_query(db_path: Path, sql: str) -> pd.DataFrame:
    """
    Utility to run an arbitrary SQL query against the SQLite DB
    and return a DataFrame.
    """
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


# ---------- Delays (synthetic) ----------

def load_delays_to_sqlite(csv_path: Path, db_path: Path):
    """
    Load a synthetic delays CSV into SQLite as table 'delays'.

    Expected columns:
      service_date, trip_id, stop_id, planned_arrival,
      actual_arrival, delay_min, reason
    """
    ensure_processed_dir()
    df = pd.read_csv(csv_path, parse_dates=["service_date"])
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql("delays", conn, if_exists="replace", index=False)
        print("Delays table loaded into SQLite.")
    finally:
        conn.close()


def delay_kpis(db_path: Path) -> pd.DataFrame:
    """
    Compute simple delay KPIs from the 'delays' table:
      - number of events
      - average delay (min)
      - share of events with delay > 5 min
      - share of on-time or early arrivals (delay <= 0)
    """
    conn = sqlite3.connect(db_path)
    try:
        sql = """
        SELECT
            COUNT(*) AS n_events,
            AVG(delay_min) AS avg_delay_min,
            SUM(CASE WHEN delay_min > 5 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_over_5min,
            SUM(CASE WHEN delay_min <= 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_on_time_or_early
        FROM delays;
        """
        df = pd.read_sql_query(sql, conn)
        return df
    finally:
        conn.close()


# ---------- Main ----------

def main():
    zip_path = DATA_RAW / "vbb_gtfs.zip"
    if not zip_path.exists():
        raise FileNotFoundError(f"GTFS zip not found at {zip_path}")

    print(f"Loading GTFS from {zip_path} into SQLite at {DB_PATH} ...")
    load_core_gtfs_to_sqlite(zip_path, DB_PATH)

    print("Calculating basic KPIs...")
    kpis = basic_kpis(DB_PATH)
    print(kpis)

    print("Calculating headway stats (subset of stop_times)...")
    headway_stats = compute_headway_stats(DB_PATH, max_rows=200000)
    print(headway_stats.head(10))

    # Load delays if synthetic CSV exists
    delays_csv = DATA_RAW / "delays_sample.csv"
    if delays_csv.exists():
        print(f"Loading delays from {delays_csv} ...")
        load_delays_to_sqlite(delays_csv, DB_PATH)

        print("Calculating delay KPIs...")
        dkpis = delay_kpis(DB_PATH)
        print(dkpis)
    else:
        print(f"No delays CSV found at {delays_csv}, skipping delay KPIs.")


if __name__ == "__main__":
    main()

