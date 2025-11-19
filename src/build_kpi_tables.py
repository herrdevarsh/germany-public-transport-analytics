from pathlib import Path
import sqlite3

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DB_PATH = DATA_PROCESSED / "vbb_gtfs.db"


def ensure_processed_dir():
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)


def get_connection():
    return sqlite3.connect(DB_PATH)


def build_route_schedule_kpis() -> pd.DataFrame:
    """
    Build route-level schedule KPIs:
      - number of trips
      - (optionally) merge with headway stats if available
    """
    conn = get_connection()
    try:
        sql = """
        SELECT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            r.route_type,
            COUNT(DISTINCT t.trip_id) AS n_trips
        FROM routes r
        JOIN trips t ON r.route_id = t.route_id
        GROUP BY
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            r.route_type
        """
        df_routes = pd.read_sql_query(sql, conn)
    finally:
        conn.close()

    # If you want, you can recompute headways here or load precomputed headways.
    # For now, keep it simple: just trips per route.
    return df_routes


def build_stop_activity_kpis(top_n: int = 5000) -> pd.DataFrame:
    """
    Build stop-level activity KPIs:
      - number of scheduled arrivals
    Limit to top_n to keep things manageable.
    """
    conn = get_connection()
    try:
        sql = f"""
        SELECT
            s.stop_id,
            s.stop_name,
            s.stop_lat,
            s.stop_lon,
            COUNT(*) AS n_arrivals
        FROM stop_times st
        JOIN stops s ON st.stop_id = s.stop_id
        GROUP BY
            s.stop_id,
            s.stop_name,
            s.stop_lat,
            s.stop_lon
        ORDER BY n_arrivals DESC
        LIMIT {int(top_n)};
        """
        df_stops = pd.read_sql_query(sql, conn)
    finally:
        conn.close()

    return df_stops


def build_delay_route_kpis() -> pd.DataFrame:
    """
    Build route-level delay KPIs from synthetic/real 'delays' table:
      - avg delay
      - share delayed > 5 min
      - share on-time or early
    Joined to route metadata via trips.
    """
    conn = get_connection()
    try:
        # Check if delays table exists
        exists = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='delays';", conn
        )
        if exists.empty:
            print("No 'delays' table found. Returning empty DataFrame.")
            return pd.DataFrame()

        sql = """
        SELECT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            COUNT(*) AS n_delay_events,
            AVG(d.delay_min) AS avg_delay_min,
            SUM(CASE WHEN d.delay_min > 5 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_over_5min,
            SUM(CASE WHEN d.delay_min <= 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS share_on_time_or_early
        FROM delays d
        JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        GROUP BY
            r.route_id,
            r.route_short_name,
            r.route_long_name
        """
        df_delay_route = pd.read_sql_query(sql, conn)
    finally:
        conn.close()

    return df_delay_route


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"SQLite DB not found at {DB_PATH}. Run gtfs_processing.py first.")

    ensure_processed_dir()

    print("Building route schedule KPIs...")
    df_routes = build_route_schedule_kpis()
    routes_csv = DATA_PROCESSED / "route_schedule_kpis.csv"
    df_routes.to_csv(routes_csv, index=False)
    print(f"Saved {len(df_routes)} route records to {routes_csv}")

    print("Building stop activity KPIs...")
    df_stops = build_stop_activity_kpis(top_n=5000)
    stops_csv = DATA_PROCESSED / "stop_activity_kpis.csv"
    df_stops.to_csv(stops_csv, index=False)
    print(f"Saved {len(df_stops)} stop records to {stops_csv}")

    print("Building delay KPIs per route...")
    df_delay_route = build_delay_route_kpis()
    if not df_delay_route.empty:
        delay_route_csv = DATA_PROCESSED / "delay_kpis_route.csv"
        df_delay_route.to_csv(delay_route_csv, index=False)
        print(f"Saved {len(df_delay_route)} route delay KPI records to {delay_route_csv}")
    else:
        print("No delay KPIs generated (no 'delays' table).")

    print("Done.")


if __name__ == "__main__":
    main()
