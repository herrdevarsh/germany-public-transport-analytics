from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DB_PATH = DATA_PROCESSED / "vbb_gtfs.db"
OUTPUT_CSV = DATA_RAW / "delays_sample.csv"


def get_connection():
    return sqlite3.connect(DB_PATH)


def time_to_seconds(t: str) -> int:
    """
    Convert GTFS HH:MM:SS (may exceed 24h) to seconds.
    """
    h, m, s = map(int, str(t).split(":"))
    return h * 3600 + m * 60 + s


def generate_delays(n_samples: int = 20000, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic delay events from GTFS stop_times + trips + routes.

    We:
      - sample stop_times joined with trips and routes
      - treat 'arrival_time' as planned_arrival
      - create delay_min from a skewed distribution:
            ~60% near 0,
            ~25% small positive (1-5 min),
            ~10% larger positive (5-20 min),
            ~5% early departures (-1 to -5 min)
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found at {DB_PATH}. Run gtfs_processing.py first.")

    conn = get_connection()
    try:
        # Total rows in stop_times to know the scale
        total_rows = pd.read_sql_query("SELECT COUNT(*) AS c FROM stop_times;", conn)["c"][0]
        n_samples = min(n_samples, total_rows)

        # Sample rows
        # NOTE: SQLite's ORDER BY RANDOM() is slow on very big tables, but this is acceptable for a one-off script.
        query = f"""
        SELECT
            st.trip_id,
            st.stop_id,
            st.arrival_time,
            t.route_id
        FROM stop_times st
        JOIN trips t ON st.trip_id = t.trip_id
        ORDER BY RANDOM()
        LIMIT {int(n_samples)};
        """
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    if df.empty:
        raise RuntimeError("No stop_times data returned from database.")

    # Add deterministic "service_date" for now – could be randomized if you want
    df["service_date"] = pd.to_datetime("2025-11-01")

    # Planned arrival as string
    df["planned_arrival"] = df["arrival_time"]

    # Generate delays
    rng = np.random.default_rng(seed)
    n = len(df)

    # Categories of delay
    categories = rng.choice(
        ["on_time", "small_delay", "big_delay", "early"],
        size=n,
        p=[0.6, 0.25, 0.1, 0.05],
    )

    delays = np.zeros(n)

    # on_time: around 0 min
    mask_on_time = categories == "on_time"
    delays[mask_on_time] = rng.normal(loc=0.0, scale=1.0, size=mask_on_time.sum())
    # clamp near [-2, 3]
    delays[mask_on_time] = np.clip(delays[mask_on_time], -2, 3)

    # small_delay: 1–5 min
    mask_small = categories == "small_delay"
    delays[mask_small] = rng.integers(1, 6, size=mask_small.sum())

    # big_delay: 5–20 min
    mask_big = categories == "big_delay"
    delays[mask_big] = rng.integers(5, 21, size=mask_big.sum())

    # early: -1 to -5 min
    mask_early = categories == "early"
    delays[mask_early] = -rng.integers(1, 6, size=mask_early.sum())

    df["delay_min"] = delays.round(0).astype(int)

    # Reason codes (simple mapping)
    reason_choices = {
        "on_time": ["on_time"],
        "small_delay": ["vehicle_late", "connection_wait"],
        "big_delay": ["signal_failure", "infrastructure_issue", "congestion"],
        "early": ["early_departure"],
    }

    reasons = []
    for cat in categories:
        reasons.append(rng.choice(reason_choices[cat]))
    df["reason"] = reasons

    # Compute actual_arrival by shifting planned_arrival by delay
    def add_delay_to_time(t_str: str, delay_minutes: int) -> str:
        sec = time_to_seconds(t_str)
        sec_actual = sec + delay_minutes * 60
        if sec_actual < 0:
            sec_actual = 0
        h = sec_actual // 3600
        m = (sec_actual % 3600) // 60
        s = sec_actual % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    df["actual_arrival"] = [
        add_delay_to_time(t, d) for t, d in zip(df["planned_arrival"], df["delay_min"])
    ]

    # Keep relevant columns and sort
    df_out = df[
        [
            "service_date",
            "trip_id",
            "stop_id",
            "route_id",
            "planned_arrival",
            "actual_arrival",
            "delay_min",
            "reason",
        ]
    ].copy()

    df_out.sort_values(["service_date", "route_id", "trip_id", "stop_id"], inplace=True)

    return df_out


def main():
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    print(f"Generating synthetic delays from {DB_PATH}...")
    df_delays = generate_delays(n_samples=20000, seed=42)
    df_delays.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df_delays)} delay events to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
