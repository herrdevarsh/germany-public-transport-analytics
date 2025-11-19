# Germany Public Transport Analytics – Berlin/Brandenburg (VBB)

## 1. Problem & context

Public transport reliability is a constant pain point in Germany. Passengers complain about delays, missed connections and confusing tickets, while operators and public authorities need to decide **where to invest limited resources**.

This project simulates the work of a **Data / BI Analyst** at a regional transport association (e.g. VBB – Berlin/Brandenburg):

> *“Given schedule data and delay events, which routes and stops should we fix first, and what impact could that have on passengers?”*

I use **real GTFS schedule data** from VBB and **synthetic but realistic delay events** to build:

- A clean **SQLite data model**
- Notebook-based **exploratory analysis** (routes, stops, delays, time of day)
- **Prioritization logic** for routes and stops (impact-based scoring)
- A simple **scenario analysis**: “What if we improve the worst routes?”

Everything is designed to be BI-ready and easy to port into tools like Power BI or Streamlit.

---

## 2. Data

### 2.1 GTFS schedule data (real)

- Source: VBB GTFS feed (Berlin/Brandenburg public transport), downloaded from the Berlin Open Data portal.
- Files used from `vbb_gtfs.zip`:
  - `stops.txt`
  - `routes.txt`
  - `trips.txt`
  - `stop_times.txt`
  - `calendar.txt` / `calendar_dates.txt`

These are loaded into a SQLite database at:

- `data/processed/vbb_gtfs.db`

### 2.2 Delay data (synthetic)

Real-time delay feeds (GTFS-RT / APIs) are not integrated here, so I generate a synthetic delay dataset on top of real scheduled trips:

- Script: `src/generate_synthetic_delays.py`
- Output: `data/raw/delays_sample.csv`
- Columns:
  - `service_date`
  - `trip_id`
  - `stop_id`
  - `route_id`
  - `planned_arrival`
  - `actual_arrival`
  - `delay_min` (can be negative for early departure)
  - `reason` (e.g. `vehicle_late`, `signal_failure`, `connection_wait`, `early_departure`)

This synthetic data is only for demonstration. The **logic** for KPIs, prioritization and scenarios is the same as with real delay data.

---

## 3. Project structure

```text
.
├─ data/
│  ├─ raw/
│  │  ├─ vbb_gtfs.zip              # GTFS feed (downloaded)
│  │  └─ delays_sample.csv         # synthetic delay events (generated)
│  └─ processed/
│     ├─ vbb_gtfs.db               # SQLite DB: GTFS + delays
│     ├─ route_schedule_kpis.csv   # schedule KPIs per route
│     ├─ stop_activity_kpis.csv    # activity KPIs per stop (arrivals)
│     ├─ route_delay_kpis.csv      # delay KPIs per route
│     ├─ hour_delay_kpis.csv       # delay KPIs per hour of day
│     ├─ prioritized_routes.csv    # routes ranked by impact / priority
│     ├─ stop_hotspot_kpis.csv     # stops ranked by hotspot score
│     ├─ scenario_routes_delay.csv # per-route baseline vs scenario delays
│     └─ scenario_summary.csv      # aggregate scenario impact
├─ notebooks/
│  ├─ 01_data_overview_gtfs.ipynb      # network structure & basic KPIs
│  ├─ 02_punctuality_and_delays.ipynb  # delay distribution & route/hour KPIs
│  ├─ 03_route_prioritization.ipynb    # composite priority score per route
│  ├─ 04_stop_hotspots.ipynb           # hotspot analysis for stops
│  └─ 05_scenario_impact.ipynb         # "what if we improve top routes?"
├─ src/
│  ├─ gtfs_processing.py           # load GTFS into SQLite, core KPIs, delays
│  ├─ generate_synthetic_delays.py # generate realistic synthetic delay events
│  └─ build_kpi_tables.py          # export BI-ready KPI tables
├─ sql/
│  └─ (optional) extra queries
├─ reports/
│  ├─ management_summary.pdf       # high-level summary for non-technical readers
│  └─ screenshots_dashboard/       # (optional) BI / notebook chart screenshots
├─ requirements.txt
└─ README.md


# Management Summary – Public Transport Reliability (Berlin/Brandenburg Synthetic Case)

**Role:** Data / BI Analyst (simulated)  
**Scope:** Berlin/Brandenburg public transport network (VBB GTFS + synthetic delay events)
## 1. Problem

Passengers experience delays, missed connections and crowded stops. The transport association needs to know:

- Which routes and stops cause the most pain?
- Where should we focus improvement efforts first?
- What would be the impact if we actually improved punctuality on those routes?

## 2. Data & method (one slide worth)

- Real GTFS schedule data (VBB – Berlin/Brandenburg)
- Synthetic delay events generated per trip and stop (realistic distributions)
- KPIs at three levels:
  - Route (prioritization score)
  - Stop (hotspot score)
  - System-wide (baseline vs scenario total delay)
## 3. Key findings (synthetic dataset)

- A small subset of routes concentrates a large share of the delay impact.
- A small set of high-traffic stops appear as **hotspots** combining:
  - high number of arrivals and
  - non-trivial average delays.
- In the current synthetic scenario, improving punctuality on the top N priority routes leads to a measurable reduction in total delay minutes at system level.
## 4. Scenario: improving top N routes

In a simple scenario, I reduce the average delay on the top N priority routes by 50%.

- Baseline total delay minutes (synthetic): 4.0
- Scenario total delay minutes: 2.0
- Reduction: 2.0 minutes (0.5% reduction)

This shows how targeted interventions on a small subset of routes can already generate a visible improvement in total passenger delay.
