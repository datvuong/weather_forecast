# User Guide — Configure and Run the Weather Pipeline

This guide takes you from a fresh checkout to a running pipeline you can test locally. It covers:

1. [Prerequisites](#1-prerequisites)
2. [Install](#2-install)
3. [Configure](#3-configure): cities, weather variables, API settings
4. [Start Dagster](#4-start-dagster)
5. [First run](#5-first-run): today's data, then history
6. [Turn on the schedules](#6-turn-on-the-schedules)
7. [Check the results](#7-check-the-results)
8. [Troubleshooting](#8-troubleshooting)
9. [Stop and reset](#9-stop-and-reset)

**What the pipeline does:** it fetches weather data from the free [Open-Meteo API](https://open-meteo.com/) for the cities you choose and stores it in three layers:

| Layer | Contents | Where |
|---|---|---|
| **Bronze** | Raw API responses, unchanged | `data/bronze/` |
| **Silver** | Clean tables, one column per configured variable: hourly, daily and current-conditions history | `data/silver/hourly/`, `daily/`, `current/` |
| **Gold** | Daily temperature and precipitation summaries, plus the latest current conditions | `data/gold/` and the DuckDB database `warehouse/weather.duckdb` |

Each layer is split into **partitions**, one per day (a UTC calendar day). Dagster (the orchestrator) runs the steps, checks data quality and shows everything in a web UI.

---

## 1. Prerequisites

| You need | Check with |
|---|---|
| Python **3.10+** | `python3 --version` |
| `git` | `git --version` |
| Internet access to `api.open-meteo.com` and `archive-api.open-meteo.com` | `curl -s "https://api.open-meteo.com/v1/forecast?latitude=0&longitude=0&current=temperature_2m"` |
| A free port **3000** for the web UI | `ss -ltn \| grep :3000` (no output means it's free) |

No API key or account is required.

> **Windows:** run everything inside WSL2 (Ubuntu). The UI opens in your Windows browser at http://localhost:3000.

## 2. Install

```bash
git clone https://github.com/datvuong/weather_forecast.git
cd weather_forecast

python3 -m venv .venv
source .venv/bin/activate          # run this in every new terminal
pip install -e ".[dev]"
```

Check the installation; both commands should succeed:

```bash
dagster definitions validate       # ends with "All code locations passed validation."
pytest -q                          # optional: runs the offline test suite
```

All commands below assume you are in the project folder with the virtual environment active.

## 3. Configure

All configuration lives in two YAML files in `config/`. Neither needs code changes. Edit them, save, and [validate](#34-validate-your-configuration).

### 3.1 Cities — `config/locations.yaml`

```yaml
locations:
  - name: Melbourne                 # used in file names and tables, so keep it unique
    latitude: -37.8136              # WGS84, -90..90
    longitude: 144.9631             # WGS84, -180..180
    timezone: Australia/Melbourne   # IANA timezone name

  - name: Sydney                    # example: add a city
    latitude: -33.8688
    longitude: 151.2093
    timezone: Australia/Sydney
```

- Look up coordinates at https://www.latlong.net/ and the timezone name in the [tz database list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).
- The timezone decides what counts as a "local day" for daily values and the local timestamps in Silver.

### 3.2 Weather variables — `config/variables.yaml`

Variables are grouped into three sections, matching Open-Meteo's request types:

| Section | What you get | Notes |
|---|---|---|
| `hourly_variables` | One value per hour | Backfillable from the archive |
| `daily_variables` | One value per city per **local** calendar day (e.g. max temperature, sunrise) | Backfillable from the archive |
| `current_variables` | A "current conditions" snapshot every 15 minutes | Can't be fetched for the past (see [5.3](#53-current-weather)) |

Each entry looks like this:

```yaml
hourly_variables:
  - name: cloud_cover        # required: the Open-Meteo parameter name
    description: "Total cloud cover"
    unit: "%"                # documentation only, except "iso8601"/"unixtime" (see below)
    valid_range: [0, 100]    # optional: the quality check fails outside this range
```

To **add a variable**, find its exact name in the [Open-Meteo docs](https://open-meteo.com/en/docs) under the right section (hourly, daily or current) and add an entry. On the next run it appears as a new column in the matching Silver table.

Rules worth knowing:
- **Names must be valid for their section.** For example, `temperature_2m_max` is daily-only and `temperature_2m` is hourly/current. A wrong name makes the run fail with Open-Meteo's error message.
- **Some variables aren't available for historical days.** `precipitation_probability`, `uv_index`, `precipitation_probability_max` and `uv_index_max` are forecast-only. Past days get empty (null) values for them, which is expected, and the run's metadata lists them.
- **Time-valued variables** such as `sunrise`/`sunset` need `unit: "iso8601"`. They're stored as UTC timestamps.
- **The same name can appear in different sections** (e.g. `temperature_2m` in hourly and current) but not twice in one section. Names such as `time`, `location_name` or `timestamp_utc` are reserved.
- **Gold needs these hourly variables:** `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, `uv_index`, `precipitation` and `precipitation_probability`. If you remove any of them, the pipeline refuses to start and tells you which one is missing.

### 3.3 API settings — `settings:` in `config/variables.yaml`

| Setting | Default | Allowed | Effect |
|---|---|---|---|
| `temperature_unit` | `celsius` | `celsius`, `fahrenheit` | Gold requires `celsius` |
| `wind_speed_unit` | `kmh` | `kmh`, `ms`, `mph`, `kn` | Gold requires `kmh` |
| `precipitation_unit` | `mm` | `mm`, `inch` | Gold requires `mm` |
| `forecast_days` | `7` | 1–16 | How far ahead today's run fetches (kept in Bronze) |
| `backfill_days` | `30` | 1–365 | How many past days are available as partitions |
| `current_interval_minutes` | `15` | `15`, `30`, `60` | How often current weather is polled |
| `extra_params` | `{}` | any other Open-Meteo parameter | e.g. `{models: best_match, cell_selection: land}` |

The pipeline sets some parameters itself (`timezone`, `timeformat`, `start_date`, `hourly`, …), and `extra_params` can't override them.

**API budget.** The free tier allows 10,000 calls per day. Each city uses 24 calls a day for the hourly pipeline, plus one per current-weather poll (96 a day at 15 minutes). That's about 120 calls per city per day, so roughly 80 cities fit. If you go above 80% of the limit, Dagster logs a warning when it loads; reduce cities or raise `current_interval_minutes`.

### 3.4 Validate your configuration

Run this after every config change:

```bash
dagster definitions validate
```

Success ends with `All code locations passed validation.` Typical errors:

| Message contains | Meaning | Fix |
|---|---|---|
| `Input should be 'celsius' or 'fahrenheit'` | A setting has an unsupported value | Use one of the allowed values in [3.3](#33-api-settings--settings-in-configvariablesyaml) |
| `doesn't meet Gold layer requirements: … needs hourly variables ['temperature_2m']` | A variable Gold needs was removed | Add it back to `hourly_variables` |
| `Gold column names assume temperature_unit='celsius'` | Non-metric units | Switch back to metric units |
| `duplicate daily variables` / `is reserved` | Name clash | Rename or remove the entry |
| `extra_params cannot set pipeline-controlled parameters` | e.g. `timezone` in `extra_params` | Remove it; the pipeline sets it |

> Validation checks the file's structure, not whether Open-Meteo knows a variable name. A misspelt name shows up on the first run (see [Troubleshooting](#8-troubleshooting)).

### 3.5 Optional: relocate data or config

| Environment variable | Default | Use |
|---|---|---|
| `WEATHER_FORECAST_DATA_DIR` | `data/` in the project | Store the data lake elsewhere, e.g. a bigger disk |
| `WEATHER_FORECAST_CONFIG_DIR` | `config/` in the project | Use another folder with `locations.yaml` + `variables.yaml` |

Export them before starting Dagster, e.g. `export WEATHER_FORECAST_DATA_DIR=/mnt/weather-data`. The DuckDB database always lives in `warehouse/`.

## 4. Start Dagster

From the project folder (virtual environment active):

```bash
dagster dev
```

Wait for a line like `Serving dagster-webserver on http://127.0.0.1:3000`, then open **http://localhost:3000**.

- Keep this terminal open: closing it stops Dagster, including the schedules.
- Run history and schedule on/off state are kept in `.dagster_home/` (configured in `dagster.yaml`), so they survive restarts.
- The startup message *"To persist information across sessions, set the environment variable DAGSTER_HOME"* is expected. It refers only to per-step stdout/stderr logs, which go to a temporary `.tmp_dagster_home_*` folder that's removed when Dagster stops.
- To use another port: `dagster dev -p 3001`. If your browser can't reach WSL on localhost: `dagster dev -h 0.0.0.0`.

### Finding your way around the UI

> UI steps in this guide were checked against **Dagster 1.13.26** (`dagster --version`). Other versions move menus around. If yours differs, the names below should still be searchable.

The left sidebar has these pages (each has an `Alt`/`⌥` + letter shortcut):

| Page | Shortcut | What you'll find there |
|---|---|---|
| **Overview** | ⌥O | Activity timeline of recent runs |
| **Runs** | ⌥R | All runs; the **Backfills** tab lists backfills |
| **Catalog** | ⌥C | The 8 assets: `bronze_forecast`, `silver_hourly_weather`, `silver_daily_weather`, `gold_daily_temperature`, `gold_daily_precipitation`, `bronze_current`, `silver_current_weather`, `gold_current_conditions` |
| **Jobs** | ⌥J | `weather_pipeline_job` (hourly + daily data) and `current_weather_job` (current conditions) |
| **Automation** | ⌥A | **Schedules and sensors**: `hourly_weather_schedule` and `current_weather_schedule`, both off by default |
| **Lineage** | ⌥L | The asset graph (Bronze → Silver → Gold) |
| **Deployment** | ⌥D | **Code locations** (reload code/config here) and daemon health |

To jump to anything by name, press **`/`** (or **⌘K** on macOS) and type, e.g. `weather_pipeline_job`.

## 5. First run

### 5.1 Today's data

**In the UI:**
1. Go to **Jobs** (⌥J) → **`weather_pipeline_job`**. The **Overview** tab shows the job's asset graph.
2. Click **Materialize all…** in the toolbar above the graph. A dialog opens.
3. Under **Partition selection**, click **Latest**. That's today's partition (UTC), the last one in the range.
4. Click **Launch 1 run**, then open the run from the confirmation (or via **Runs**, ⌥R).
5. The run page shows each step turning green, followed by the quality checks: `bronze_file_exists_check`, `silver_quality_check`, `silver_daily_check` and `gold_temperature_check`.

**Or from a second terminal** (virtual environment active):

```bash
dagster asset materialize -m weather_forecast.definitions \
  --select 'bronze_forecast*' --partition "$(date -u +%F)"
```

`bronze_forecast*` means "this asset and everything downstream of it". The command ends with `RUN_SUCCESS`.

> **Partitions are UTC days.** In eastern Australia the UTC date changes at 10:30–11:00 local time, so in the early morning today's partition is still dated yesterday.

### 5.2 History (backfill)

To load the last 30 days (`backfill_days`):

1. Go to **Jobs** → **`weather_pipeline_job`** → **Materialize all…**.
2. Under **Partition selection**, choose which days to load:
   - **All**: every day in the range.
   - **Missing**, or **Failed and missing**: only days not yet loaded (or that failed). Good for filling gaps later.
   - **Custom**: drag across the partition bar to pick a range.
3. Optionally tick **Backfill only failed and missing partitions within selection** under **Options**. If the dialog asks how to **Launch as…**, choose **Multiple runs (One per selected partition)**.
4. Click **Launch backfill**. Once several partitions are selected, the button changes from **Launch 1 run** to this.
5. Follow progress under **Runs → Backfills**; click the backfill to see each partition. Dagster runs up to 10 partitions at a time, so 30 days take a few minutes.

Past days come from Open-Meteo's archive. The last day or two, which the archive doesn't have yet, come from the forecast API automatically.

> Materializing a single asset from the **Catalog** (e.g. only `bronze_forecast`) doesn't update Silver or Gold. Use the job's **Materialize all…**. The current-weather assets aren't in this job, by design.

### 5.3 Current weather

Current conditions are the forecast model's value for the current 15-minute slot, not a weather-station reading. They **can only be captured for today**, as they happen.

- Run once now: **Jobs** → **`current_weather_job`** → **Materialize all…** → **Partition selection: Latest** → **Launch 1 run**, or

  ```bash
  dagster asset materialize -m weather_forecast.definitions \
    --select 'bronze_current*' --partition "$(date -u +%F)"
  ```

- Running it again within the same 15-minute slot overwrites that snapshot instead of duplicating it.
- Running it for a past date does nothing (the run succeeds and its metadata says why).

## 6. Turn on the schedules

Schedules start **off**. Go to **Automation** (⌥A) → **Schedules and sensors** and switch on the toggle next to each schedule:

| Schedule | Runs | What it does |
|---|---|---|
| `hourly_weather_schedule` | every hour at :00 UTC | Refreshes today's forecast through Bronze → Silver → Gold |
| `current_weather_schedule` | every 15 minutes (`current_interval_minutes`) | Adds a current-conditions snapshot and updates the latest-conditions table |

Clicking a schedule's name shows its upcoming and past ticks, and the runs each tick launched. Schedules only run while `dagster dev` is running. Snapshots missed while it's stopped can't be recovered later. The `gold_current_freshness_check` turns red if the latest snapshot is older than twice the interval, so a stopped scheduler shows up quickly.

## 7. Check the results

### In the UI

Open an asset from **Catalog** (⌥C), or click it in **Lineage** (⌥L). Its tabs:

| Tab | Shows |
|---|---|
| **Overview** | Latest materialization and its metadata: row counts, null percentages, the columns, any `unsupported_variables` |
| **Partitions** | One cell per day, coloured by status; click a day for its details |
| **Checks** | Quality-check results and history |
| **Events** | Every materialization with its metadata |
| **Lineage** | Upstream and downstream assets |

Gold assets also show a preview table.

### Files

```text
data/bronze/{city}/{date}.parquet                 raw hourly + daily responses
data/bronze/current/{city}/{date}/{HHMM}.parquet  raw current snapshots
data/silver/hourly/{date}.parquet                 one row per city per hour
data/silver/daily/{date}.parquet                  one row per city per local day
data/silver/current/{date}.parquet                all snapshots of the day
data/gold/daily_temperature/{date}.parquet
data/gold/daily_precipitation/{date}.parquet
data/gold/current_conditions/latest.parquet
warehouse/weather.duckdb                          Gold tables for SQL
```

### SQL (DuckDB)

DuckDB is installed with the project. Open the warehouse **read-only** so you don't block running jobs, and close it when done:

```bash
python - <<'EOF'
import duckdb
con = duckdb.connect("warehouse/weather.duckdb", read_only=True)
con.sql("SET TimeZone = 'UTC'")   # show timestamps in UTC instead of your machine's timezone
print(con.sql("SHOW TABLES"))
print(con.sql("""
    SELECT location_name, date, temp_min_c, temp_max_c, uv_max
    FROM gold_daily_temperature ORDER BY date DESC, location_name LIMIT 10
"""))
print(con.sql("SELECT location_name, observed_at_utc, temperature_2m, age_minutes FROM gold_current_conditions"))
con.close()
EOF
```

Silver files can be queried directly. `union_by_name` lets files from before and after a variable was added line up:

```bash
python -c "
import duckdb
print(duckdb.sql(\"\"\"
    SELECT location_name, count(*) AS hours, max(temperature_2m) AS t_max
    FROM read_parquet('data/silver/hourly/*.parquet', union_by_name=true)
    GROUP BY 1
\"\"\"))"
```

**Reading timestamps:**
- `timestamp_utc` / `observed_at_utc` are UTC; `timestamp_local` / `observed_at_local` are in the city's timezone.
- For half-hour-offset cities like Adelaide (UTC+10:30), hourly values fall at `:30` UTC, which is the top of the hour locally.
- Daily rows (`date_local`) cover the city's own calendar day.

## 8. Troubleshooting

| Symptom | Cause | What to do |
|---|---|---|
| UI shows a red code-location error / `dagster definitions validate` fails | Config problem | Read the message; see [3.4](#34-validate-your-configuration). After fixing, go to **Deployment** (⌥D) → **Code locations** → **Reload**, or click **Reload definitions** (top right); restarting `dagster dev` also works |
| Run fails with `Open-Meteo API error 400: … invalid String value <name>` | Misspelt variable, or it's in the wrong section | Fix the name in `variables.yaml` (check the right section in the Open-Meteo docs) |
| Run fails with `Open-Meteo API error 429` or `5xx` after retries | Rate limit or API outage (calls are retried 3 times) | Wait and re-run; reduce cities or poll frequency if it's 429 |
| Some columns are completely empty for past days | Forecast-only variable (e.g. `precipitation_probability`, `uv_index`) | Expected; see the asset's `unsupported_variables` metadata |
| `silver_quality_check` fails with `out of range` | A value is outside `valid_range` | Check whether the data or the range is wrong; adjust `valid_range` if it's too strict |
| `gold_current_freshness_check` fails | No recent snapshot (schedule off, Dagster was stopped, or API failures) | Turn on `current_weather_schedule` and check recent `current_weather_job` runs |
| `bronze_current` run succeeds but writes nothing | Partition isn't today (UTC), or `current_variables` is empty | Expected; run it for today's partition |
| `IOException: Could not set lock on file … weather.duckdb` | Another process (e.g. your Python session) holds DuckDB open for writing | Close it and open the warehouse with `read_only=True`; jobs retry for ~10 s |
| `Address already in use` on start | Port 3000 is taken (maybe another `dagster dev`) | Stop the other one (`Ctrl+C` in its terminal) or use `dagster dev -p 3001` |

## 9. Stop and reset

- **Stop:** press `Ctrl+C` in the `dagster dev` terminal. Schedules stop with it.
- **Clear data and start fresh** (this deletes everything the pipeline produced):

  ```bash
  rm -rf data/bronze/* data/silver/* data/gold/* warehouse/weather.duckdb
  ```

- **Also clear run history and schedule state:** stop Dagster, then `rm -rf .dagster_home`.

The `.gitkeep` files in `data/` and `warehouse/` are hidden, so the globs above leave them in place.
