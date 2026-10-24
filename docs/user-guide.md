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
| **Gold** | Daily temperature and precipitation summaries (per local day), plus the latest current conditions | `data/gold/` and the DuckDB database `warehouse/weather.duckdb` |
| **Dimensions** | `dim_location` (city attributes, grid elevation) and `dim_weather_code` (WMO code labels), plus ready-made SQL views joining them to the data | `data/gold/dimensions/` and `warehouse/weather.duckdb` |

Each layer is split into **partitions**, one per day: a UTC calendar day for Bronze and Silver, and each city's **local** calendar day for Gold. Dagster (the orchestrator) runs the steps, checks data quality and shows everything in a web UI.

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
  - id: melbourne                   # optional stable key (lowercase, digits, _); defaults to a slug of name
    name: Melbourne                 # used in file names and tables, so keep it unique
    latitude: -37.8136              # WGS84, -90..90
    longitude: 144.9631             # WGS84, -180..180
    timezone: Australia/Melbourne   # IANA timezone name
    country: Australia              # optional, for grouping in analytics
    region: Victoria                # optional, for grouping in analytics
    tags: [capital]                 # optional free-form labels

  - name: Sydney                    # example: add a city (id becomes "sydney")
    latitude: -33.8688
    longitude: 151.2093
    timezone: Australia/Sydney
    region: New South Wales
```

- Look up coordinates at https://www.latlong.net/ and the timezone name in the [tz database list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).
- The timezone decides what counts as a "local day" for daily values, Gold summaries and the local timestamps in Silver.
- Every table carries `location_id`; analytics join it to the **`dim_location`** table (country, region, tags, plus the model grid coordinates and elevation that Open-Meteo reports). **Keep a city's `id` unchanged** once data exists: changing it starts a new location.
- Removing a city keeps its history: it stays in `dim_location` with `is_active = false`.

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
    dtype: float             # optional: float (default) or integer, for codes and flags
```

To **add a variable**, find its exact name in the [Open-Meteo docs](https://open-meteo.com/en/docs) under the right section (hourly, daily or current) and add an entry. On the next run it appears as a new column in the matching Silver table.

Rules worth knowing:
- **Names must be valid for their section.** For example, `temperature_2m_max` is daily-only and `temperature_2m` is hourly/current. A wrong name makes the run fail with Open-Meteo's error message.
- **Some variables aren't available for historical days.** `precipitation_probability`, `uv_index`, `precipitation_probability_max` and `uv_index_max` are forecast-only. Past days get empty (null) values for them, which is expected, and the run's metadata lists them.
- **Time-valued variables** such as `sunrise`/`sunset` need `unit: "iso8601"`. They're stored as UTC timestamps.
- **Codes and flags** such as `weather_code` and `is_day` use `dtype: integer`, so they're stored as whole numbers (empty stays empty).
- **The same name can appear in different sections** (e.g. `temperature_2m` in hourly and current) but not twice in one section. Names such as `time`, `location_name` or `timestamp_utc` are reserved.
- **Gold needs these hourly variables:** `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, `uv_index`, `precipitation` and `precipitation_probability`. If you remove any of them, the pipeline refuses to start and tells you which one is missing.
- **A key written twice** in an entry (e.g. two `valid_range` lines) is an error, not silently overwritten.

**Weather code labels — `config/weather_codes.yaml`.** `weather_code` values are WMO codes (e.g. 61). This file maps all 29 codes Open-Meteo uses to a description ("Slight rain") and analytics categories:
- `condition_group` (clear, cloudy, fog, drizzle, rain, snow, showers, thunderstorm)
- `precipitation_type`, `intensity`
- `is_precipitation`, `is_freezing`, `is_thunderstorm`, `severity_rank`

It feeds the **`dim_weather_code`** table. You can relabel or translate entries without code changes. If a code appears in the data that isn't in the file, the `*_weather_code_check` checks fail and name it.

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
| `duplicate location ids` / `String should match pattern` (id) | Two cities share an id, or an id has capitals/spaces | Give each city a unique lowercase `id` (letters, digits, `_`) |
| `unknown IANA timezone` | Misspelt `timezone` | Use a name from the tz database list, e.g. `Australia/Sydney` |
| `duplicate keys [...]` | A key appears twice in one YAML entry | Keep one of them |
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
| **Catalog** | ⌥C | The 11 assets: `bronze_forecast`, `silver_hourly_weather`, `silver_daily_weather`, `gold_daily_temperature`, `gold_daily_precipitation`, `bronze_current`, `silver_current_weather`, `gold_current_conditions`, and (group *dimensions*) `dim_location`, `dim_weather_code`, `warehouse_views` |
| **Jobs** | ⌥J | `weather_pipeline_job` (hourly + daily data, then dimensions and views), `current_weather_job` (current conditions), `dimensions_job` (dimensions and views only) |
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

> **Pipeline partitions are UTC days.** In eastern Australia the UTC date changes at 10:30–11:00 local time, so in the early morning today's partition is still dated yesterday.

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

Past days come from Open-Meteo's archive. The last day or two, which the archive doesn't have yet, come from the forecast API automatically. When the partitions are done, the backfill also rebuilds `dim_location`, `dim_weather_code` and the SQL views (once).

> **After upgrading the pipeline** (new columns such as `location_id`, or a change in how Gold days are computed), run this backfill once over **All** partitions so older files are rewritten in the new format.

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
data/gold/daily_temperature/{date}.parquet       one row per city per LOCAL day
data/gold/daily_precipitation/{date}.parquet
data/gold/current_conditions/latest.parquet
data/gold/dimensions/dim_location.parquet         one row per city
data/gold/dimensions/dim_weather_code.parquet     one row per WMO code
warehouse/weather.duckdb                          Gold + dimension tables, and SQL views over Silver
```

### SQL (DuckDB)

DuckDB is installed with the project. Open the warehouse **read-only** so you don't block running jobs, and close it when done. All examples below run inside this wrapper:

```bash
python - <<'EOF'
import duckdb
con = duckdb.connect("warehouse/weather.duckdb", read_only=True)
con.sql("SET TimeZone = 'UTC'")   # show timestamps in UTC instead of your machine's timezone
print(con.sql("SHOW TABLES"))
print(con.sql("""
    SELECT location_id, date, temp_min_c, temp_max_c, hours_covered
    FROM gold_daily_temperature ORDER BY date DESC, location_id LIMIT 10
"""))
con.close()
EOF
```

**What's in the warehouse:**

| Name | Kind | Grain / contents |
|---|---|---|
| `gold_daily_temperature`, `gold_daily_precipitation` | table | city × **local** day (`date`); `hours_covered` / `total_hours` say how many hours went in |
| `gold_current_conditions` | table | latest snapshot per city, with `age_minutes` |
| `dim_location` | table | one row per city: `country`, `region`, `tags`, `elevation_m`, grid coordinates, `is_active` |
| `dim_weather_code` | table | one row per WMO code: `description`, `condition_group`, `precipitation_type`, `intensity`, flags, `severity_rank` |
| `silver_hourly_weather`, `silver_daily_weather`, `silver_current_weather` | view | the Silver Parquet files, read in place |
| `v_hourly_weather`, `v_daily_weather`, `v_current_conditions` | view | Silver/current joined to both dimensions (adds `region`, `weather_description`, `condition_group`, …) |

Join your own tables on **`location_id`** (and the local `date`). The views are refreshed at the end of every pipeline run, or on demand with `dimensions_job`.

**Example analytics** (paste a query into `print(con.sql("""..."""))`):

```sql
-- Share of hours by condition (clear, cloudy, drizzle, rain, ...) per city
SELECT location_name, condition_group, count(*) AS hours,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY location_name), 1) AS pct_hours
FROM v_hourly_weather
WHERE weather_code IS NOT NULL
GROUP BY location_name, condition_group
ORDER BY location_name, hours DESC;

-- Monthly summary by region (Gold days are local calendar days)
SELECT l.region, strftime(CAST(t.date AS DATE), '%Y-%m') AS month,
       round(avg(t.temp_max_c), 1) AS avg_daily_max_c,
       round(sum(p.total_precipitation_mm), 1) AS total_precip_mm,
       count(*) FILTER (WHERE p.rainy_hours > 0) AS days_with_rain
FROM gold_daily_temperature t
JOIN gold_daily_precipitation p USING (location_id, date)
JOIN dim_location l USING (location_id)
GROUP BY ALL
ORDER BY month, l.region;

-- Day vs night
SELECT location_name, CASE is_day WHEN 1 THEN 'day' ELSE 'night' END AS period,
       round(avg(temperature_2m), 1) AS avg_temp_c, round(sum(precipitation), 1) AS precip_mm,
       round(avg(cloud_cover), 0) AS avg_cloud_pct
FROM v_hourly_weather
WHERE is_day IS NOT NULL
GROUP BY ALL
ORDER BY location_name, period;

-- "Feels like" vs actual temperature
SELECT location_name,
       round(avg(apparent_temperature - temperature_2m), 1) AS avg_feels_minus_actual_c,
       round(min(apparent_temperature), 1) AS coldest_feels_like_c,
       round(max(apparent_temperature), 1) AS hottest_feels_like_c
FROM v_hourly_weather
WHERE apparent_temperature IS NOT NULL
GROUP BY ALL;

-- Most severe days (daily weather_code = the day's most severe condition, ranked by WMO code order)
SELECT location_name, date_local, weather_description, precipitation_sum, temperature_2m_max
FROM v_daily_weather
WHERE weather_code IS NOT NULL
ORDER BY weather_severity_rank DESC, precipitation_sum DESC
LIMIT 5;

-- Joining your own data, e.g. daily sales per city (location_id + local date)
WITH sales(location_id, date, revenue) AS (
    VALUES ('adelaide', '2026-10-20', 1200.0), ('melbourne', '2026-10-20', 1500.0)
)
SELECT s.*, t.temp_max_c, p.total_precipitation_mm
FROM sales s
JOIN gold_daily_temperature t USING (location_id, date)
JOIN gold_daily_precipitation p USING (location_id, date);
```

**Reading timestamps:**
- `timestamp_utc` / `observed_at_utc` are UTC; `timestamp_local` / `observed_at_local` are in the city's timezone.
- For half-hour-offset cities like Adelaide (UTC+10:30), hourly values fall at `:30` UTC, which is the top of the hour locally.
- Daily rows (`date_local`) cover the city's own calendar day. Open-Meteo computes them with the UTC offset in effect when the data was requested, so for days across a daylight-saving change the day's window is shifted by an hour.

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
| `*_weather_code_check` fails with `Unknown weather codes [..]` | Open-Meteo returned a code missing from `config/weather_codes.yaml` | Add the code to the file, then run `dimensions_job` |
| `gold_temperature_check` warns `only N hours of the local day` | A neighbouring UTC day isn't loaded yet (e.g. the first day of the range) | Load the neighbouring day; the warning clears on the next run |
| `dim_location_check` warns about missing grid data | A newly added city has no Bronze data yet | Run `weather_pipeline_job` for it once |
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
