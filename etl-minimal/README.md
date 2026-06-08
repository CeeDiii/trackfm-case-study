# ETL Minimal — Last.fm 1K Dataset

## How to run

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

### Step 1 — Build the image

```bash
docker-compose build
```

### Step 2 — Download and extract the dataset

```bash
docker-compose run ingest
```

This downloads the Last.fm 1K dataset (~640 MB) into `data/` and extracts it. If the archive or extracted directory already exist the step is skipped automatically.

### Step 3 — Run the exercises

**Run all exercises in sequence:**

```bash
docker-compose run solve
```

This runs exercise 2 followed by exercise 3 with a 90-day forecast horizon. Results are written to `data/results/`:

| File | Description |
|---|---|
| `excercise2_top_10_songs_in_top_50_longest_sessions.tsv` | Top 10 songs from the 50 longest sessions |
| `exercise3_forecast_session_count.tsv` | 90-day session count forecast for the top user |

**Run exercises individually:**

```bash
docker-compose run exercise2   # top 10 songs in the 50 longest sessions
docker-compose run exercise3   # session metric forecast (see configuration below)
```

### Exercise 3 configuration

One environment variable controls the forecast horizon:

| Variable | Default | Options |
|---|---|---|
| `FORECAST_HORIZON_DAYS` | `90` | any positive integer |

Override the horizon inline:

```bash
FORECAST_HORIZON_DAYS=90 docker-compose run exercise3
```
---

## Assumptions

- **Ingestion** — ingestion is intentionally naive: the dataset is downloaded and extracted in a single script with no orchestration layer. No scheduler, workflow engine, or retry logic is required.
- **Data modelling** — data modelling concerns (schemas, dimensional design, data warehouse layers) are out of scope and have been deliberately ignored.

---

## What I would improve with more time

- **Exercise 2** — replace the current single-script approach with a proper ELT pipeline: orchestrated ingestion, structured transformation steps, and a medallion architecture (bronze → silver → gold) to separate raw data from cleaned and aggregated layers.
- **Exercise 3** — invest more time in model selection to find one that is better suited to short-frequency seasonal patterns (e.g. weekly cycles). The current `AutoARIMA` model does not reliably capture weekly seasonality in sparse user data and tends to converge to a flat mean forecast.
