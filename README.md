# Equipment Utilization & Activity Classification

A microservices pipeline that processes construction equipment video to track utilization (ACTIVE / INACTIVE), classify activities (Digging, Swinging, Dumping, Idle), and display results on a live dashboard — all streamed through Kafka.

## Demo

![Demo](demo.gif)

A pre-processed sample is included in the `demo/` folder (`annotated_sample.mp4` + `events_sample.json`).

## How It Works

```
cv-service  ──>  Kafka  ──>  analytics-service  ──>  TimescaleDB  <──  dashboard (Streamlit)
```

- **cv-service** — Detects equipment using Roboflow, tracks them with IoU + HSV histogram re-ID, runs region-based optical flow to classify motion and activity, then publishes events to Kafka.
- **analytics-service** — Consumes Kafka events, writes to TimescaleDB, computes utilization stats per equipment.
- **dashboard** — Reads from TimescaleDB and shows KPIs, activity breakdowns, timeline, and a live event table. Auto-refreshes every 5 seconds.

Infrastructure: Zookeeper, Kafka, TimescaleDB — all managed through `docker-compose.yml`.

## Getting Started

**Requirements:** Docker + Docker Compose installed.

```bash
git clone https://github.com/abdogomaa201099/EquipmentsActivityMonitoring.git
cd EquipmentsActivityMonitoring
```

Create the `.env` file (PowerShell):
```powershell
Set-Content -Path .env -Value "ROBOFLOW_API_KEY=3GPqFHWy4SNHqmgqGCI6" -Encoding ASCII
```

Or on Linux/Mac:
```bash
echo "ROBOFLOW_API_KEY=3GPqFHWy4SNHqmgqGCI6" > .env
```

Place a construction equipment video at `videos/sample.mp4`, then:

```bash
docker compose up --build -d
docker compose logs cv-service -f       # watch progress
```

Open **http://localhost:8501** to see the dashboard.

To process a different video:
```bash
VIDEO_SOURCE=/videos/other.mp4 docker compose up cv-service -d
```

To stop:
```bash
docker compose down -v
```

## Project Structure

```
cv-service/
    app/main.py              # detection, tracking, motion analysis, activity classification
    Dockerfile
    requirements.txt
analytics-service/
    app/main.py              # Kafka consumer, DB writes, utilization calculation
    init.sql
    Dockerfile
    requirements.txt
dashboard/
    app/main.py              # Streamlit UI
    Dockerfile
    requirements.txt
demo/
    annotated_sample.mp4     # pre-processed output video
    events_sample.json       # corresponding event log
docker-compose.yml
.env                         # API key (not committed — create it yourself)
.gitignore
README.md
```
