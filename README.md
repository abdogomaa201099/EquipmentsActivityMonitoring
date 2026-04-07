# Equipment Utilization & Activity Classification

Real-time microservices pipeline that processes construction equipment video, tracks utilization states (ACTIVE/INACTIVE), classifies work activities, and streams results through Kafka to a dashboard.

## Demo

![Demo](demo.gif)

## Architecture

```
┌──────────────┐     ┌───────┐     ┌───────────────────┐     ┌────────────┐
│  cv-service  │────>│ Kafka │────>│ analytics-service │────>│ TimescaleDB│
│  (detection, │     │       │     │ (consumer, DB     │     │            │
│   tracking,  │     └───────┘     │  writer, stats)   │     └─────┬──────┘
│   motion,    │                   └───────────────────┘           │
│   activity)  │                                                   │
└──────────────┘                   ┌───────────────────┐           │
                                   │    dashboard      │───────────┘
                                   │   (Streamlit UI)  │
                                   └───────────────────┘
```

**Six containers** orchestrated via Docker Compose:

| Service | Role |
|---------|------|
| `zookeeper` | Kafka cluster coordination |
| `kafka` | Message broker between CV and analytics |
| `timescaledb` | PostgreSQL with time-series extensions for event storage |
| `cv-service` | Object detection (Roboflow), tracking, optical flow, activity classification |
| `analytics-service` | Kafka consumer, stores events in DB, calculates utilization |
| `dashboard` | Streamlit web UI showing KPIs, activity breakdowns, timeline |

## Prerequisites

- **Docker** and **Docker Compose**
- A **Roboflow API key** (set in `.env`)
- A test video placed at `videos/sample.mp4`

## Setup & Run

1. **Clone and configure:**
   ```bash
   git clone <repo-url> && cd assessmentCV
   echo "ROBOFLOW_API_KEY=your_key_here" > .env
   ```

2. **Place a video:**
   Put a construction equipment video (e.g., excavator footage) at `videos/sample.mp4`.

3. **Start:**
   ```bash
   docker compose up --build -d
   ```

4. **Monitor progress:**
   ```bash
   docker compose logs cv-service -f
   ```

5. **View dashboard:**
   Open [http://localhost:8501](http://localhost:8501) in your browser once processing begins.

6. **Check results:**
   - Annotated video: `output/annotated_sample.mp4`
   - Events JSON: `output/events_sample.json`
   - Database: query TimescaleDB on port 5432

## Processing a Different Video

```bash
VIDEO_SOURCE=/videos/my_video.mp4 docker compose up cv-service -d
```

## Stopping

```bash
docker compose down        # stop containers
docker compose down -v     # stop + delete database volume
```

## Project Structure

```
├── cv-service/
│   ├── app/main.py         # Detection, tracking, motion analysis, activity classification
│   ├── Dockerfile
│   └── requirements.txt
├── analytics-service/
│   ├── app/main.py         # Kafka consumer, DB writer, utilization calculation
│   ├── init.sql            # TimescaleDB extension setup
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/
│   ├── app/main.py         # Streamlit UI
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env                    # Roboflow API key (not committed)
├── .gitignore
├── videos/                 # Input videos (gitignored)
├── output/                 # Annotated videos + event logs (gitignored)
└── WRITEUP.md              # Technical design write-up
```
