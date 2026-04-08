"""
Analytics Service — Kafka Consumer & Database Writer.

Consumes equipment events from Kafka, stores them in TimescaleDB,
and calculates per-equipment utilization metrics.
"""

import os
import json
import time
import psycopg2
from kafka import KafkaConsumer


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "equipment-events")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "analytics-group")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://admin:test1234@localhost:5432/equipment_tracking"
)

UTILIZATION_REPORT_EVERY = 50


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def connect_to_database():
    """Connect to PostgreSQL/TimescaleDB with retry for container startup."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            print(f"Database connected: {DATABASE_URL.split('@')[1]}")
            return conn
        except Exception as e:
            wait = 3
            print(f"  DB not ready (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(wait)

    print("ERROR: Could not connect to database.")
    return None


def create_tables(conn):
    """Create the equipment_events and utilization_summary tables."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS equipment_events (
            time              TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
            timestamp_sec     DOUBLE PRECISION NOT NULL,
            frame             INTEGER        NOT NULL,
            equipment_id      TEXT           NOT NULL DEFAULT 'unknown',
            equipment_label   TEXT           NOT NULL,
            activity          TEXT           NOT NULL,
            is_active         BOOLEAN        NOT NULL,
            motion_score      DOUBLE PRECISION,
            confidence        DOUBLE PRECISION,
            bbox              JSONB,
            region_scores     JSONB
        );
    """)

    # Add equipment_id column if migrating from an older schema
    try:
        cur.execute("ALTER TABLE equipment_events ADD COLUMN IF NOT EXISTS equipment_id TEXT DEFAULT 'unknown';")
    except Exception:
        pass

    # Convert to TimescaleDB hypertable if extension is available
    try:
        cur.execute("""
            SELECT create_hypertable('equipment_events', 'time',
                                     if_not_exists => TRUE);
        """)
        print("  TimescaleDB hypertable created.")
    except Exception:
        print("  Using regular PostgreSQL table.")

    # Utilization summary table (recalculated cache, keyed by equipment_id)
    cur.execute("DROP TABLE IF EXISTS utilization_summary;")
    cur.execute("""
        CREATE TABLE utilization_summary (
            equipment_id      TEXT           NOT NULL,
            equipment_label   TEXT           NOT NULL DEFAULT 'unknown',
            total_events      INTEGER        NOT NULL,
            active_events     INTEGER        NOT NULL,
            idle_events       INTEGER        NOT NULL,
            utilization_pct   DOUBLE PRECISION NOT NULL,
            activity_breakdown JSONB,
            updated_at        TIMESTAMPTZ    DEFAULT NOW(),
            PRIMARY KEY (equipment_id)
        );
    """)

    cur.close()
    print("  Database tables ready.")


# ---------------------------------------------------------------------------
# Event storage
# ---------------------------------------------------------------------------

def store_event(conn, event):
    """Insert one equipment event into the database."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO equipment_events
            (timestamp_sec, frame, equipment_id, equipment_label, activity,
             is_active, motion_score, confidence, bbox, region_scores)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        event.get("timestamp_sec", 0),
        event.get("frame", 0),
        event.get("equipment_id", event.get("equipment_label", "unknown")),
        event.get("equipment_label", "unknown"),
        event.get("activity", "IDLE"),
        event.get("is_active", False),
        event.get("motion_score", 0),
        event.get("confidence", 0),
        json.dumps(event.get("bbox", [])),
        json.dumps(event.get("region_scores", {})),
    ))
    cur.close()


# ---------------------------------------------------------------------------
# Utilization calculation
# ---------------------------------------------------------------------------

def calculate_utilization(conn):
    """
    Calculate utilization = (active_events / total_events) * 100%
    per equipment, and upsert into utilization_summary table.
    """
    cur = conn.cursor()

    # Count events per equipment and activity
    cur.execute("""
        SELECT equipment_id, equipment_label, activity, COUNT(*) as cnt
        FROM equipment_events
        GROUP BY equipment_id, equipment_label, activity
        ORDER BY equipment_id, cnt DESC
    """)
    rows = cur.fetchall()

    if not rows:
        cur.close()
        return

    # Build per-equipment summaries
    equipment_data = {}
    for eq_id, label, activity, count in rows:
        if eq_id not in equipment_data:
            equipment_data[eq_id] = {"label": label, "total": 0, "active": 0, "breakdown": {}}
        equipment_data[eq_id]["total"] += count
        equipment_data[eq_id]["breakdown"][activity] = count
        if activity != "IDLE":
            equipment_data[eq_id]["active"] += count

    # Print and store summaries
    print("\n  ╔══════════════════════════════════════════╗")
    print("  ║       UTILIZATION REPORT                 ║")
    print("  ╠══════════════════════════════════════════╣")

    for eq_id, data in equipment_data.items():
        total = data["total"]
        active = data["active"]
        idle = total - active
        utilization = (active / total * 100) if total > 0 else 0

        print(f"  ║ {eq_id:<20s}                     ║")
        print(f"  ║   Total events:  {total:<6d}                  ║")
        print(f"  ║   Active events: {active:<6d} ({utilization:.1f}%)          ║")
        print(f"  ║   Idle events:   {idle:<6d}                  ║")
        print(f"  ║   Breakdown:                              ║")
        for activity, count in sorted(data["breakdown"].items(),
                                       key=lambda x: -x[1]):
            pct = count / total * 100
            print(f"  ║     {activity:<12s} {count:>5d} ({pct:>5.1f}%)        ║")

        # Upsert to summary table
        cur.execute("""
            INSERT INTO utilization_summary
                (equipment_id, equipment_label, total_events, active_events,
                 idle_events, utilization_pct, activity_breakdown, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (equipment_id) DO UPDATE SET
                equipment_label = EXCLUDED.equipment_label,
                total_events = EXCLUDED.total_events,
                active_events = EXCLUDED.active_events,
                idle_events = EXCLUDED.idle_events,
                utilization_pct = EXCLUDED.utilization_pct,
                activity_breakdown = EXCLUDED.activity_breakdown,
                updated_at = NOW()
        """, (
            eq_id, data["label"], total, active, idle, utilization,
            json.dumps(data["breakdown"]),
        ))

    print("  ╚══════════════════════════════════════════╝\n")
    cur.close()


# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------

def create_kafka_consumer():
    """Create a Kafka consumer with retry for container startup ordering."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            print(f"Kafka consumer connected. Listening on topic: '{KAFKA_TOPIC}'")
            return consumer
        except Exception as e:
            wait = 3
            print(f"  Kafka not ready (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(wait)

    print("ERROR: Could not connect to Kafka after all retries.")
    return None


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    """Connect to Kafka + DB, consume events, store and compute utilization."""
    print("=== Analytics Service Starting ===")

    conn = connect_to_database()
    if conn is not None:
        create_tables(conn)
    else:
        print("WARNING: Running without database.")

    consumer = create_kafka_consumer()
    if consumer is None:
        print("Exiting: no Kafka connection.")
        return

    print("Waiting for equipment events...\n")

    event_count = 0
    try:
        for message in consumer:
            event = message.value

            # Print the event
            timestamp = event.get("timestamp_sec", 0)
            eq_id = event.get("equipment_id", event.get("equipment_label", "unknown"))
            activity = event.get("activity", "IDLE")
            motion = event.get("motion_score", 0)
            print(f"  [{timestamp:>7.2f}s] {eq_id}: {activity} (motion={motion:.2f})")

            # Store in database
            if conn is not None:
                store_event(conn, event)

            event_count += 1

            # Periodically calculate and print utilization
            if conn is not None and event_count % UTILIZATION_REPORT_EVERY == 0:
                calculate_utilization(conn)

    except KeyboardInterrupt:
        print(f"\nStopping. Processed {event_count} events total.")
    finally:
        # Final utilization report
        if conn is not None and event_count > 0:
            print("\n=== FINAL UTILIZATION REPORT ===")
            calculate_utilization(conn)
            conn.close()
            print("Database connection closed.")
        if consumer is not None:
            consumer.close()
            print("Kafka consumer closed.")


if __name__ == "__main__":
    main()
