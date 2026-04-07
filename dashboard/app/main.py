"""
Dashboard — Streamlit UI for equipment utilization monitoring.

Connects to TimescaleDB, displays per-equipment KPIs,
activity breakdowns, timeline chart, and recent events.
Auto-refreshes on a configurable interval.
"""

import os
import time
import json
import sqlite3
import pandas as pd
import streamlit as st

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://admin:changeme@localhost:5432/equipment_tracking"
)
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "5"))


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Equipment Tracker",
    page_icon="🏗️",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

@st.cache_resource
def get_db_connection():
    """Connect to PostgreSQL or SQLite depending on DATABASE_URL."""
    try:
        if DATABASE_URL.startswith("sqlite:///"):
            db_path = DATABASE_URL.replace("sqlite:///", "")
            conn = sqlite3.connect(db_path, check_same_thread=False)
            return conn
        elif HAS_PSYCOPG2:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            return conn
        else:
            st.error("No database driver available. Install psycopg2-binary for PostgreSQL.")
            return None
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def get_utilization_summary(conn):
    """Read per-equipment utilization summary."""
    try:
        df = pd.read_sql("""
            SELECT equipment_id, equipment_label, total_events, active_events,
                   idle_events, utilization_pct, activity_breakdown,
                   updated_at
            FROM utilization_summary
            ORDER BY equipment_id
        """, conn)
        return df
    except Exception:
        return pd.DataFrame()


def get_recent_events(conn, limit=50):
    """Fetch the most recent equipment events."""
    try:
        df = pd.read_sql(f"""
            SELECT timestamp_sec, equipment_id, equipment_label, activity,
                   is_active, motion_score, frame
            FROM equipment_events
            ORDER BY frame DESC
            LIMIT {int(limit)}
        """, conn)
        return df
    except Exception:
        return pd.DataFrame()


def get_activity_timeline(conn):
    """Activity counts grouped into 5-second time buckets for timeline chart."""
    try:
        df = pd.read_sql("""
            SELECT
                FLOOR(timestamp_sec / 5) * 5 AS time_bucket,
                activity,
                COUNT(*) AS count
            FROM equipment_events
            GROUP BY time_bucket, activity
            ORDER BY time_bucket
        """, conn)
        return df
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------

def render_dashboard():
    """Main dashboard rendering."""

    st.title("🏗️ Construction Equipment Tracker")
    st.caption("Real-time equipment utilization monitoring")

    conn = get_db_connection()
    if conn is None:
        st.warning("Waiting for database connection...")
        time.sleep(REFRESH_INTERVAL)
        st.rerun()
        return

    # --- Fetch data ---
    summary_df = get_utilization_summary(conn)
    recent_df = get_recent_events(conn)
    timeline_df = get_activity_timeline(conn)

    if summary_df.empty:
        st.info("No data yet. Start the CV service and analytics service to see results.")
        time.sleep(REFRESH_INTERVAL)
        st.rerun()
        return

    # =====================================================================
    # KPI cards — one per equipment
    # =====================================================================
    st.subheader("Equipment Utilization")

    cols = st.columns(len(summary_df))
    for i, row in summary_df.iterrows():
        with cols[i]:
            utilization = row["utilization_pct"]
            eq_id = row.get("equipment_id", row["equipment_label"])
            total = row["total_events"]
            active = row["active_events"]

            # Utilization percentage
            st.metric(
                label=f"📊 {eq_id}",
                value=f"{utilization:.1f}%",
                delta=f"{active} active / {total} total events",
            )

    st.divider()

    # =====================================================================
    # Activity breakdown
    # =====================================================================
    col_chart, col_details = st.columns([2, 1])

    with col_chart:
        st.subheader("Activity Breakdown")

        for _, row in summary_df.iterrows():
            breakdown = row["activity_breakdown"]
            if isinstance(breakdown, str):
                breakdown = json.loads(breakdown)

            if breakdown:
                # Convert to DataFrame for charting
                activity_df = pd.DataFrame(
                    list(breakdown.items()),
                    columns=["Activity", "Events"]
                ).set_index("Activity")

                st.bar_chart(activity_df, horizontal=True, height=200)

    with col_details:
        st.subheader("Details")
        for _, row in summary_df.iterrows():
            breakdown = row["activity_breakdown"]
            if isinstance(breakdown, str):
                breakdown = json.loads(breakdown)
            total = row["total_events"]

            if breakdown:
                for activity, count in sorted(breakdown.items(), key=lambda x: -x[1]):
                    pct = count / total * 100 if total > 0 else 0
                    icon = {
                        "DIGGING": "⛏️",
                        "SWINGING": "🔄",
                        "DUMPING": "📦",
                        "IDLE": "💤",
                        "ACTIVE": "✅",
                    }.get(activity, "▪️")
                    st.text(f"{icon} {activity:<12s} {count:>4d} events ({pct:>5.1f}%)")

    st.divider()

    # =====================================================================
    # Activity timeline
    # =====================================================================
    if not timeline_df.empty:
        st.subheader("Activity Timeline")

        # Pivot for stacked area chart
        pivot_df = timeline_df.pivot_table(
            index="time_bucket",
            columns="activity",
            values="count",
            fill_value=0,
        )
        pivot_df.index.name = "Time (seconds)"

        st.area_chart(pivot_df, height=300)

    st.divider()

    # =====================================================================
    # Recent events table
    # =====================================================================
    st.subheader("Recent Events")

    if not recent_df.empty:
        recent_df["status"] = recent_df["activity"].map({
            "DIGGING": "⛏️ DIGGING",
            "SWINGING": "🔄 SWINGING",
            "DUMPING": "📦 DUMPING",
            "IDLE": "💤 IDLE",
            "ACTIVE": "✅ ACTIVE",
        })

        st.dataframe(
            recent_df[["timestamp_sec", "equipment_id", "status", "motion_score", "frame"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "timestamp_sec": st.column_config.NumberColumn("Time (s)", format="%.2f"),
                "equipment_id": "Equipment",
                "status": "Activity",
                "motion_score": st.column_config.NumberColumn("Motion", format="%.2f"),
                "frame": "Frame #",
            },
        )
    else:
        st.info("No events recorded yet.")

    # Auto-refresh
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    render_dashboard()
