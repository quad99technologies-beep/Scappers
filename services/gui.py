#!/usr/bin/env python3
"""
Streamlit GUI Control Panel for the Scraping Platform.

This provides a web-based interface for:
- Starting new pipeline runs
- Viewing running jobs
- Stopping/resuming jobs
- Viewing errors and logs
- Monitoring statistics

Run with:
    streamlit run services/gui.py
"""

import os
import sys

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Try to import database functions
try:
    from services.db import (
        get_cursor,
        create_pipeline_run,
        issue_command,
        ensure_platform_schema,
    )
    DB_AVAILABLE = True
except Exception as e:
    DB_AVAILABLE = False
    DB_ERROR = str(e)


# =============================================================================
# Page Configuration
# =============================================================================

st.set_page_config(
    page_title="Scraper Control Panel",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =============================================================================
# Database Helper Functions
# =============================================================================

@st.cache_data(ttl=5)  # Cache for 5 seconds
def get_pipeline_runs(status_filter: str = None, limit: int = 50) -> pd.DataFrame:
    """Get pipeline runs from database."""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        with get_cursor(dict_cursor=True) as cur:
            if status_filter and status_filter != "All":
                cur.execute("""
                    SELECT run_id, country, status, current_step, current_step_num, total_steps,
                           worker_id, started_at, ended_at, last_heartbeat, retry_count, error_message,
                           created_at
                    FROM pipeline_runs
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (status_filter.lower(), limit))
            else:
                cur.execute("""
                    SELECT run_id, country, status, current_step, current_step_num, total_steps,
                           worker_id, started_at, ended_at, last_heartbeat, retry_count, error_message,
                           created_at
                    FROM pipeline_runs
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
            
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def get_workers() -> pd.DataFrame:
    """Get worker status from database."""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        with get_cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT worker_id, hostname, pid, status, current_run_id, 
                       started_at, last_heartbeat
                FROM workers
                ORDER BY last_heartbeat DESC
            """)
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_recent_errors(country: str = None, limit: int = 100) -> pd.DataFrame:
    """Get recent errors from database."""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        with get_cursor(dict_cursor=True) as cur:
            if country and country != "All":
                cur.execute("""
                    SELECT id, run_id, country, step, error_type, error_code, 
                           error_message, severity, created_at
                    FROM errors
                    WHERE country = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (country, limit))
            else:
                cur.execute("""
                    SELECT id, run_id, country, step, error_type, error_code, 
                           error_message, severity, created_at
                    FROM errors
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
            
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_fetch_stats(days: int = 7) -> pd.DataFrame:
    """Get fetch statistics."""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        with get_cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT 
                    DATE(fetched_at) AS date,
                    method,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE success) AS success_count,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE success) / COUNT(*), 1) AS success_rate,
                    ROUND(AVG(latency_ms)::numeric, 0) AS avg_latency_ms,
                    COUNT(*) FILTER (WHERE fallback_used) AS fallback_count
                FROM fetch_logs
                WHERE fetched_at > CURRENT_DATE - INTERVAL '%s days'
                GROUP BY DATE(fetched_at), method
                ORDER BY date DESC, method
            """, (days,))
            
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_entity_counts() -> pd.DataFrame:
    """Get entity counts by country and type."""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        with get_cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT country, entity_type, COUNT(*) AS count,
                       MAX(created_at) AS last_created
                FROM entities
                WHERE status = 'active'
                GROUP BY country, entity_type
                ORDER BY country, entity_type
            """)
            
            rows = cur.fetchall()
            return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def get_available_countries() -> list:
    """Get list of available countries."""
    return [
        "Argentina", "Belarus", "Canada Ontario", "CanadaQuebec",
        "India", "Malaysia", "Netherlands", "North Macedonia",
        "Russia", "Taiwan", "Tender- Chile"
    ]


# =============================================================================
# Sidebar
# =============================================================================

with st.sidebar:
    st.title("🕷️ Scraper Control")
    
    if not DB_AVAILABLE:
        st.error(f"Database not available: {DB_ERROR}")
    else:
        st.success("Database connected")
    
    st.divider()
    
    # Navigation
    page = st.radio(
        "Navigation",
        ["Dashboard", "Pipeline Runs", "Start New Run", "Workers", "Errors", "Statistics"]
    )
    
    st.divider()
    
    # Quick actions
    st.subheader("Quick Actions")
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# =============================================================================
# Dashboard Page
# =============================================================================

if page == "Dashboard":
    st.title("📊 Dashboard")
    
    if not DB_AVAILABLE:
        st.warning("Database not available. Please check your configuration.")
    else:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        runs_df = get_pipeline_runs(limit=1000)
        workers_df = get_workers()
        
        if not runs_df.empty:
            with col1:
                queued = len(runs_df[runs_df['status'] == 'queued'])
                st.metric("Queued", queued)
            
            with col2:
                running = len(runs_df[runs_df['status'] == 'running'])
                st.metric("Running", running)
            
            with col3:
                completed = len(runs_df[runs_df['status'] == 'completed'])
                st.metric("Completed (7d)", completed)
            
            with col4:
                failed = len(runs_df[runs_df['status'] == 'failed'])
                st.metric("Failed (7d)", failed, delta_color="inverse")
        
        st.divider()
        
        # Active runs
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏃 Active Runs")
            active_runs = runs_df[runs_df['status'].isin(['queued', 'running'])]
            if not active_runs.empty:
                for _, run in active_runs.head(5).iterrows():
                    status_emoji = "🔄" if run['status'] == 'running' else "⏳"
                    step_info = f"Step {run['current_step_num']}/{run['total_steps']}" if run['total_steps'] else ""
                    st.write(f"{status_emoji} **{run['country']}** - {run['status']} {step_info}")
            else:
                st.info("No active runs")
        
        with col2:
            st.subheader("🖥️ Workers")
            if not workers_df.empty:
                active_workers = workers_df[workers_df['status'].isin(['active', 'busy', 'idle'])]
                for _, worker in active_workers.head(5).iterrows():
                    status_emoji = "🟢" if worker['status'] in ['active', 'busy'] else "🟡"
                    st.write(f"{status_emoji} **{worker['hostname']}** - {worker['status']}")
            else:
                st.info("No workers registered")
        
        st.divider()
        
        # Recent errors
        st.subheader("⚠️ Recent Errors")
        errors_df = get_recent_errors(limit=5)
        if not errors_df.empty:
            for _, error in errors_df.iterrows():
                severity_color = "🔴" if error['severity'] == 'critical' else "🟠" if error['severity'] == 'error' else "🟡"
                st.write(f"{severity_color} [{error['country']}] {error['error_type']}: {error['error_message'][:100]}")
        else:
            st.success("No recent errors")


# =============================================================================
# Pipeline Runs Page
# =============================================================================

elif page == "Pipeline Runs":
    st.title("🔄 Pipeline Runs")
    
    if not DB_AVAILABLE:
        st.warning("Database not available")
    else:
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.selectbox(
                "Status Filter",
                ["All", "Queued", "Running", "Completed", "Failed", "Stopped"]
            )
        
        with col2:
            limit = st.number_input("Limit", min_value=10, max_value=500, value=50)
        
        with col3:
            auto_refresh = st.checkbox("Auto-refresh (5s)", value=False)
        
        if auto_refresh:
            st.cache_data.clear()
        
        # Get runs
        runs_df = get_pipeline_runs(
            status_filter=status_filter if status_filter != "All" else None,
            limit=limit
        )
        
        if not runs_df.empty:
            # Display as table
            st.dataframe(
                runs_df[['run_id', 'country', 'status', 'current_step', 'worker_id', 'started_at', 'ended_at']],
                use_container_width=True
            )
            
            # Actions
            st.subheader("Actions")
            
            selected_run = st.selectbox(
                "Select Run",
                runs_df['run_id'].tolist(),
                format_func=lambda x: f"{x[:8]}... - {runs_df[runs_df['run_id']==x]['country'].values[0]}"
            )
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("⏹️ Stop"):
                    try:
                        issue_command(selected_run, "stop", "gui")
                        st.success("Stop command issued")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col2:
                if st.button("▶️ Resume"):
                    try:
                        issue_command(selected_run, "resume", "gui")
                        st.success("Resume command issued")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col3:
                if st.button("❌ Cancel"):
                    try:
                        issue_command(selected_run, "cancel", "gui")
                        st.success("Cancel command issued")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.info("No pipeline runs found")


# =============================================================================
# Start New Run Page
# =============================================================================

elif page == "Start New Run":
    st.title("🚀 Start New Run")
    
    if not DB_AVAILABLE:
        st.warning("Database not available")
    else:
        with st.form("new_run_form"):
            country = st.selectbox("Country", get_available_countries())
            
            col1, col2 = st.columns(2)
            
            with col1:
                total_steps = st.number_input("Total Steps", min_value=1, max_value=20, value=5)
            
            with col2:
                priority = st.number_input("Priority", min_value=0, max_value=100, value=0)
            
            submitted = st.form_submit_button("Start Run")
            
            if submitted:
                try:
                    run_id = create_pipeline_run(
                        country=country,
                        total_steps=total_steps,
                        priority=priority
                    )
                    st.success(f"Created run: {run_id}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"Error creating run: {e}")
        
        st.divider()
        
        # Queued runs
        st.subheader("⏳ Queued Runs")
        queued_df = get_pipeline_runs(status_filter="queued", limit=10)
        if not queued_df.empty:
            st.dataframe(queued_df[['run_id', 'country', 'priority', 'created_at']])
        else:
            st.info("No queued runs")


# =============================================================================
# Workers Page
# =============================================================================

elif page == "Workers":
    st.title("🖥️ Workers")
    
    if not DB_AVAILABLE:
        st.warning("Database not available")
    else:
        workers_df = get_workers()
        
        if not workers_df.empty:
            # Summary
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total = len(workers_df)
                st.metric("Total Workers", total)
            
            with col2:
                active = len(workers_df[workers_df['status'].isin(['active', 'busy'])])
                st.metric("Active", active)
            
            with col3:
                idle = len(workers_df[workers_df['status'] == 'idle'])
                st.metric("Idle", idle)
            
            with col4:
                offline = len(workers_df[workers_df['status'] == 'offline'])
                st.metric("Offline", offline)
            
            st.divider()
            
            # Table
            st.dataframe(workers_df, use_container_width=True)
        else:
            st.info("No workers registered")
            
        st.divider()
        
        st.subheader("📝 Worker Instructions")
        st.code("""
# Start a worker
python services/worker.py

# Start worker for specific countries
python services/worker.py --countries "India,Malaysia"

# Start watchdog (stale job recovery)
python services/watchdog.py --interval 120
        """, language="bash")


# =============================================================================
# Errors Page
# =============================================================================

elif page == "Errors":
    st.title("⚠️ Errors")
    
    if not DB_AVAILABLE:
        st.warning("Database not available")
    else:
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            country_filter = st.selectbox("Country", ["All"] + get_available_countries())
        
        with col2:
            limit = st.number_input("Limit", min_value=10, max_value=500, value=100)
        
        errors_df = get_recent_errors(
            country=country_filter if country_filter != "All" else None,
            limit=limit
        )
        
        if not errors_df.empty:
            # Summary by type
            st.subheader("Error Summary")
            error_summary = errors_df.groupby('error_type').size().reset_index(name='count')
            st.bar_chart(error_summary.set_index('error_type'))
            
            st.divider()
            
            # Table
            st.subheader("Error Details")
            st.dataframe(errors_df, use_container_width=True)
        else:
            st.success("No errors found")


# =============================================================================
# Statistics Page
# =============================================================================

elif page == "Statistics":
    st.title("📈 Statistics")
    
    if not DB_AVAILABLE:
        st.warning("Database not available")
    else:
        # Fetch stats
        st.subheader("Fetch Performance")
        fetch_stats = get_fetch_stats(days=7)
        
        if not fetch_stats.empty:
            # Success rate by method
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Success Rate by Method**")
                method_stats = fetch_stats.groupby('method').agg({
                    'total': 'sum',
                    'success_count': 'sum'
                }).reset_index()
                method_stats['success_rate'] = (method_stats['success_count'] / method_stats['total'] * 100).round(1)
                st.dataframe(method_stats)
            
            with col2:
                st.write("**Average Latency by Method**")
                latency_stats = fetch_stats.groupby('method')['avg_latency_ms'].mean().reset_index()
                st.bar_chart(latency_stats.set_index('method'))
        
        st.divider()
        
        # Entity counts
        st.subheader("Entity Counts")
        entity_counts = get_entity_counts()
        
        if not entity_counts.empty:
            st.dataframe(entity_counts, use_container_width=True)
        else:
            st.info("No entities found")


# =============================================================================
# Footer
# =============================================================================

st.divider()
st.caption("Scraper Control Panel v1.0 | Powered by Streamlit")
