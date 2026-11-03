"""
NYC 311 Service Requests - Streamlit dashboard

Reads the dbt-transformed silver layer in BigQuery and visualizes it:
  * silver_layer.stg_nyc_311             -> row-level cleaned 311 requests
  * silver_layer.mart_agency_performance -> dbt-aggregated agency performance

Run from the project root so the relative credentials path resolves:
    streamlit run app.py
"""

import os

# Authenticate with the service account key file. This must be set before any
# BigQuery client is created.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/gcp.json"

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Page configuration (must be the first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="NYC 311 Dashboard",
    page_icon="🗽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nyc-311-pipeline-512717")
DATASET = "silver_layer"
STG_TABLE = f"`{PROJECT_ID}.{DATASET}.stg_nyc_311`"
MART_TABLE = f"`{PROJECT_ID}.{DATASET}.mart_agency_performance`"

CACHE_TTL_SECONDS = 3600
DEFAULT_WINDOW_DAYS = 90  # default date window ending at the latest request date

# Reusable SQL fragments. Every query that touches stg_nyc_311 uses the same
# filter so all visuals stay consistent with the sidebar selections.
# An empty agency array means "no agency filter" (all agencies).
FILTER_SQL = """
    created_at IS NOT NULL
    AND DATE(created_at) BETWEEN @start_date AND @end_date
    AND (COALESCE(ARRAY_LENGTH(@agencies), 0) = 0 OR agency IN UNNEST(@agencies))
"""

# Resolution time in hours. Rows without a closed_at, or with a closed_at
# before created_at (bad data), yield NULL so they are ignored by AVG().
# TIMESTAMP() makes this work whether the columns are TIMESTAMP, DATETIME or STRING.
RESOLUTION_HOURS_SQL = """
    IF(
        closed_at IS NOT NULL
        AND TIMESTAMP(closed_at) >= TIMESTAMP(created_at),
        TIMESTAMP_DIFF(TIMESTAMP(closed_at), TIMESTAMP(created_at), SECOND) / 3600.0,
        NULL
    )
"""


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def get_client() -> bigquery.Client:
    """Create one BigQuery client per Streamlit server process."""
    return bigquery.Client(project=PROJECT_ID)


def run_query(sql: str, params: list | None = None) -> pd.DataFrame:
    """Execute a parameterized query and return the result as a DataFrame."""
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    return get_client().query(sql, job_config=job_config).to_dataframe()


def filter_params(start: date, end: date, agencies: tuple) -> list:
    """Build the BigQuery query parameters shared by all filtered queries."""
    return [
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
        bigquery.ArrayQueryParameter("agencies", "STRING", list(agencies)),
    ]


# ---------------------------------------------------------------------------
# Cached data loaders (results are cached for one hour per unique argument set)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading filter options...")
def load_filter_options() -> tuple[date | None, date | None, list[str]]:
    """Return the min/max request date and the list of distinct agencies."""
    bounds = run_query(
        f"""
        SELECT MIN(DATE(created_at)) AS min_date,
               MAX(DATE(created_at)) AS max_date
        FROM {STG_TABLE}
        WHERE created_at IS NOT NULL
        """
    )
    agencies = run_query(
        f"""
        SELECT DISTINCT TRIM(agency) AS agency
        FROM {STG_TABLE}
        WHERE agency IS NOT NULL AND TRIM(agency) != ''
        ORDER BY agency
        """
    )
    min_date = pd.to_datetime(bounds.loc[0, "min_date"]).date() if pd.notna(bounds.loc[0, "min_date"]) else None
    max_date = pd.to_datetime(bounds.loc[0, "max_date"]).date() if pd.notna(bounds.loc[0, "max_date"]) else None
    return min_date, max_date, agencies["agency"].dropna().tolist()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Calculating KPIs...")
def load_kpis(start: date, end: date, agencies: tuple) -> dict:
    """Total, closed and average-resolution KPIs for the selected filters."""
    df = run_query(
        f"""
        SELECT
            COUNT(*) AS total_requests,
            COUNTIF(UPPER(TRIM(status)) = 'CLOSED') AS closed_requests,
            AVG({RESOLUTION_HOURS_SQL}) AS avg_resolution_hours
        FROM {STG_TABLE}
        WHERE {FILTER_SQL}
        """,
        filter_params(start, end, agencies),
    )
    row = df.iloc[0]
    total = int(row["total_requests"]) if pd.notna(row["total_requests"]) else 0
    closed = int(row["closed_requests"]) if pd.notna(row["closed_requests"]) else 0
    avg_hours = float(row["avg_resolution_hours"]) if pd.notna(row["avg_resolution_hours"]) else None
    return {
        "total": total,
        "closed": closed,
        # Anything that is not "Closed" (open, assigned, in progress, null...) counts as active.
        "active": total - closed,
        "avg_hours": avg_hours,
    }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading daily volume...")
def load_daily_volume(start: date, end: date, agencies: tuple) -> pd.DataFrame:
    """Number of requests created per day."""
    df = run_query(
        f"""
        SELECT DATE(created_at) AS created_day, COUNT(*) AS requests
        FROM {STG_TABLE}
        WHERE {FILTER_SQL}
        GROUP BY created_day
        ORDER BY created_day
        """,
        filter_params(start, end, agencies),
    )
    df["created_day"] = pd.to_datetime(df["created_day"])
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading complaint types...")
def load_top_complaints(start: date, end: date, agencies: tuple, limit: int = 10) -> pd.DataFrame:
    """Top complaint types by volume. Blank/NULL types are grouped as 'Unspecified'."""
    return run_query(
        f"""
        SELECT
            COALESCE(NULLIF(TRIM(complaint_type), ''), 'Unspecified') AS complaint_type,
            COUNT(*) AS requests
        FROM {STG_TABLE}
        WHERE {FILTER_SQL}
        GROUP BY complaint_type
        ORDER BY requests DESC
        LIMIT {int(limit)}
        """,
        filter_params(start, end, agencies),
    )


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Loading agency performance...")
def load_agency_performance() -> pd.DataFrame:
    """The dbt mart is small and pre-aggregated, so load it whole."""
    return run_query(f"SELECT * FROM {MART_TABLE}")


# ---------------------------------------------------------------------------
# Presentation helpers
# ---------------------------------------------------------------------------
def format_duration(hours: float | None, unit: str) -> str:
    """Format a duration given in hours as hours or days; 'N/A' when missing."""
    if hours is None or pd.isna(hours):
        return "N/A"
    return f"{hours / 24:,.1f} days" if unit == "Days" else f"{hours:,.1f} hrs"


def find_column(columns: list[str], patterns: list[tuple], exclude: set | None = None) -> str | None:
    """
    Find the first column whose lower-cased name contains every substring in
    one of the patterns. Patterns are tried in order of priority. This keeps
    the app working if dbt column names differ slightly from what is assumed.
    """
    exclude = exclude or set()
    for pattern in patterns:
        for col in columns:
            if col in exclude:
                continue
            if all(token in col.lower() for token in pattern):
                return col
    return None


def hours_per_unit(column_name: str) -> float:
    """Guess the unit of a mart duration column from its name; defaults to hours."""
    name = column_name.lower()
    if "hour" in name:
        return 1.0
    if "day" in name:
        return 24.0
    if "min" in name:
        return 1.0 / 60.0
    if "sec" in name:
        return 1.0 / 3600.0
    return 1.0


# ---------------------------------------------------------------------------
# Dashboard sections
# ---------------------------------------------------------------------------
def render_kpis(kpis: dict, unit: str) -> None:
    """Top row of KPI tiles."""
    total, closed, active = kpis["total"], kpis["closed"], kpis["active"]
    closure_rate = (closed / total * 100) if total else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Requests", f"{total:,}")
    c2.metric("Avg Resolution Time", format_duration(kpis["avg_hours"], unit))
    c3.metric("Closed Cases", f"{closed:,}")
    c4.metric("Active Cases", f"{active:,}")
    c5.metric("Closure Rate", f"{closure_rate:.1f}%")


def render_daily_volume(daily: pd.DataFrame) -> None:
    """Time-series line chart of daily request volume with a 7-day average."""
    st.subheader("Daily Complaint Volume")
    if daily.empty:
        st.info("No requests match the selected filters.")
        return

    # Fill days with no requests with zero so the line is not misleadingly interpolated.
    full_range = pd.date_range(daily["created_day"].min(), daily["created_day"].max(), freq="D")
    daily = (
        daily.set_index("created_day")
        .reindex(full_range, fill_value=0)
        .rename_axis("created_day")
        .reset_index()
        .rename(columns={"requests": "Daily requests"})
    )
    daily["7-day average"] = daily["Daily requests"].rolling(7, min_periods=1).mean()

    fig = px.line(
        daily,
        x="created_day",
        y=["Daily requests", "7-day average"],
        labels={"created_day": "Date", "value": "Requests", "variable": ""},
    )
    fig.update_traces(selector={"name": "7-day average"}, line={"dash": "dash"})
    fig.update_layout(hovermode="x unified", legend={"orientation": "h", "y": 1.1}, margin={"t": 30})
    st.plotly_chart(fig)


def render_top_complaints(top: pd.DataFrame) -> None:
    """Horizontal bar chart of the top complaint types."""
    st.subheader("Top 10 Complaint Types")
    if top.empty:
        st.info("No complaint data for the selected filters.")
        return

    fig = px.bar(
        top,
        x="requests",
        y="complaint_type",
        orientation="h",
        labels={"requests": "Requests", "complaint_type": ""},
        text="requests",
    )
    fig.update_traces(texttemplate="%{text:,}", textposition="outside", cliponaxis=False)
    # Largest bar on top.
    fig.update_yaxes(categoryorder="total ascending")
    fig.update_layout(margin={"t": 30})
    st.plotly_chart(fig)


def render_agency_performance(selected_agencies: tuple, unit: str) -> None:
    """Fastest-resolving agencies, based on the dbt mart table."""
    st.subheader("Fastest Resolving Agencies")

    try:
        mart = load_agency_performance()
    except Exception as exc:  # noqa: BLE001 - surface any BigQuery error in the UI
        st.warning(f"Could not load `mart_agency_performance`: {exc}")
        return

    if mart.empty:
        st.info("`mart_agency_performance` returned no rows.")
        return

    # Detect the relevant columns by name so minor dbt naming differences do not break the app.
    cols = list(mart.columns)
    agency_col = find_column(cols, [("agency",)])
    time_col = find_column(
        cols,
        [("avg", "resol"), ("average", "resol"), ("mean", "resol"), ("resol",), ("avg", "time"), ("average", "time")],
    )
    volume_col = find_column(
        cols, [("total",), ("complaint",), ("request",), ("count",)], exclude={time_col, agency_col}
    )

    if agency_col is None or time_col is None:
        st.warning(
            "Could not identify the agency and average-resolution columns in "
            f"`mart_agency_performance`. Columns found: {', '.join(cols)}. "
            "Adjust the patterns in `render_agency_performance`."
        )
        st.dataframe(mart, hide_index=True)
        return

    perf = mart.copy()
    perf[agency_col] = perf[agency_col].fillna("Unspecified").astype(str).str.strip()
    perf[time_col] = pd.to_numeric(perf[time_col], errors="coerce")
    if volume_col:
        perf[volume_col] = pd.to_numeric(perf[volume_col], errors="coerce").fillna(0)

    # Respect the sidebar agency filter (the mart is pre-aggregated, so dates do not apply).
    if selected_agencies:
        perf = perf[perf[agency_col].isin(selected_agencies)]

    # Optional guard against agencies with very few complaints skewing the ranking.
    if volume_col:
        min_volume = st.number_input("Minimum complaints per agency", min_value=0, value=0, step=100)
        perf = perf[perf[volume_col] >= min_volume]

    perf = perf.dropna(subset=[time_col])
    if perf.empty:
        st.info("No agencies with resolution data match the current selection.")
        return

    # Normalize to hours, then convert to the unit chosen in the sidebar.
    perf["resolution_hours"] = perf[time_col] * hours_per_unit(time_col)
    perf["avg_resolution"] = perf["resolution_hours"] / 24 if unit == "Days" else perf["resolution_hours"]
    perf = perf.sort_values("avg_resolution").head(15)

    fig = px.bar(
        perf,
        x="avg_resolution",
        y=agency_col,
        orientation="h",
        color=volume_col,
        color_continuous_scale="Blues",
        labels={
            "avg_resolution": f"Avg resolution time ({unit.lower()})",
            agency_col: "",
            volume_col: "Total complaints" if volume_col else "",
        },
        hover_data={volume_col: ":,"} if volume_col else None,
    )
    # Fastest (smallest) bar on top.
    fig.update_yaxes(categoryorder="total descending")
    fig.update_layout(margin={"t": 30})
    st.plotly_chart(fig)
    st.caption("Source: `mart_agency_performance`. Date filter does not apply to this chart.")


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main() -> None:
    st.title("🗽 NYC 311 Service Requests")
    st.caption(f"Source: BigQuery `{PROJECT_ID}.{DATASET}` (dbt silver layer)")

    # Fail early with a clear message if the key file is missing.
    key_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    if not os.path.isfile(key_path):
        st.error(
            f"Service account key not found at `{key_path}`. "
            "Run Streamlit from the project root and make sure the file exists."
        )
        st.stop()

    try:
        min_date, max_date, agency_options = load_filter_options()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not connect to BigQuery: {exc}")
        st.stop()

    if min_date is None or max_date is None:
        st.warning("`stg_nyc_311` has no rows with a valid `created_at`.")
        st.stop()

    # ----------------------------- Sidebar filters -----------------------------
    with st.sidebar:
        st.header("Filters")

        default_start = max(min_date, max_date - timedelta(days=DEFAULT_WINDOW_DAYS))
        date_range = st.date_input(
            "Created date range",
            value=(default_start, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        selected_agencies = st.multiselect(
            "Agency",
            options=agency_options,
            placeholder="All agencies",
            help="Leave empty to include every agency.",
        )
        unit = st.radio("Resolution time unit", ["Hours", "Days"], horizontal=True)

        st.divider()
        if st.button("Refresh data", help="Clear the cache and re-query BigQuery"):
            st.cache_data.clear()
            st.rerun()

    # st.date_input returns a single date while the user is mid-selection.
    if not isinstance(date_range, (tuple, list)) or len(date_range) != 2:
        st.info("Select both a start and an end date in the sidebar to continue.")
        st.stop()
    start_date, end_date = date_range
    agencies = tuple(sorted(selected_agencies))

    # ------------------------------- Data loading ------------------------------
    try:
        kpis = load_kpis(start_date, end_date, agencies)
        daily = load_daily_volume(start_date, end_date, agencies)
        top_complaints = load_top_complaints(start_date, end_date, agencies)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Query failed: {exc}")
        st.stop()

    if kpis["total"] == 0:
        st.warning("No requests match the selected filters. Try widening the date range or agency selection.")
        st.stop()

    # -------------------------------- Dashboard --------------------------------
    render_kpis(kpis, unit)
    st.divider()

    render_daily_volume(daily)
    st.divider()

    left, right = st.columns(2)
    with left:
        render_top_complaints(top_complaints)
    with right:
        render_agency_performance(agencies, unit)


main()
