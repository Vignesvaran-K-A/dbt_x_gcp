import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="NYC 311 Pipeline Control Center",
    page_icon=":satellite:",
    layout="wide",
)

GITHUB_API = "https://api.github.com"
GITHUB_API_VERSION = "2022-11-28"
REQUEST_TIMEOUT = 15
DEFAULT_ARTIFACT_DIR = os.getenv("DBT_TARGET_DIR", "target")
STALE_AFTER_SECONDS = 24 * 3600

SUCCESS_STATUSES = {"success", "pass"}
WARNING_STATUSES = {"warn", "partial success"}
FAILURE_STATUSES = {"error", "fail"}
NO_REQUEST_STATUSES = {"skipped", "no-op"}

STATUS_COLORS = {
    "success": "#2e9e5b",
    "pass": "#2e9e5b",
    "warn": "#e0a526",
    "partial success": "#e0a526",
    "error": "#d64545",
    "fail": "#d64545",
    "skipped": "#8a94a6",
    "no-op": "#8a94a6",
}

HTTP_CLASS_COLORS = {
    "2xx Success": "#2e9e5b",
    "4xx Client Error": "#e0a526",
    "5xx Server Error": "#d64545",
    "Not sent": "#8a94a6",
}

HTTP_LABELS = {
    200: "200 OK",
    400: "400 Bad Request",
    401: "401 Unauthorized",
    403: "403 Forbidden",
    404: "404 Not Found",
    409: "409 Conflict",
    429: "429 Too Many Requests",
    500: "500 Internal Error",
    503: "503 Unavailable",
    504: "504 Gateway Timeout",
}

EXPLICIT_HTTP_PATTERN = re.compile(
    r"\b([45]\d{2})\s+(?:GET|POST|PUT|PATCH|DELETE)\s+https?://",
    re.IGNORECASE,
)

HTTP_ERROR_PATTERNS = [
    (re.compile(r"unauthorized|invalid_grant|could not authenticate|invalid credentials", re.I), 401),
    (re.compile(r"access denied|permission|forbidden", re.I), 403),
    (re.compile(r"not found|does not exist", re.I), 404),
    (re.compile(r"already exists", re.I), 409),
    (re.compile(r"quota|rate ?limit|too many", re.I), 429),
    (re.compile(r"syntax error|unrecognized name|invalid query|invalidquery|bad request", re.I), 400),
    (re.compile(r"deadline|timed ?out|timeout", re.I), 504),
    (re.compile(r"unavailable|backend ?error", re.I), 503),
]


@dataclass(frozen=True)
class GitHubConfig:
    token: str
    owner: str
    repo: str
    workflow: str
    ref: str

    @property
    def missing(self):
        required = (
            ("GITHUB_TOKEN", self.token),
            ("GITHUB_OWNER", self.owner),
            ("GITHUB_REPO", self.repo),
        )
        return [name for name, value in required if not value]


def load_github_config():
    return GitHubConfig(
        token=os.getenv("GITHUB_TOKEN", "").strip(),
        owner=os.getenv("GITHUB_OWNER", "").strip(),
        repo=os.getenv("GITHUB_REPO", "").strip(),
        workflow=os.getenv("GITHUB_WORKFLOW_FILE", "pipeline.yml").strip(),
        ref=os.getenv("GITHUB_DEFAULT_BRANCH", "main").strip(),
    )


def format_bytes(value):
    size = float(value or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{int(size)} B" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1024


def format_duration(seconds):
    seconds = float(seconds or 0)
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, remainder = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m {remainder}s"


def style_figure(fig, height=360):
    fig.update_layout(
        height=height,
        margin={"l": 10, "r": 10, "t": 50, "b": 10},
        legend_title_text="",
    )
    return fig


@st.cache_data(show_spinner=False)
def load_json(path_str, modified_time):
    with open(path_str, "r", encoding="utf-8") as handle:
        return json.load(handle)


def read_artifact(directory, filename):
    path = Path(directory).expanduser() / filename
    if not path.is_file():
        return None
    try:
        return load_json(str(path), path.stat().st_mtime)
    except (OSError, json.JSONDecodeError) as exc:
        st.error(f"Could not parse {path}: {exc}")
        return None


def span_seconds(entry):
    if not entry:
        return 0.0
    start = pd.to_datetime(entry.get("started_at"), utc=True, errors="coerce")
    end = pd.to_datetime(entry.get("completed_at"), utc=True, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return 0.0
    return (end - start).total_seconds()


def infer_http_status(status, resource_type, message):
    if status in NO_REQUEST_STATUSES:
        return None
    if status in SUCCESS_STATUSES or status in WARNING_STATUSES:
        return 200
    if resource_type == "test" and status == "fail":
        return 200
    explicit = EXPLICIT_HTTP_PATTERN.search(message)
    if explicit:
        return int(explicit.group(1))
    for pattern, code in HTTP_ERROR_PATTERNS:
        if pattern.search(message):
            return code
    return 500


def classify_http(code):
    if code is None:
        return "Not sent"
    if code < 300:
        return "2xx Success"
    if code < 500:
        return "4xx Client Error"
    return "5xx Server Error"


def build_results_frame(run_results, manifest):
    nodes = (manifest or {}).get("nodes", {})
    rows = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        node = nodes.get(unique_id, {})
        adapter = result.get("adapter_response") or {}
        timing = {entry.get("name"): entry for entry in result.get("timing", [])}
        execute = timing.get("execute", {})
        status = str(result.get("status", "unknown")).lower()
        resource_type = node.get("resource_type") or unique_id.split(".")[0] or "unknown"
        message = str(result.get("message") or adapter.get("_message") or "")
        http_status = infer_http_status(status, resource_type, message)
        rows.append(
            {
                "unique_id": unique_id,
                "name": node.get("name") or unique_id.split(".")[-1] or "unknown",
                "resource_type": resource_type,
                "materialization": (node.get("config") or {}).get("materialized") or "n/a",
                "schema": node.get("schema") or "",
                "path": node.get("original_file_path") or "",
                "status": status,
                "http_status": http_status,
                "http_class": classify_http(http_status),
                "execution_time": result.get("execution_time") or 0.0,
                "compile_seconds": span_seconds(timing.get("compile")),
                "execute_seconds": span_seconds(execute),
                "execute_started": execute.get("started_at"),
                "execute_completed": execute.get("completed_at"),
                "thread_id": result.get("thread_id") or "n/a",
                "rows_affected": adapter.get("rows_affected"),
                "bytes_processed": adapter.get("bytes_processed"),
                "bytes_billed": adapter.get("bytes_billed"),
                "slot_ms": adapter.get("slot_ms"),
                "job_id": adapter.get("job_id"),
                "location": adapter.get("location"),
                "failures": result.get("failures"),
                "message": message,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ("rows_affected", "bytes_processed", "bytes_billed", "slot_ms", "failures"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["http_status"] = pd.to_numeric(frame["http_status"], errors="coerce").astype("Int64")
    frame["http_label"] = frame["http_status"].map(
        lambda code: "No request" if pd.isna(code) else HTTP_LABELS.get(int(code), str(int(code)))
    )
    frame["processed_mib"] = (frame["bytes_processed"] / 1024**2).round(2)
    frame["billed_mib"] = (frame["bytes_billed"] / 1024**2).round(2)
    frame["execute_started"] = pd.to_datetime(frame["execute_started"], utc=True, errors="coerce")
    frame["execute_completed"] = pd.to_datetime(frame["execute_completed"], utc=True, errors="coerce")
    return frame


def compute_health(frame):
    attempted = frame[~frame["status"].isin(NO_REQUEST_STATUSES)]
    total = len(attempted)
    succeeded = int(attempted["status"].isin(SUCCESS_STATUSES).sum())
    warned = int(attempted["status"].isin(WARNING_STATUSES).sum())
    failed = int(attempted["status"].isin(FAILURE_STATUSES).sum())
    return {
        "attempted": total,
        "succeeded": succeeded,
        "warned": warned,
        "failed": failed,
        "skipped": len(frame) - total,
        "success_rate": succeeded / total * 100 if total else 0.0,
        "failure_rate": failed / total * 100 if total else 0.0,
    }


def render_run_context(run_results, manifest):
    metadata = run_results.get("metadata", {})
    args = run_results.get("args", {})
    manifest_meta = (manifest or {}).get("metadata", {})
    parsed = pd.to_datetime(metadata.get("generated_at"), utc=True, errors="coerce")
    age_label = "unknown"
    if not pd.isna(parsed):
        age_seconds = (datetime.now(timezone.utc) - parsed.to_pydatetime()).total_seconds()
        age_label = f"{format_duration(age_seconds)} ago"
        if age_seconds > STALE_AFTER_SECONDS:
            st.warning("The loaded dbt artifacts are more than 24 hours old.")
    details = [
        f"dbt {metadata.get('dbt_version', 'unknown')}",
        f"adapter {manifest_meta.get('adapter_type', 'bigquery')}",
        f"command dbt {args.get('which', 'run')}",
        f"last run {age_label}",
        f"invocation {str(metadata.get('invocation_id', 'n/a'))[:8]}",
    ]
    st.caption(" | ".join(details))


def render_filters(frame):
    with st.sidebar:
        st.subheader("Filters")
        resource_types = sorted(frame["resource_type"].unique())
        statuses = sorted(frame["status"].unique())
        selected_types = st.multiselect("Resource types", resource_types, default=resource_types)
        selected_statuses = st.multiselect("Statuses", statuses, default=statuses)
    mask = frame["resource_type"].isin(selected_types) & frame["status"].isin(selected_statuses)
    return frame[mask].reset_index(drop=True)


def render_overview(frame, run_results):
    health = compute_health(frame)
    if health["failed"]:
        st.error(f"{health['failed']} of {health['attempted']} executed nodes failed in the latest run.")
    elif health["warned"]:
        st.warning(f"The latest run finished with {health['warned']} warning(s).")
    else:
        st.success("All executed nodes completed successfully.")

    elapsed = run_results.get("elapsed_time") or frame["execution_time"].sum()
    columns = st.columns(6)
    columns[0].metric("Nodes executed", health["attempted"])
    columns[1].metric("Success rate", f"{health['success_rate']:.1f}%")
    columns[2].metric("Failure rate", f"{health['failure_rate']:.1f}%")
    columns[3].metric("Skipped", health["skipped"])
    columns[4].metric("Wall-clock", format_duration(elapsed))
    columns[5].metric("BigQuery scanned", format_bytes(frame["bytes_processed"].sum()))

    left, right = st.columns(2)
    with left:
        counts = frame["status"].value_counts().rename_axis("status").reset_index(name="count")
        fig = px.pie(
            counts,
            names="status",
            values="count",
            hole=0.55,
            color="status",
            color_discrete_map=STATUS_COLORS,
            title="Status distribution",
        )
        st.plotly_chart(style_figure(fig))
    with right:
        grouped = frame.groupby(["resource_type", "status"]).size().reset_index(name="count")
        fig = px.bar(
            grouped,
            x="resource_type",
            y="count",
            color="status",
            color_discrete_map=STATUS_COLORS,
            title="Outcomes by resource type",
        )
        st.plotly_chart(style_figure(fig))

    problems = frame[frame["status"].isin(FAILURE_STATUSES | WARNING_STATUSES)]
    if not problems.empty:
        st.subheader("Failures and warnings")
        for row in problems.itertuples():
            with st.expander(f"{row.name} | {row.status.upper()} | {row.http_label}"):
                st.caption(row.path or row.unique_id)
                st.code(row.message or "No message recorded.", language="text")


def render_bigquery(frame):
    st.caption(
        "dbt does not persist raw HTTP codes in run_results.json. Codes are derived from node "
        "status and the BigQuery error payload: successful jobs map to 200, errors are matched "
        "to their HTTP equivalent, and skipped nodes issued no request."
    )
    sent = frame[frame["http_class"] != "Not sent"]
    counts = frame["http_class"].value_counts()
    columns = st.columns(5)
    columns[0].metric("API transactions", len(sent))
    columns[1].metric("2xx success", int(counts.get("2xx Success", 0)))
    columns[2].metric("4xx client errors", int(counts.get("4xx Client Error", 0)))
    columns[3].metric("5xx server errors", int(counts.get("5xx Server Error", 0)))
    columns[4].metric("Not sent", int(counts.get("Not sent", 0)))

    if sent.empty:
        st.info("No BigQuery transactions were recorded for the current selection.")
        return

    left, right = st.columns(2)
    with left:
        grouped = sent.groupby(["http_label", "http_class"]).size().reset_index(name="count")
        fig = px.bar(
            grouped,
            x="http_label",
            y="count",
            color="http_class",
            color_discrete_map=HTTP_CLASS_COLORS,
            title="Transactions by HTTP status",
        )
        st.plotly_chart(style_figure(fig))
    with right:
        scanned = frame.dropna(subset=["bytes_processed"]).nlargest(15, "bytes_processed")
        if scanned.empty:
            st.info("No bytes-processed metadata found in adapter responses.")
        else:
            fig = px.bar(
                scanned,
                x="processed_mib",
                y="name",
                orientation="h",
                color="status",
                color_discrete_map=STATUS_COLORS,
                title="Top models by data scanned (MiB)",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(style_figure(fig))

    st.subheader("Transaction log")
    st.dataframe(
        sent[
            [
                "name",
                "resource_type",
                "status",
                "http_label",
                "job_id",
                "location",
                "rows_affected",
                "processed_mib",
                "billed_mib",
                "slot_ms",
                "execute_seconds",
            ]
        ],
        hide_index=True,
        column_config={
            "name": "Node",
            "resource_type": "Type",
            "status": "dbt status",
            "http_label": "HTTP status",
            "job_id": "BigQuery job",
            "location": "Location",
            "rows_affected": st.column_config.NumberColumn("Rows", format="%d"),
            "processed_mib": st.column_config.NumberColumn("Processed (MiB)", format="%.2f"),
            "billed_mib": st.column_config.NumberColumn("Billed (MiB)", format="%.2f"),
            "slot_ms": st.column_config.NumberColumn("Slot ms", format="%d"),
            "execute_seconds": st.column_config.NumberColumn("Execute (s)", format="%.2f"),
        },
    )


def render_timing(frame):
    executed = frame[~frame["status"].isin(NO_REQUEST_STATUSES)]
    if executed.empty:
        st.info("No executed nodes in the current selection.")
        return

    times = executed["execution_time"]
    slowest = executed.loc[times.idxmax()]
    columns = st.columns(5)
    columns[0].metric("Total execution", format_duration(times.sum()))
    columns[1].metric("Mean", format_duration(times.mean()))
    columns[2].metric("Median", format_duration(times.median()))
    columns[3].metric("P95", format_duration(times.quantile(0.95)))
    columns[4].metric("Slowest", format_duration(times.max()), help=slowest["name"])

    top_n = st.slider("Models to display", 5, 50, min(15, max(5, len(executed))))
    top = executed.nlargest(top_n, "execution_time")

    left, right = st.columns(2)
    with left:
        fig = px.bar(
            top,
            x="execution_time",
            y="name",
            orientation="h",
            color="status",
            color_discrete_map=STATUS_COLORS,
            title="Slowest nodes (seconds)",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(style_figure(fig, 420))
    with right:
        phases = top.melt(
            id_vars="name",
            value_vars=["compile_seconds", "execute_seconds"],
            var_name="phase",
            value_name="seconds",
        )
        phases["phase"] = phases["phase"].str.replace("_seconds", "", regex=False)
        fig = px.bar(
            phases,
            x="seconds",
            y="name",
            color="phase",
            orientation="h",
            title="Compile versus execute time",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(style_figure(fig, 420))

    timeline = executed.dropna(subset=["execute_started", "execute_completed"])
    if not timeline.empty:
        fig = px.timeline(
            timeline,
            x_start="execute_started",
            x_end="execute_completed",
            y="thread_id",
            color="status",
            hover_name="name",
            color_discrete_map=STATUS_COLORS,
            title="Execution timeline by worker thread",
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(style_figure(fig, 320))


def render_details(frame):
    columns = [
        "name",
        "resource_type",
        "materialization",
        "schema",
        "status",
        "http_label",
        "execution_time",
        "rows_affected",
        "processed_mib",
        "thread_id",
        "path",
        "message",
    ]
    st.dataframe(
        frame[columns],
        hide_index=True,
        column_config={
            "execution_time": st.column_config.NumberColumn("Execution (s)", format="%.2f"),
            "rows_affected": st.column_config.NumberColumn("Rows", format="%d"),
            "processed_mib": st.column_config.NumberColumn("Processed (MiB)", format="%.2f"),
        },
    )
    st.download_button(
        "Download CSV",
        data=frame.drop(columns=["execute_started", "execute_completed"]).to_csv(index=False),
        file_name="dbt_run_results.csv",
        mime="text/csv",
    )


def github_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }


def describe_github_error(response):
    hints = {
        401: "Token rejected. Check GITHUB_TOKEN.",
        403: "Token lacks permission. Classic tokens need the workflow scope; fine-grained tokens need Actions read and write.",
        404: "Repository or workflow file not found, or the token has no access to it.",
        422: "GitHub rejected the request. Verify the ref exists and the workflow defines workflow_dispatch with matching inputs.",
    }
    try:
        detail = response.json().get("message", "")
    except ValueError:
        detail = response.text[:200]
    hint = hints.get(response.status_code, "Unexpected response from GitHub.")
    return f"HTTP {response.status_code}: {hint} {detail}".strip()


def trigger_workflow(config, ref, inputs):
    url = f"{GITHUB_API}/repos/{config.owner}/{config.repo}/actions/workflows/{config.workflow}/dispatches"
    try:
        response = requests.post(
            url,
            headers=github_headers(config.token),
            json={"ref": ref, "inputs": inputs},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        return False, f"Network error while contacting GitHub: {exc}"
    if response.status_code == 204:
        return True, f"Workflow dispatch accepted for {config.owner}/{config.repo} on {ref}."
    return False, describe_github_error(response)


@st.cache_data(ttl=15, show_spinner=False)
def fetch_recent_runs(config, limit=10):
    url = f"{GITHUB_API}/repos/{config.owner}/{config.repo}/actions/workflows/{config.workflow}/runs"
    try:
        response = requests.get(
            url,
            headers=github_headers(config.token),
            params={"per_page": limit},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        return None, f"Network error while contacting GitHub: {exc}"
    if response.status_code != 200:
        return None, describe_github_error(response)
    runs = response.json().get("workflow_runs", [])
    frame = pd.DataFrame(
        [
            {
                "run": f"#{run['run_number']}",
                "status": run.get("status"),
                "conclusion": run.get("conclusion") or "in progress",
                "branch": run.get("head_branch"),
                "event": run.get("event"),
                "started": pd.to_datetime(run.get("created_at"), utc=True, errors="coerce"),
                "link": run.get("html_url"),
            }
            for run in runs
        ]
    )
    return frame, None


def render_control_center(config):
    st.subheader("GitHub Actions control panel")
    info_column, action_column = st.columns(2)

    with info_column:
        st.markdown(f"**Repository** `{config.owner or 'not set'}/{config.repo or 'not set'}`")
        st.markdown(f"**Workflow** `{config.workflow}`")
        st.markdown(f"**Access token** {'loaded from .env' if config.token else 'missing'}")
        if config.missing:
            st.error("Missing in .env: " + ", ".join(config.missing))

    with action_column:
        ref = st.text_input("Branch or tag", value=config.ref)
        dbt_target = st.selectbox("dbt target", ["dev", "prod"])
        dbt_select = st.text_input("dbt --select (optional)", placeholder="tag:nyc311+")
        full_refresh = st.checkbox("Full refresh incremental models")
        confirmed = True
        if dbt_target == "prod":
            confirmed = st.checkbox("Confirm this run targets production")
        if st.button("Trigger Pipeline", type="primary", disabled=bool(config.missing) or not confirmed):
            with st.spinner("Dispatching workflow..."):
                succeeded, detail = trigger_workflow(
                    config,
                    ref.strip() or config.ref,
                    {
                        "dbt_target": dbt_target,
                        "dbt_select": dbt_select.strip(),
                        "full_refresh": str(full_refresh).lower(),
                    },
                )
            if succeeded:
                fetch_recent_runs.clear()
                st.success(detail)
            else:
                st.error(detail)

    st.divider()
    st.subheader("Recent workflow runs")
    if config.missing:
        st.info("Add the missing .env values to see workflow history.")
        return
    if st.button("Refresh runs"):
        fetch_recent_runs.clear()
    runs, error = fetch_recent_runs(config)
    if error:
        st.error(error)
    elif runs is None or runs.empty:
        st.info("No runs found for this workflow yet.")
    else:
        st.dataframe(
            runs,
            hide_index=True,
            column_config={
                "run": "Run",
                "status": "Status",
                "conclusion": "Conclusion",
                "branch": "Branch",
                "event": "Event",
                "started": st.column_config.DatetimeColumn("Started", format="YYYY-MM-DD HH:mm"),
                "link": st.column_config.LinkColumn("Link", display_text="Open"),
            },
        )


def main():
    st.title("NYC 311 ELT Observability and CI/CD Control Center")
    st.caption("NYC 311 API → Docker → Google BigQuery → dbt")

    with st.sidebar:
        st.header("dbt artifacts")
        artifact_dir = st.text_input("Target directory", value=DEFAULT_ARTIFACT_DIR)
        st.button("Reload artifacts")

    run_results = read_artifact(artifact_dir, "run_results.json")
    manifest = read_artifact(artifact_dir, "manifest.json")
    if manifest is None:
        st.sidebar.warning("manifest.json not found. Materialization and schema details are unavailable.")

    tabs = st.tabs(
        ["Pipeline Health", "BigQuery API", "Execution Times", "Run Details", "CI/CD Control Center"]
    )

    with tabs[4]:
        render_control_center(load_github_config())

    if run_results is None:
        for tab in tabs[:4]:
            with tab:
                st.warning(f"run_results.json was not found in `{artifact_dir}`. Run dbt or download the workflow artifact first.")
        return

    frame = build_results_frame(run_results, manifest)
    if frame.empty:
        for tab in tabs[:4]:
            with tab:
                st.info("run_results.json contains no node results.")
        return

    filtered = render_filters(frame)
    with tabs[0]:
        render_run_context(run_results, manifest)
        if filtered.empty:
            st.info("No nodes match the current filters.")
        else:
            render_overview(filtered, run_results)
    for tab, renderer in zip(tabs[1:4], (render_bigquery, render_timing, render_details)):
        with tab:
            if filtered.empty:
                st.info("No nodes match the current filters.")
            else:
                renderer(filtered)


if __name__ == "__main__":
    main()
