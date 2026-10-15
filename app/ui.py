from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

import requests
import streamlit as st

from app.runtime.workflow import REPORT_WORKFLOW, get_workflow

API_TIMEOUT_SECONDS = 15
TERMINAL_EVENTS = {"RunCompleted", "RunFailed", "RunCancelled"}


def _api_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _select_recent_run() -> None:
    selected = st.session_state.get("recent_run_selector")
    if selected and selected != "(choose a run)":
        st.session_state["run_id_input"] = selected


def _show_api_error(response: requests.Response) -> None:
    try:
        detail = response.json()
    except ValueError:
        detail = response.text or response.reason
    st.error(f"API returned HTTP {response.status_code}: {detail}")


def _request(
    base_url: str,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> requests.Response | None:
    try:
        response = requests.request(
            method,
            _api_url(base_url, path),
            json=body,
            params=params,
            timeout=API_TIMEOUT_SECONDS,
        )
        if not response.ok:
            _show_api_error(response)
            return None
        return response
    except requests.RequestException as exc:
        st.error(f"Could not reach the API at {base_url}: {exc}")
        return None


def _show_response(response: requests.Response, *, expanded: bool = False) -> None:
    try:
        payload = response.json()
    except ValueError:
        st.code(response.text or "(empty response)", language="text")
    else:
        st.json(payload, expanded=expanded)


def _read_sse(
    base_url: str,
    run_id: str,
    after_sequence: int,
    timeout_seconds: int,
    max_events: int,
) -> None:
    url = _api_url(base_url, f"/runs/{quote(run_id, safe='')}/stream")
    received: list[dict[str, Any]] = []
    timed_out = False
    try:
        with requests.get(
            url,
            headers={"Last-Event-ID": str(after_sequence), "Accept": "text/event-stream"},
            stream=True,
            timeout=(5, timeout_seconds),
        ) as response:
            if not response.ok:
                _show_api_error(response)
                return

            event: dict[str, str] = {"event": "message", "id": "", "data": ""}
            for raw_line in response.iter_lines(decode_unicode=True, chunk_size=1):
                line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                if not line:
                    if event["data"]:
                        try:
                            data = json.loads(event["data"])
                        except ValueError:
                            data = event["data"]
                        received.append({"event": event["event"], "id": event["id"], "data": data})
                        if event["event"] in TERMINAL_EVENTS or len(received) >= max_events:
                            break
                    event = {"event": "message", "id": "", "data": ""}
                    continue
                if line.startswith(":"):
                    continue
                field, separator, value = line.partition(":")
                if separator and value.startswith(" "):
                    value = value[1:]
                if field == "event":
                    event["event"] = value
                elif field == "id":
                    event["id"] = value
                elif field == "data":
                    event["data"] = f"{event['data']}\n{value}".strip()
    except requests.Timeout:
        timed_out = True
    except requests.RequestException as exc:
        if received:
            st.warning(f"The stream closed after delivering events: {exc}")
        else:
            st.error(f"Could not read the event stream: {exc}")
            return

    if received:
        st.dataframe(received, use_container_width=True, hide_index=True)
    else:
        st.info("No events arrived during this stream read.")
    if timed_out:
        st.warning("The stream is still open; the read timed out. Try again to continue.")
    if received:
        st.caption("Reconnect by setting Last-Event-ID to the last event sequence shown.")


def _operation_options(spec: dict[str, Any]) -> list[tuple[str, str, str, dict[str, Any]]]:
    operations = []
    for path, path_item in spec.get("paths", {}).items():
        for method, operation in path_item.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete"}:
                continue
            summary = operation.get("summary") or operation.get("operationId") or ""
            label = f"{method.upper()} {path}" + (f" - {summary}" if summary else "")
            operations.append((label, method.upper(), path, operation))
    return sorted(operations, key=lambda item: (item[2], item[1]))


def _default_request_body(path: str, method: str) -> str:
    if path == "/runs" and method == "POST":
        return json.dumps(
            {
                "task": "Prepare a weekly operations report",
                "notify_channel": "email",
                "workflow_name": REPORT_WORKFLOW,
            },
            indent=2,
        )
    return "{}"


def _render_workflow() -> None:
    st.subheader("Built-in workflow")
    st.write(
        "The runtime currently registers one workflow. Each step is driven by the durable "
        "event log, so a worker can resume after a crash without repeating recorded LLM "
        "responses or committed tool effects."
    )
    steps = get_workflow(REPORT_WORKFLOW)
    columns = st.columns(len(steps))
    descriptions = {
        "llm": "Recorded fake LLM analysis",
        "compute": "Deterministic report metrics",
        "tool": "Idempotent tool effect",
        "finalize": "Persist final run result",
    }
    for column, step in zip(columns, steps, strict=True):
        with column:
            st.markdown(
                f"""
                <div class="step-card">
                  <div class="step-kind">{step.kind.upper()}</div>
                  <div class="step-name">{step.name}</div>
                  <div class="step-id">{step.id}</div>
                  <div class="step-description">{descriptions[step.kind]}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    st.caption(
        "Analyze -> compute -> save report -> notify -> finalize. Notifications are mocked "
        "database effects; this demo does not send real messages."
    )


def main() -> None:
    st.set_page_config(
        page_title="Durable Agent Runtime",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(
        """
        <style>
        .block-container { padding-top: 2rem; padding-bottom: 3rem; }
        .step-card {
            min-height: 150px; padding: 1rem; border-radius: 12px;
            border: 1px solid #dce5ee; background: #f7fafc;
        }
        .step-kind { color: #087e8b; font-size: .72rem; font-weight: 700; letter-spacing: .08em; }
        .step-name { color: #142b3b; font-size: 1rem; font-weight: 700; margin-top: .6rem; }
        .step-id { color: #536878; font-family: monospace; font-size: .82rem; margin-top: .2rem; }
        .step-description { color: #536878; font-size: .82rem; margin-top: .6rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Durable Agent Runtime")
    st.caption("Explore replayable workflows, event history, idempotent tools, and the FastAPI.")

    with st.sidebar:
        st.header("API connection")
        base_url = (
            st.text_input(
                "FastAPI base URL",
                value="http://127.0.0.1:8000",
                help="Use the address reachable from this Streamlit process.",
            )
            .strip()
            .rstrip("/")
        )
        previous_base_url = st.session_state.get("configured_api_base_url")
        if previous_base_url is not None and previous_base_url != base_url:
            for key in (
                "openapi_spec",
                "last_run_response",
                "last_events_response",
                "last_replay_response",
                "selected_run_id",
                "recent_runs",
                "run_id_input",
                "recent_run_selector",
            ):
                st.session_state.pop(key, None)
        st.session_state["configured_api_base_url"] = base_url
        health_col, docs_col = st.columns(2)
        if health_col.button("Check health", use_container_width=True):
            response = _request(base_url, "GET", "/health")
            if response is not None:
                _show_response(response)
        docs_col.link_button("API docs", _api_url(base_url, "/docs"), use_container_width=True)
        st.caption("FastAPI schema: /openapi.json")

    tabs = st.tabs(["Workflow Lab", "Run Explorer", "API Playground", "How it works"])

    with tabs[0]:
        _render_workflow()
        st.divider()
        st.subheader("Start a run")
        with st.form("create_run_form"):
            task = st.text_area(
                "Task",
                value="Prepare a weekly operations report for the platform team.",
                max_chars=4000,
                help="The task is analyzed and used to produce a deterministic report.",
            )
            notify_channel = st.text_input("Notification channel", value="email", max_chars=64)
            submitted = st.form_submit_button("Create run", type="primary")
        if submitted:
            if not task.strip():
                st.warning("Enter a task before creating a run.")
            else:
                response = _request(
                    base_url,
                    "POST",
                    "/runs",
                    body={
                        "task": task,
                        "notify_channel": notify_channel,
                        "workflow_name": REPORT_WORKFLOW,
                    },
                )
                if response is not None:
                    payload = response.json()
                    run_id = payload["run_id"]
                    st.session_state["selected_run_id"] = run_id
                    st.session_state["run_id_input"] = run_id
                    recent = st.session_state.setdefault("recent_runs", [])
                    recent = [run_id, *[item for item in recent if item != run_id]]
                    st.session_state["recent_runs"] = recent[:10]
                    st.session_state["recent_run_selector"] = "(choose a run)"
                    st.success(f"Run created: {run_id}")
                    _show_response(response)
                    st.info(
                        "Open Run Explorer to inspect progress, events, replay, or the SSE stream."
                    )

    with tabs[1]:
        st.subheader("Inspect and operate on a run")
        default_run_id = st.session_state.get("selected_run_id", "")
        recent_runs = st.session_state.get("recent_runs", [])
        selected_recent = st.selectbox(
            "Recent runs",
            ["(choose a run)"] + recent_runs,
            key="recent_run_selector",
            on_change=_select_recent_run,
        )
        if "run_id_input" not in st.session_state:
            st.session_state["run_id_input"] = (
                default_run_id if selected_recent == "(choose a run)" else selected_recent
            )
        run_id = st.text_input("Run ID", key="run_id_input").strip()
        status_col, events_col, replay_col, retry_col = st.columns(4)
        get_clicked = status_col.button("Refresh status", key="refresh_run")
        events_clicked = events_col.button("Load events", key="load_events")
        replay_clicked = replay_col.button("Replay decisions", key="replay_run")
        retry_clicked = retry_col.button("Retry failed run", key="retry_run")

        if run_id and get_clicked:
            response = _request(base_url, "GET", f"/runs/{quote(run_id, safe='')}")
            if response is not None:
                st.session_state["last_run_response"] = response.json()
        if run_id and events_clicked:
            response = _request(base_url, "GET", f"/runs/{quote(run_id, safe='')}/events")
            if response is not None:
                st.session_state["last_events_response"] = response.json()
        if run_id and replay_clicked:
            response = _request(base_url, "POST", f"/runs/{quote(run_id, safe='')}/replay")
            if response is not None:
                st.session_state["last_replay_response"] = response.json()
        if run_id and retry_clicked:
            response = _request(base_url, "POST", f"/runs/{quote(run_id, safe='')}/retry")
            if response is not None:
                st.session_state["last_run_response"] = response.json()

        run_payload = st.session_state.get("last_run_response")
        if run_payload and run_payload.get("run_id") == run_id:
            st.markdown("#### Run status")
            status = run_payload.get("status", "unknown")
            st.metric(
                "Status",
                status.upper(),
                help="Status is reduced from the persisted event log.",
            )
            completed = run_payload.get("completed_steps", [])
            st.progress(min(len(completed) / len(get_workflow(REPORT_WORKFLOW)), 1.0))
            st.caption(
                f"Completed steps: {', '.join(completed) or 'none'} | "
                f"Current step: {run_payload.get('current_step') or 'none'} | "
                f"Event sequence: {run_payload.get('last_sequence', 0)}"
            )
            if run_payload.get("error"):
                st.error(run_payload["error"])
            if run_payload.get("result"):
                st.markdown("#### Result")
                st.json(run_payload["result"])
            with st.expander("Full run response"):
                st.json(run_payload)

        event_payload = st.session_state.get("last_events_response")
        if event_payload and event_payload[0].get("run_id") == run_id:
            st.markdown("#### Ordered event history")
            st.dataframe(event_payload, use_container_width=True, hide_index=True)
        replay_payload = st.session_state.get("last_replay_response")
        if replay_payload and replay_payload.get("run_id") == run_id:
            st.markdown("#### Replay report")
            st.json(replay_payload)

        st.markdown("#### Resumable live event stream")
        stream_col, sequence_col, timeout_col = st.columns([2, 1, 1])
        after_sequence = sequence_col.number_input(
            "Last-Event-ID",
            min_value=0,
            value=0,
            step=1,
            help="Resume after this event sequence.",
        )
        timeout_seconds = timeout_col.slider("Read timeout (seconds)", 5, 60, 30)
        max_events = st.slider("Maximum events to read", 1, 100, 30)
        if stream_col.button("Read SSE stream", key="read_sse"):
            if not run_id:
                st.warning("Enter a run ID first.")
            else:
                _read_sse(base_url, run_id, int(after_sequence), timeout_seconds, max_events)

    with tabs[2]:
        st.subheader("FastAPI endpoint playground")
        st.write(
            "Load the server's OpenAPI document to browse and call its current endpoints. "
            "Path parameters, query parameters, and JSON request bodies are editable below."
        )
        if st.button("Load / refresh endpoint catalog", type="primary"):
            response = _request(base_url, "GET", "/openapi.json")
            if response is not None:
                st.session_state["openapi_spec"] = response.json()

        spec = st.session_state.get("openapi_spec")
        if spec:
            operations = _operation_options(spec)
            if not operations:
                st.warning("The API schema contains no supported HTTP operations.")
            else:
                operation_labels = [item[0] for item in operations]
                selected_label = st.selectbox("Endpoint", operation_labels, key="api_operation")
                _, method, path, operation = next(
                    item for item in operations if item[0] == selected_label
                )
                if operation.get("description"):
                    st.caption(operation["description"])
                path_values: dict[str, str] = {}
                for parameter in operation.get("parameters", []):
                    if parameter.get("in") == "path":
                        path_values[parameter["name"]] = st.text_input(
                            f"Path parameter: {parameter['name']}",
                            value=st.session_state.get("selected_run_id", ""),
                            key=f"path_{path}_{parameter['name']}",
                        )
                query_text = st.text_area(
                    "Query parameters (JSON object)",
                    value="{}",
                    key=f"query_{method}_{path}",
                )
                request_body = operation.get("requestBody", {})
                body_text = "{}"
                if request_body:
                    body_schema = (
                        request_body.get("content", {})
                        .get("application/json", {})
                        .get("schema", {})
                    )
                    st.caption("Request JSON schema: " + json.dumps(body_schema, ensure_ascii=True))
                    body_text = st.text_area(
                        "JSON request body",
                        value=_default_request_body(path, method),
                        height=180,
                        key=f"body_{method}_{path}",
                    )
                if st.button("Send request", key=f"send_{method}_{path}"):
                    try:
                        query = json.loads(query_text)
                        if not isinstance(query, dict):
                            raise ValueError("Query parameters must be a JSON object.")
                        body = json.loads(body_text) if request_body else None
                        if body is not None and not isinstance(body, dict):
                            raise ValueError("The JSON request body must be an object.")
                    except ValueError as exc:
                        st.error(f"Invalid request input: {exc}")
                    else:
                        resolved_path = path
                        for name, value in path_values.items():
                            resolved_path = resolved_path.replace(
                                f"{{{name}}}", quote(value, safe="")
                            )
                        if "{" in resolved_path:
                            st.error(f"Fill in all path parameters for {path}.")
                        elif resolved_path.endswith("/stream"):
                            run_match = resolved_path.split("/")
                            stream_run_id = run_match[-2]
                            try:
                                after = int(query.get("last_event_id", 0))
                            except (TypeError, ValueError):
                                st.error("last_event_id must be an integer.")
                            else:
                                _read_sse(base_url, stream_run_id, after, 30, 30)
                        else:
                            response = _request(
                                base_url,
                                method,
                                resolved_path,
                                body=body,
                                params=query,
                            )
                            if response is not None:
                                st.caption(
                                    f"HTTP {response.status_code} - "
                                    f"{response.headers.get('content-type', '')}"
                                )
                                _show_response(response, expanded=True)
        else:
            st.info(
                "Load the endpoint catalog to begin. FastAPI Swagger and ReDoc are also available."
            )
        st.markdown(
            f"[Swagger UI]({_api_url(base_url, '/docs')}) | "
            f"[ReDoc]({_api_url(base_url, '/redoc')}) | "
            f"[OpenAPI JSON]({_api_url(base_url, '/openapi.json')})"
        )

    with tabs[3]:
        st.subheader("Runtime concepts")
        st.markdown(
            """
            - **Event sourcing:** state is reduced from the ordered, append-only event log.
            - **Crash recovery:** workers lease runs and resume from committed events after a
              crash.
            - **Idempotent tools:** stable keys prevent repeating committed tool effects.
            - **Recorded model calls:** the deterministic fake LLM response is persisted and reused
              during recovery and replay.
            - **Replay:** recorded decisions are reconstructed without invoking tools or the LLM.
            - **Snapshots:** periodic state snapshots bound event-reconstruction work.
            - **Retry:** failed runs can be explicitly re-queued through the retry endpoint.

            Report-saving and notification tools persist **mock effects in MySQL**. They do not
            write files or contact external notification services.
            """
        )
        st.markdown("#### Endpoint map")
        st.table(
            [
                {
                    "Method": "GET",
                    "Path": "/health",
                    "Purpose": "Liveness and database connectivity",
                },
                {"Method": "POST", "Path": "/runs", "Purpose": "Create a report workflow run"},
                {"Method": "GET", "Path": "/runs/{run_id}", "Purpose": "Read reduced run state"},
                {
                    "Method": "GET",
                    "Path": "/runs/{run_id}/events",
                    "Purpose": "Read ordered event history",
                },
                {
                    "Method": "POST",
                    "Path": "/runs/{run_id}/retry",
                    "Purpose": "Retry a failed run",
                },
                {
                    "Method": "POST",
                    "Path": "/runs/{run_id}/replay",
                    "Purpose": "Replay recorded decisions",
                },
                {
                    "Method": "GET",
                    "Path": "/runs/{run_id}/stream",
                    "Purpose": "Resumable SSE event stream",
                },
            ]
        )


if __name__ == "__main__":
    main()
