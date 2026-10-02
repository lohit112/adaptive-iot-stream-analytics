"""
Unified stdlib HTTP API server and static frontend file server.

Provides:
- POST /api/upload
- POST /api/run (and POST /api/start)
- GET /api/status
- GET /api/results
- GET /api/profile
- GET /api/ml
- GET /api/correctness
- GET /api/export
- GET /api/system
- Static file serving for the frontend dashboard
"""

import json
import mimetypes
import os
import threading
import time
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from src.bda.ml.detector import StreamAnomalyDetector
from src.bda.pipeline.ingest import parse_csv_stream, profile_dataset
from src.bda.pipeline.stream import StreamPipeline, check_kafka_availability
from src.bda.bda_runtime import get_bda_infrastructure_status


FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent.parent / "frontend"
DEMO_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "demo"


class ProcessingState:
    """Thread-safe state container for dataset, active run, and results."""

    def __init__(self):
        self._lock = threading.Lock()
        self.filename: str = ""
        self.events: List[Dict[str, Any]] = []
        self.profile: Optional[Dict[str, Any]] = None

        # Job state
        self.status: str = "idle"  # idle, running, completed, error
        self.mode: str = "full"
        self.transport: str = "in_process"
        self.transport_label: str = "In-process demo fallback"
        self.progress: Dict[str, Any] = {
            "received": 0,
            "total": 0,
            "pct": 0.0,
            "throughput": 0.0,
            "active_state": 0,
            "finalized_state": 0,
            "frozen_emissions": 0,
            "reconciled_events": 0,
            "current_watermark": None,
            "current_event_time": None,
            "peak_active": 0,
        }
        self.error_message: Optional[str] = None
        self.results: Optional[Dict[str, Any]] = None
        self.ml_results: Optional[Dict[str, Any]] = None
        self.comparison_matrix: Dict[str, Any] = {}

    def set_dataset(
        self, filename: str, events: List[Dict[str, Any]], profile: Dict[str, Any]
    ):
        with self._lock:
            self.filename = filename
            self.events = events
            self.profile = profile
            self.status = "idle"
            self.error_message = None
            self.results = None
            self.ml_results = None
            self.comparison_matrix = {}
            self.progress = {
                "received": 0,
                "total": len(events),
                "pct": 0.0,
                "throughput": 0.0,
                "active_state": 0,
                "finalized_state": 0,
                "frozen_emissions": 0,
                "reconciled_events": 0,
                "current_watermark": None,
                "current_event_time": None,
                "peak_active": 0,
            }

    def start_job(self, mode: str):
        with self._lock:
            if self.status == "running":
                raise RuntimeError("A processing run is already active")
            if not self.events:
                raise RuntimeError("No dataset loaded. Upload a CSV first.")
            self.status = "running"
            self.mode = mode
            self.error_message = None
            self.results = None
            self.ml_results = None
            self.progress["received"] = 0
            self.progress["total"] = len(self.events)
            self.progress["pct"] = 0.0

    def update_progress(self, current: int, total: int, metrics: Dict[str, Any]):
        with self._lock:
            pct = round((current / total) * 100.0, 1) if total else 0.0
            self.progress.update(metrics)
            self.progress["pct"] = pct

    def finish_job(
        self,
        results: Dict[str, Any],
        ml_results: Dict[str, Any],
        comparison_matrix: Dict[str, Any],
        transport: str,
        transport_label: str,
    ):
        with self._lock:
            self.status = "completed"
            self.results = results
            self.ml_results = ml_results
            self.comparison_matrix = comparison_matrix
            self.transport = transport
            self.transport_label = transport_label
            self.progress["pct"] = 100.0

    def fail_job(self, error_message: str):
        with self._lock:
            self.status = "error"
            self.error_message = error_message

    def get_status_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "status": self.status,
                "mode": self.mode,
                "dataset": self.filename,
                "progress": dict(self.progress),
                "error": self.error_message,
                "has_results": self.results is not None,
                "has_ml": self.ml_results is not None,
            }


GLOBAL_STATE = ProcessingState()


def _run_processing_worker(mode: str):
    """Background worker executing the stream processing pipeline."""
    try:
        events = list(GLOBAL_STATE.events)
        total = len(events)

        def progress_cb(cur, tot, m):
            GLOBAL_STATE.update_progress(cur, tot, m)

        pipeline = StreamPipeline(mode=mode)
        transport = pipeline.transport
        transport_label = pipeline.transport_label

        # Run primary mode
        main_run = pipeline.run(events, progress_callback=progress_cb)

        # Run Anomaly Detector
        detector = StreamAnomalyDetector()
        ml_res = detector.detect(main_run["result_rows"])

        # Also generate comparison matrix for CMiX, ALOA, MASO, Full
        matrix = {}
        modes_to_run = ["cmix", "aloa", "maso", "full"]
        for m in modes_to_run:
            if m == mode:
                matrix[m] = main_run["metrics"]
            else:
                p = StreamPipeline(mode=m)
                r = p.run(events)
                matrix[m] = r["metrics"]

        main_run["bda_infrastructure"] = get_bda_infrastructure_status()

        GLOBAL_STATE.finish_job(
            results=main_run,
            ml_results=ml_res,
            comparison_matrix=matrix,
            transport=transport,
            transport_label=transport_label,
        )
    except Exception as e:
        GLOBAL_STATE.fail_job(str(e))


class BDAApiHandler(SimpleHTTPRequestHandler):
    """HTTP request handler supporting stdlib API endpoints and static frontend."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

    def log_message(self, format, *args):
        # Concise logging
        if not os.environ.get("BDA_QUIET"):
            super().log_message(format, *args)

    def _send_json(self, data: Any, status: int = HTTPStatus.OK):
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header(
            "Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE"
        )
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, message: str, status: int = HTTPStatus.BAD_REQUEST):
        self._send_json({"error": message, "status": "error"}, status=status)

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header(
            "Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE"
        )
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        # API Endpoints
        if path == "/api/status":
            self._send_json(GLOBAL_STATE.get_status_dict())
            return

        if path == "/api/profile":
            if GLOBAL_STATE.profile is None:
                self._send_error_json(
                    "No dataset currently profiled", HTTPStatus.NOT_FOUND
                )
                return
            self._send_json(
                {
                    "filename": GLOBAL_STATE.filename,
                    "profile": GLOBAL_STATE.profile,
                }
            )
            return

        if path == "/api/results":
            if GLOBAL_STATE.results is None:
                self._send_error_json(
                    "No run results available", HTTPStatus.NOT_FOUND
                )
                return
            res = dict(GLOBAL_STATE.results)
            res["comparison_matrix"] = GLOBAL_STATE.comparison_matrix
            res["filename"] = GLOBAL_STATE.filename
            self._send_json(res)
            return

        if path == "/api/ml":
            if GLOBAL_STATE.ml_results is None:
                self._send_error_json(
                    "No ML anomaly results available", HTTPStatus.NOT_FOUND
                )
                return
            self._send_json(GLOBAL_STATE.ml_results)
            return

        if path == "/api/correctness":
            if (
                GLOBAL_STATE.results is None
                or "correctness" not in GLOBAL_STATE.results
            ):
                self._send_error_json(
                    "No correctness report available", HTTPStatus.NOT_FOUND
                )
                return
            self._send_json(GLOBAL_STATE.results["correctness"])
            return

        if path == "/api/system":
            kafka_ok = check_kafka_availability()
            bda_infra = get_bda_infrastructure_status()
            resp = {
                "kafka_available": kafka_ok,
                "transport_label": "Kafka (localhost:9092)"
                if kafka_ok
                else "In-process demo fallback",
                "dataset_loaded": bool(GLOBAL_STATE.events),
                "dataset_name": GLOBAL_STATE.filename,
            }
            resp.update(bda_infra)
            self._send_json(resp)
            return

        if path == "/api/export":
            if GLOBAL_STATE.results is None:
                self._send_error_json(
                    "No results to export", HTTPStatus.NOT_FOUND
                )
                return
            export_payload = {
                "dataset": GLOBAL_STATE.filename,
                "profile": GLOBAL_STATE.profile,
                "metrics": GLOBAL_STATE.results.get("metrics"),
                "correctness": GLOBAL_STATE.results.get("correctness"),
                "comparison_matrix": GLOBAL_STATE.comparison_matrix,
                "ml": GLOBAL_STATE.ml_results,
                "result_rows_count": len(
                    GLOBAL_STATE.results.get("result_rows", [])
                ),
            }
            self._send_json(export_payload)
            return

        # Serve static frontend files
        if path == "/" or path == "/index.html":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        content_type = self.headers.get("Content-Type", "")

        body_bytes = self.rfile.read(content_length) if content_length > 0 else b""

        # POST /api/upload
        if path == "/api/upload":
            filename = "uploaded.csv"
            csv_text = ""

            if "application/json" in content_type:
                try:
                    payload = json.loads(body_bytes.decode("utf-8"))
                    # Support pre-packaged demo samples via sample key
                    if "sample" in payload:
                        sample_name = payload["sample"]
                        sample_file = DEMO_DATA_DIR / sample_name
                        if not sample_file.exists():
                            self._send_error_json(
                                f"Demo sample '{sample_name}' not found",
                                HTTPStatus.NOT_FOUND,
                            )
                            return
                        filename = sample_name
                        with open(sample_file, "r", encoding="utf-8") as f:
                            csv_text = f.read()
                    elif "csv" in payload:
                        csv_text = payload["csv"]
                        filename = payload.get("filename", "uploaded.csv")
                    else:
                        self._send_error_json(
                            "JSON body must contain 'csv' or 'sample'"
                        )
                        return
                except Exception as e:
                    self._send_error_json(f"Malformed JSON: {e}")
                    return

            elif "multipart/form-data" in content_type:
                # Simple extraction of multipart form data file
                try:
                    # Find boundary
                    boundary = content_type.split("boundary=")[1].encode()
                    parts = body_bytes.split(b"--" + boundary)
                    for part in parts:
                        if b'Content-Disposition: form-data;' in part and b'filename="' in part:
                            # Extract filename
                            header_part, content_part = part.split(b"\r\n\r\n", 1)
                            for line in header_part.split(b"\r\n"):
                                if b'filename="' in line:
                                    fn_part = line.split(b'filename="')[1]
                                    filename = fn_part.split(b'"')[0].decode(
                                        "utf-8", errors="replace"
                                    )
                            # Remove trailing \r\n
                            if content_part.endswith(b"\r\n"):
                                content_part = content_part[:-2]
                            csv_text = content_part.decode(
                                "utf-8", errors="replace"
                            )
                            break
                    if not csv_text:
                        self._send_error_json("No file found in multipart upload")
                        return
                except Exception as e:
                    self._send_error_json(f"Failed to parse multipart data: {e}")
                    return
            else:
                # Raw text/csv body
                csv_text = body_bytes.decode("utf-8", errors="replace")
                filename = self.headers.get("X-Filename", "dataset.csv")

            try:
                events, profile = parse_csv_stream(csv_text)
                GLOBAL_STATE.set_dataset(filename, events, profile)
                self._send_json(
                    {
                        "status": "ok",
                        "filename": filename,
                        "total_events": len(events),
                        "profile": profile,
                    }
                )
            except Exception as e:
                self._send_error_json(f"Validation failed: {str(e)}")
            return

        # POST /api/run or /api/start
        if path in ("/api/run", "/api/start"):
            mode = "full"
            if content_length > 0:
                try:
                    payload = json.loads(body_bytes.decode("utf-8"))
                    mode = payload.get("mode", "full").lower()
                except Exception:
                    pass

            try:
                GLOBAL_STATE.start_job(mode)
            except RuntimeError as e:
                self._send_error_json(str(e), HTTPStatus.CONFLICT)
                return

            # Launch background worker
            thread = threading.Thread(
                target=_run_processing_worker, args=(mode,), daemon=True
            )
            thread.start()

            self._send_json(
                {
                    "status": "started",
                    "mode": mode,
                    "dataset": GLOBAL_STATE.filename,
                    "total_events": len(GLOBAL_STATE.events),
                },
                status=HTTPStatus.ACCEPTED,
            )
            return

        self._send_error_json(f"Unknown endpoint: {path}", HTTPStatus.NOT_FOUND)


def run_api_server(host: str = "127.0.0.1", port: int = 8765):
    """Run the BDA server serving both API and frontend."""
    # Pre-load sample_ooo_2k.csv by default if available so demo is immediately ready
    default_sample = DEMO_DATA_DIR / "sample_ooo_2k.csv"
    if default_sample.exists():
        try:
            evs, prof = parse_csv_stream(default_sample)
            GLOBAL_STATE.set_dataset("sample_ooo_2k.csv", evs, prof)
        except Exception:
            pass

    server = ThreadingHTTPServer((host, port), BDAApiHandler)
    print("=" * 70)
    print(f"BDA LIVE STREAM PROCESSING DEMO")
    print(f"Server running at: http://{host}:{port}")
    print(f"Loaded demo dataset: {GLOBAL_STATE.filename} ({len(GLOBAL_STATE.events)} events)")
    print("=" * 70)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_api_server()
