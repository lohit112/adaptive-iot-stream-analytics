"""
Comprehensive unit and end-to-end integration tests for:
- Ingestion & schema validation
- Stream pipeline & mechanisms (CMiX, ALOA, MASO, EARM, Full)
- Live ground truth & correctness verification
- Unsupervised Isolation Forest ML anomaly detection
- Stdlib REST API server endpoints
"""

import json
import sys
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.bda.api.server import BDAApiHandler, GLOBAL_STATE
from src.bda.ml.detector import StreamAnomalyDetector
from src.bda.pipeline.ingest import parse_csv_stream
from src.bda.pipeline.stream import StreamPipeline, compute_ground_truth_rows

DEMO_CSV = Path(__file__).resolve().parent.parent / "data" / "demo" / "sample_ooo.csv"
DEMO_2K_CSV = Path(__file__).resolve().parent.parent / "data" / "demo" / "sample_ooo_2k.csv"


def test_ingestion_valid_and_ooo_profiling():
    events, profile = parse_csv_stream(DEMO_CSV)
    assert len(events) == 5
    assert profile["total_events"] == 5
    assert profile["ooo_events"] == 3
    assert profile["ooo_percentage"] == 60.0
    assert profile["max_lateness_seconds"] == 3.0
    assert profile["devices_count"] == 1
    assert profile["sensors_count"] == 1
    assert "arrival_preview" in profile
    assert "event_time_preview" in profile


def test_ingestion_malformed_and_empty():
    with pytest.raises(ValueError, match="empty"):
        parse_csv_stream("")

    with pytest.raises(ValueError, match="Missing required timestamp"):
        parse_csv_stream("A,B,C\n1,2,3")

    with pytest.raises(ValueError, match="Invalid numeric value"):
        parse_csv_stream("Timestamp,DeviceId,Sensor,Value\n2024-01-01 10:00:00,D1,Temp,NotANumber")


def test_pipeline_all_mechanisms_exact_correctness():
    events, _ = parse_csv_stream(DEMO_CSV)
    for mode in ["cmix", "aloa", "maso", "full"]:
        pipeline = StreamPipeline(mode=mode)
        res = pipeline.run(events)
        assert res["correctness"]["exact"] is True
        assert res["correctness"]["missing_in_result"] == 0
        assert res["correctness"]["extra_in_result"] == 0
        assert res["correctness"]["field_mismatches"] == 0
        assert res["metrics"]["transport_label"] is not None
        assert res["metrics"]["throughput_ev_per_sec"] > 0


def test_ml_anomaly_detection_deterministic():
    events, _ = parse_csv_stream(DEMO_2K_CSV)
    pipeline = StreamPipeline(mode="full")
    res = pipeline.run(events)
    rows = res["result_rows"]
    assert len(rows) > 10

    det1 = StreamAnomalyDetector(random_state=42)
    out1 = det1.detect(rows)

    det2 = StreamAnomalyDetector(random_state=42)
    out2 = det2.detect(rows)

    assert out1["status"] == "success"
    assert out1["anomaly_count"] == out2["anomaly_count"]
    assert out1["anomaly_percentage"] == out2["anomaly_percentage"]
    assert out1["score_distribution"]["mean"] == out2["score_distribution"]["mean"]
    assert len(out1["score_distribution"]["histogram"]) == 10
    assert len(out1["anomalous_records"]) > 0


def test_ml_insufficient_data():
    det = StreamAnomalyDetector(min_samples_required=5)
    out = det.detect([])
    assert out["status"] == "insufficient_data"
    assert out["anomaly_count"] == 0


def test_api_end_to_end_server_flow():
    # Start server on dynamic port
    server = ThreadingHTTPServer(("127.0.0.1", 0), BDAApiHandler)
    port = server.server_address[1]
    base_url = f"http://127.0.0.1:{port}"

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        # 1. GET /api/system
        req = urllib.request.Request(f"{base_url}/api/system")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            assert "transport_label" in data

        # 2. POST /api/upload with sample_ooo_2k.csv
        upload_payload = json.dumps({"sample": "sample_ooo_2k.csv"}).encode()
        req = urllib.request.Request(
            f"{base_url}/api/upload",
            data=upload_payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            assert data["status"] == "ok"
            assert data["total_events"] == 2400

        # 3. GET /api/profile
        req = urllib.request.Request(f"{base_url}/api/profile")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            assert data["profile"]["total_events"] == 2400
            assert data["profile"]["ooo_events"] > 0

        # 4. POST /api/run
        run_payload = json.dumps({"mode": "full"}).encode()
        req = urllib.request.Request(
            f"{base_url}/api/run",
            data=run_payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 202

        # 5. Poll GET /api/status until completed
        completed = False
        for _ in range(50):
            time.sleep(0.1)
            req = urllib.request.Request(f"{base_url}/api/status")
            with urllib.request.urlopen(req) as resp:
                status_data = json.loads(resp.read().decode())
                if status_data["status"] == "completed":
                    completed = True
                    break
        assert completed is True

        # 6. GET /api/results
        req = urllib.request.Request(f"{base_url}/api/results")
        with urllib.request.urlopen(req) as resp:
            res_data = json.loads(resp.read().decode())
            assert res_data["correctness"]["exact"] is True
            assert res_data["metrics"]["peak_hot"] > 0
            assert "comparison_matrix" in res_data
            assert "cmix" in res_data["comparison_matrix"]
            assert "full" in res_data["comparison_matrix"]

        # 7. GET /api/ml
        req = urllib.request.Request(f"{base_url}/api/ml")
        with urllib.request.urlopen(req) as resp:
            ml_data = json.loads(resp.read().decode())
            assert ml_data["status"] == "success"
            assert ml_data["anomaly_count"] > 0

        # 8. GET /api/correctness
        req = urllib.request.Request(f"{base_url}/api/correctness")
        with urllib.request.urlopen(req) as resp:
            corr_data = json.loads(resp.read().decode())
            assert corr_data["exact"] is True

        # 9. GET /api/export
        req = urllib.request.Request(f"{base_url}/api/export")
        with urllib.request.urlopen(req) as resp:
            export_data = json.loads(resp.read().decode())
            assert export_data["dataset"] == "sample_ooo_2k.csv"
            assert "metrics" in export_data
            assert "ml" in export_data

    finally:
        server.shutdown()
        server.server_close()
