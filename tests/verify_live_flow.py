"""
End-to-end live flow verification script against the running BDA server.
"""

import json
import time
import urllib.request

base = "http://127.0.0.1:8765"

print("1. Testing GET / (HTML delivery)...")
with urllib.request.urlopen(f"{base}/") as resp:
    html = resp.read().decode()
    assert "Adaptive IoT Stream Processing" in html
    assert "Precomputed Research Results" in html
    assert "btnRunStream" in html
    assert "compTable" in html
    print(f"   -> OK: HTML delivered ({len(html)} bytes)")

print("2. Testing GET /assets/results_bundle.json...")
with urllib.request.urlopen(f"{base}/assets/results_bundle.json") as resp:
    data = json.loads(resp.read().decode())
    assert len(data) > 0
    print(f"   -> OK: Benchmark bundle delivered ({len(data)} result sets)")

print("3. Testing GET /api/system...")
with urllib.request.urlopen(f"{base}/api/system") as resp:
    data = json.loads(resp.read().decode())
    print(f"   -> OK: Transport is: {data['transport_label']}")

print("4. Testing Upload sample_ooo_2k.csv...")
req = urllib.request.Request(
    f"{base}/api/upload",
    data=json.dumps({"sample": "sample_ooo_2k.csv"}).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    data = json.loads(resp.read().decode())
    p = data["profile"]
    print(
        f"   -> OK: Profiled {p['total_events']} events, OOO: {p['ooo_percentage']}%, Max Lateness: {p['max_lateness_seconds']}s"
    )

print("5. Testing POST /api/run (mode=full)...")
req = urllib.request.Request(
    f"{base}/api/run",
    data=json.dumps({"mode": "full"}).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    assert resp.status == 202
    print("   -> OK: Background run started")

print("6. Polling status until completed...")
for _ in range(50):
    time.sleep(0.1)
    with urllib.request.urlopen(f"{base}/api/status") as resp:
        st = json.loads(resp.read().decode())
        if st["status"] == "completed":
            print(f"   -> OK: Completed {st['progress']['received']} events")
            break

print("7. Testing GET /api/results...")
with urllib.request.urlopen(f"{base}/api/results") as resp:
    res = json.loads(resp.read().decode())
    m = res["metrics"]
    c = res["correctness"]
    print(
        f"   -> OK: Correctness: {c['exact']}, Throughput: {m['throughput_ev_per_sec']} ev/s, Peak Hot: {m['peak_hot']}, Frozen: {m['frozen_emissions']}"
    )
    assert c["exact"] is True
    assert "comparison_matrix" in res
    print(f"   -> OK: Comparison matrix keys: {list(res['comparison_matrix'].keys())}")

print("8. Testing GET /api/ml...")
with urllib.request.urlopen(f"{base}/api/ml") as resp:
    ml = json.loads(resp.read().decode())
    print(
        f"   -> OK: ML Status: {ml['status']}, Scored: {ml['observations_scored']}, Anomalies: {ml['anomaly_count']} ({ml['anomaly_percentage']}%)"
    )

print("9. Testing Dataset Change to sample_ooo.csv...")
req = urllib.request.Request(
    f"{base}/api/upload",
    data=json.dumps({"sample": "sample_ooo.csv"}).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    data = json.loads(resp.read().decode())
    p = data["profile"]
    print(
        f"   -> OK: New profile: {p['total_events']} events, OOO: {p['ooo_percentage']}%, Max Late: {p['max_lateness_seconds']}s"
    )
    assert p["total_events"] == 5

print("10. Running stream on new 5-event dataset...")
req = urllib.request.Request(
    f"{base}/api/run",
    data=json.dumps({"mode": "full"}).encode(),
    headers={"Content-Type": "application/json"},
)
urllib.request.urlopen(req)
for _ in range(50):
    time.sleep(0.05)
    with urllib.request.urlopen(f"{base}/api/status") as resp:
        st = json.loads(resp.read().decode())
        if st["status"] == "completed":
            break

with urllib.request.urlopen(f"{base}/api/results") as resp:
    res = json.loads(resp.read().decode())
    assert res["metrics"]["total_events"] == 5
    assert res["correctness"]["exact"] is True
    print(
        f"   -> OK: Verified numbers changed dynamically to 5 events (Exact: {res['correctness']['exact']})"
    )

print("\nALL 10 API & UI INTEGRATION FLOWS VERIFIED SUCCESSFULLY!")
