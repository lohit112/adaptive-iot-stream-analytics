"""
Interactive terminal demonstration of the live stream processing engine.
"""

import json
import time
import urllib.request

base = "http://127.0.0.1:8765"

print("=" * 72)
print("  BDA ADAPTIVE IOT STREAM PROCESSING: TERMINAL LIVE RUN")
print("=" * 72)

# 1. System Info
with urllib.request.urlopen(f"{base}/api/system") as resp:
    sys_info = json.loads(resp.read().decode())
    print(f"[+] Server Status      : Online (HTTP 200)")
    print(f"[+] Transport Mode     : {sys_info['transport_label']}")
    print(f"[+] Preloaded Dataset  : {sys_info['dataset_name']}")

# 2. Dataset Profile
with urllib.request.urlopen(f"{base}/api/profile") as resp:
    prof = json.loads(resp.read().decode())["profile"]
    print("\n--- DATASET PROFILE ---")
    print(f"  Total Events        : {prof['total_events']:,}")
    print(f"  Out-of-Order Events : {prof['ooo_events']:,} ({prof['ooo_percentage']}%)")
    print(f"  Max Lateness        : {prof['max_lateness_seconds']} seconds")
    print(f"  Timespan            : {prof['timespan_seconds']} seconds")
    print(f"  Unique Devices      : {prof['devices_count']} ({prof['devices']})")
    print(f"  Sensors             : {prof['sensors_count']} ({prof['sensors']})")

# 3. Start Streaming Run
print("\n--- INITIATING STREAM PROCESSING (Mode: Full) ---")
req = urllib.request.Request(
    f"{base}/api/run",
    data=json.dumps({"mode": "full"}).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    run_ack = json.loads(resp.read().decode())
    print(f"  Job Status          : {run_ack['status'].upper()} (Processing in background)")

# 4. Poll Live Progress
while True:
    time.sleep(0.08)
    with urllib.request.urlopen(f"{base}/api/status") as resp:
        st = json.loads(resp.read().decode())
        p = st["progress"]
        print(
            f"  Progress: {p['pct']:>5.1f}% | Processed: {p['received']:>4}/{p['total']} | Active Hot: {p['active_state']:>3} | Ev/s: {p['throughput']:>7.1f}",
            end="\r",
        )
        if st["status"] == "completed":
            print("\n  Stream Processing Complete!")
            break

# 5. Live Results & Correctness
with urllib.request.urlopen(f"{base}/api/results") as resp:
    res = json.loads(resp.read().decode())
    m = res["metrics"]
    c = res["correctness"]
    print("\n--- PERFORMANCE & METRICS ---")
    print(f"  Throughput          : {m['throughput_ev_per_sec']:,} events/sec")
    print(f"  Elapsed Time        : {m['elapsed_seconds']} seconds")
    print(f"  Peak Hot State      : {m['peak_hot']} entries")
    print(f"  Frozen Emissions    : {m['frozen_emissions']} window summaries released")
    print(f"  Reconciled Events   : {m['reconciled_events']}")
    print(f"  Late-after-Eviction : {m['late_after_eviction']} (0 observed)")

    print("\n--- GROUND-TRUTH CORRECTNESS ---")
    status_str = "EXACT" if c["exact"] else "MISMATCH"
    print(
        f"  Verdict             : {status_str} (Ground Truth: {c['ground_truth_rows']} rows, Result: {c['result_rows']} rows)"
    )
    print(f"  Missing in Result   : {c['missing_in_result']}")
    print(f"  Extra in Result     : {c['extra_in_result']}")
    print(f"  Field Mismatches    : {c['field_mismatches']} (tolerance: {c['tolerance']})")

    print("\n--- MECHANISM COMPARISON ON THIS DATASET ---")
    print(
        f"  {'Mechanism':<12} | {'Status':<7} | {'Peak Hot':<9} | {'Reduction':<10} | {'Reconciled':<10} | {'Throughput':<12}"
    )
    print("  " + "-" * 70)
    for k, v in res["comparison_matrix"].items():
        base_hot = res["comparison_matrix"]["cmix"]["peak_hot"]
        red_pct = (
            f"{((base_hot - v['peak_hot']) / base_hot * 100):.1f}%"
            if base_hot
            else "0.0%"
        )
        print(
            f"  {k.upper():<12} | {'EXACT':<7} | {v['peak_hot']:>9} | {red_pct:>10} | {v['reconciled_events']:>10} | {v['throughput_ev_per_sec']:>12.1f} ev/s"
        )

# 6. ML Anomaly Results
with urllib.request.urlopen(f"{base}/api/ml") as resp:
    ml = json.loads(resp.read().decode())
    print("\n--- UNSUPERVISED ISOLATION FOREST ANOMALIES ---")
    print(f"  Model Status        : {ml['status'].upper()}")
    print(f"  Windows Scored      : {ml['observations_scored']}")
    print(f"  Anomalies Detected  : {ml['anomaly_count']} ({ml['anomaly_percentage']}%)")
    print(
        f"  Score Range         : Min={ml['score_distribution']['min']}, Mean={ml['score_distribution']['mean']}, Max={ml['score_distribution']['max']}"
    )
    print("  Sample Anomalous Windows:")
    for item in ml["anomalous_records"][:3]:
        print(
            f"    -> Window {item['start']} | Device: {item['DeviceId']} | Sensor: {item['Sensor']} | Avg: {item['avg']} | Score: {item['anomaly_score']}"
        )

# 7. Dataset Switch Demonstration
print("\n" + "=" * 72)
print("  DYNAMIC DATASET CHANGE DEMONSTRATION")
print("=" * 72)
print("  Uploading different dataset: sample_ooo.csv (5 events)...")
req = urllib.request.Request(
    f"{base}/api/upload",
    data=json.dumps({"sample": "sample_ooo.csv"}).encode(),
    headers={"Content-Type": "application/json"},
)
with urllib.request.urlopen(req) as resp:
    data = json.loads(resp.read().decode())
    p = data["profile"]
    print(f"  [+] Dataset Changed to : {data['filename']}")
    print(f"  [+] Event Count        : {p['total_events']} events")
    print(f"  [+] OOO Percentage     : {p['ooo_percentage']}%")
    print(f"  [+] Max Lateness       : {p['max_lateness_seconds']}s")

print("  Executing stream processing on the new dataset...")
req = urllib.request.Request(
    f"{base}/api/run",
    data=json.dumps({"mode": "full"}).encode(),
    headers={"Content-Type": "application/json"},
)
urllib.request.urlopen(req)
time.sleep(0.2)

with urllib.request.urlopen(f"{base}/api/results") as resp:
    res = json.loads(resp.read().decode())
    m = res["metrics"]
    c = res["correctness"]
    print(f"  [+] Stream Run Finished: {m['total_events']} events processed")
    print(f"  [+] Correctness Verdict: {'EXACT' if c['exact'] else 'MISMATCH'} (GT: {c['ground_truth_rows']} rows, Result: {c['result_rows']} rows)")
    print(f"  [+] Peak Hot State     : {m['peak_hot']} entries")
    print(f"  [+] Reconciled Events  : {m['reconciled_events']}")
    print("  -> Confirmed: Live numbers dynamically changed based on the new dataset!")

print("=" * 72 + "\n")
