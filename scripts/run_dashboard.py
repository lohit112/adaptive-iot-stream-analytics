#!/usr/bin/env python3
"""
One-command launch script for the BDA IoT Stream Processing Live Demo.

Runs a unified stdlib HTTP server serving both the REST API and the
real-time research & engineering web dashboard.

Usage:
    python run_demo.py [--port 8765] [--host 127.0.0.1] [--open]
"""

import argparse
import os
import sys
import webbrowser
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.bda.api.server import run_api_server
from src.bda.pipeline.stream import check_kafka_availability


def main():
    parser = argparse.ArgumentParser(
        description="Launch BDA Adaptive IoT Stream Processing Live Demo"
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host interface (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="Port number (default: 8765)")
    parser.add_argument("--open", action="store_true", help="Automatically open dashboard in browser")
    args = parser.parse_args()

    url = f"http://{args.host}:{args.port}"
    kafka_online = check_kafka_availability()

    print("*" * 75)
    print("  BDA ADAPTIVE IOT STREAM PROCESSING — LIVE DEMO")
    print("  CMiX · ALOA · MASO · EARM · Online Anomaly Detection")
    print("*" * 75)
    print(f"  -> Web Dashboard : {url}")
    print(f"  -> Transport     : {'Kafka (localhost:9092)' if kafka_online else 'In-process demo fallback'}")
    print(f"  -> Default Data  : sample_ooo_2k.csv (ready to run)")
    print("*" * 75)
    print("Instructions:")
    print("  1. Open the URL above in any modern web browser.")
    print("  2. Explore the pre-loaded dataset profile or upload your own IoT CSV.")
    print("  3. Click 'Run Live Demo' to see live streaming aggregation, state")
    print("     reduction, exact correctness verification, and anomaly detection.")
    print("  4. Switch to 'Precomputed Research Results' to view benchmark baselines.")
    print("*" * 75)
    print("Press Ctrl+C to terminate the server.\n")

    if args.open:
        try:
            webbrowser.open(url)
        except Exception:
            pass

    run_api_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
