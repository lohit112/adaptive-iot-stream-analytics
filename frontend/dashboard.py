"""
Static file server for the researcher results dashboard.

Serves frontend/index.html plus the generated JSON/plot assets.

Run:
    .venv/bin/python frontend/dashboard.py [--port 8765]
"""

import argparse
import http.server
import os
from functools import partial
from pathlib import Path

FRONTEND_DIR = Path(__file__).resolve().parent


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

    def log_message(self, format, *args):
        if os.environ.get("DASHBOARD_QUIET"):
            return
        super().log_message(format, *args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.quiet:
        os.environ["DASHBOARD_QUIET"] = "1"

    server = http.server.ThreadingHTTPServer(
        (args.host, args.port),
        partial(DashboardHandler),
    )

    print(
        f"BDA results dashboard: "
        f"http://{args.host}:{args.port}"
    )
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()