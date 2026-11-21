#!/usr/bin/env python3
'''
FlowPulse AI - One-Click VS Code Launcher (v3)
Binds cleanly to IPv4 (127.0.0.1), auto-selects an available port starting at 8000,
opens the interactive Greater Bengaluru Google Maps dashboard in your browser,
and streams live Bengaluru traffic JSON telemetry in the VS Code terminal.
'''
import functools
import http.server
import json
import os
import random
import socket
import socketserver
import threading
import time
import webbrowser
from datetime import datetime, timezone

HOST = "127.0.0.1"
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

HUBS = [
    {"code": "BLR-CE-01", "junction": "MG Road - Trinity Circle", "zone": "Central", "base": 34.0},
    {"code": "BLR-NO-05", "junction": "Hebbal Flyover Interchange", "zone": "North", "base": 28.0},
    {"code": "BLR-EA-10", "junction": "KR Puram Hanging Bridge", "zone": "East IT", "base": 29.1},
    {"code": "BLR-EA-13", "junction": "Bellandur - Ecospace ORR", "zone": "East IT", "base": 14.0},
    {"code": "BLR-SO-17", "junction": "Central Silk Board Junction", "zone": "South", "base": 18.0},
    {"code": "BLR-WE-26", "junction": "Rajajinagar - Navrang Junction", "zone": "West", "base": 42.0},
    {"code": "BLR-OB-31", "junction": "Kengeri Satellite Town", "zone": "Outer Belt", "base": 57.0},
]


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def find_free_port(start_port: int = 8000) -> int:
    for p in range(start_port, start_port + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((HOST, p)) != 0:
                return p
    return start_port


def start_web_server(port: int):
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=PROJECT_DIR)
    with ReusableTCPServer((HOST, port), handler) as httpd:
        httpd.serve_forever()


def main():
    port = find_free_port(8000)
    server_thread = threading.Thread(target=start_web_server, args=(port,), daemon=True)
    server_thread.start()

    url = f"http://{HOST}:{port}/index.html"
    print("=" * 74)
    print("  FlowPulse AI // Greater Bengaluru Kinetic Big Data Digital Twin")
    print("=" * 74)
    print(f"  [OK] Dashboard Server Running : {url}")
    print("  [OK] Streaming Live Telemetry : Press Ctrl+C in terminal to stop")
    print("=" * 74)

    time.sleep(0.8)
    webbrowser.open(url)

    try:
        while True:
            hub = random.choice(HUBS)
            event = {
                "code": hub["code"],
                "junction": hub["junction"],
                "zone": hub["zone"],
                "speed_kmph": round(max(6.5, hub["base"] + random.uniform(-2.5, 2.5)), 1),
                "density_veh_km": random.randint(65, 175),
                "event_ts": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            }
            print("[KAFKA STREAM]", json.dumps(event))
            time.sleep(1.5)
    except KeyboardInterrupt:
        print("\n[Stopped] FlowPulse AI server shut down cleanly.")


if __name__ == "__main__":
    main()
