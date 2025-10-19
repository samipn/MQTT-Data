#!/usr/bin/env python3
"""
Single MQTT Subscriber that listens to all *WML topics and writes a daily report.
Consumes messages like those emitted by mqtt_publishers.py.

Usage:
  python mqtt_subscriber_report.py --broker mqtt.example.org --port 1883 --out ./out
Optional:
  --username USER --password PASS --qos 1 --tz America/Los_Angeles
"""
import argparse
import json
from pathlib import Path
from datetime import datetime
import pytz
import pandas as pd
import paho.mqtt.client as mqtt

class DailyAggregator:
    def __init__(self, tz="America/Los_Angeles"):
        self.tz = pytz.timezone(tz)
        self.rows = []

    def add_message(self, payload: dict):
        ts = pd.to_datetime(payload["timestamp"])
        day = ts.date()
        self.rows.append({
            "date": day,
            "reservoir_id": payload["reservoir_id"],
            "storage_taf": float(payload["wml_taf"]),
            "storage_af": int(payload["wml_af"]),
        })

    def write_report(self, out_dir: Path):
        if not self.rows:
            return None
        df = pd.DataFrame(self.rows)
        per_res = (
            df.groupby(["date","reservoir_id"], as_index=False)
              .agg(storage_taf=("storage_taf","mean"), storage_af=("storage_af","mean"))
        )
        per_res["storage_af"] = per_res["storage_af"].round(0).astype(int)
        per_res = per_res.sort_values(["reservoir_id","date"])
        per_res["delta_taf"] = per_res.groupby("reservoir_id")["storage_taf"].diff().round(3)
        totals = per_res.groupby("date", as_index=False).agg(total_taf=("storage_taf","sum"),
                                                             total_af=("storage_af","sum"))
        report = per_res.merge(totals, on="date", how="left")
        report["pct_of_total"] = (report["storage_taf"]/report["total_taf"]*100).round(2)
        report = report[["date","reservoir_id","storage_taf","storage_af","delta_taf","pct_of_total","total_taf","total_af"]]
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "Daily_WML_Report.csv"
        json_path = out_dir / "Daily_WML_Report.json"
        report.to_csv(csv_path, index=False)
        report.to_json(json_path, orient="records", indent=2, date_format="iso")
        print(f"Wrote {csv_path} and {json_path}")
        return csv_path, json_path

class Subscriber:
    def __init__(self, broker, port, username=None, password=None, qos=1, tz="America/Los_Angeles", out_dir="."):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if username:
            self.client.username_pw_set(username, password=password)
        self.client.on_message = self.on_message
        self.client.on_connect = self.on_connect
        self.qos = qos
        self.agg = DailyAggregator(tz=tz)
        self.out_dir = Path(out_dir)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        print("Connected:", reason_code)
        # Subscribe to ALL reservoir WML topics with a single subscriber
        client.subscribe("+/WML", qos=self.qos)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            self.agg.add_message(payload)
        except Exception as e:
            print("Bad message:", e)

    def loop_forever(self):
        self.client.loop_forever()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", required=True)
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--username")
    ap.add_argument("--password")
    ap.add_argument("--qos", type=int, default=1)
    ap.add_argument("--tz", default="America/Los_Angeles")
    ap.add_argument("--out", default="./out")
    args = ap.parse_args()

    sub = Subscriber(args.broker, args.port, args.username, args.password, args.qos, args.tz, args.out)
    try:
        sub.loop_forever()
    finally:
        sub.agg.write_report(Path(args.out))

if __name__ == "__main__":
    main()
