#!/usr/bin/env python3
"""
MQTT Publishers for Reservoir WML (TAF).

Reads CSV files and publishes JSON messages to topics:
  SHASTA/WML, OROVILLE/WML, SONOMA/WML

Usage:
  python mqtt_publishers.py --broker mqtt.example.org --port 1883 --csv-dir ./data
Optional:
  --username USER --password PASS --qos 1 --retain
"""
import argparse
import json
import os
from pathlib import Path
from datetime import datetime
import pytz
import pandas as pd
import paho.mqtt.client as mqtt

RESERVOIR_TOPICS = {
    "Shasta_WML(Sample),.csv": "SHASTA/WML",
    "Oroville_WML(Sample),.csv": "OROVILLE/WML",
    "Sonoma_WML(Sample),.csv": "SONOMA/WML",
}

def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.rename(columns={"Date":"date","TAF":"taf"})
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")
    df["taf"] = pd.to_numeric(df["taf"])
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", required=True)
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--username")
    ap.add_argument("--password")
    ap.add_argument("--csv-dir", default=".")
    ap.add_argument("--qos", type=int, default=1)
    ap.add_argument("--retain", action="store_true")
    ap.add_argument("--tz", default="America/Los_Angeles")
    args = ap.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if args.username:
        client.username_pw_set(args.username, password=args.password)
    client.connect(args.broker, args.port, keepalive=60)

    tz = pytz.timezone(args.tz)
    csv_dir = Path(args.csv_dir)

    for fname, topic in RESERVOIR_TOPICS.items():
        fp = csv_dir / fname
        if not fp.exists():
            print(f"WARNING: missing {fp}")
            continue
        df = load_csv(fp)
        for _, row in df.sort_values("date").iterrows():
            ts = tz.localize(datetime(row['date'].year, row['date'].month, row['date'].day))
            payload = {
                "reservoir_id": topic.split("/")[0],
                "topic": topic,
                "timestamp": ts.isoformat(),
                "wml_taf": float(row["taf"]),
                "wml_af": int(round(row["taf"] * 1000)),
                "units": {"wml": "TAF", "alternate": "AF"},
                "source_file": fname,
                "message_type": "publisher"
            }
            client.publish(topic, json.dumps(payload), qos=args.qos, retain=args.retain)
            print(f"Published {topic} {payload['timestamp']} TAF={payload['wml_taf']}")
    client.disconnect()

if __name__ == "__main__":
    main()
