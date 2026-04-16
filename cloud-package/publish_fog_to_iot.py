#!/usr/bin/env python3
"""Publish fog telemetry and L2 inference streams from InfluxDB to AWS IoT MQTT topics."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient


META_COLS = {"result", "table", "_start", "_stop"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish metrics and L2 inference from Influx to AWS IoT")

    parser.add_argument("--influx-url", default=os.getenv("INFLUXDB_URL", os.getenv("INFLUX_URL", "http://localhost:8086")))
    parser.add_argument("--influx-token", default=os.getenv("INFLUXDB_TOKEN", os.getenv("INFLUX_TOKEN", "")))
    parser.add_argument("--influx-org", default=os.getenv("INFLUXDB_ORG", os.getenv("INFLUX_ORG", "NEO-SOLAR")))
    parser.add_argument("--metrics-bucket", default=os.getenv("INFLUX_BUCKET_METRICS", "metrics"))
    parser.add_argument("--l2-bucket", default=os.getenv("INFLUX_BUCKET_L2", "inferences"))

    parser.add_argument(
        "--mqtt-host",
        default=os.getenv("AWS_IOT_ENDPOINT", os.getenv("MQTT_BROKER", "a2i2can38qdenj-ats.iot.eu-west-2.amazonaws.com")),
    )
    parser.add_argument("--mqtt-port", type=int, default=None)
    parser.add_argument("--mqtt-client-id", default=os.getenv("MQTT_CLIENT_ID", "fog-iot-publisher"))
    parser.add_argument("--qos", type=int, default=1, choices=[0, 1, 2])
    parser.add_argument("--metrics-topic", default=os.getenv("FOG_METRICS_TOPIC", "solar/rtu"))
    parser.add_argument("--l2-topic-prefix", default=os.getenv("FOG_L2_TOPIC_PREFIX", "solar/l2"))

    parser.add_argument("--mqtt-username", default=os.getenv("MQTT_USERNAME", ""))
    parser.add_argument("--mqtt-password", default=os.getenv("MQTT_PASSWORD", ""))
    parser.add_argument("--ca-cert", default=os.getenv("AWS_CA_CERT", "AmazonRootCA1.pem"))
    parser.add_argument("--client-cert", default=os.getenv("AWS_CLIENT_CERT", "ec2.cert.pem"))
    parser.add_argument("--client-key", default=os.getenv("AWS_CLIENT_KEY", "ec2.private.key"))
    parser.add_argument("--use-tls", action="store_true", help="Force TLS for MQTT connection")
    parser.add_argument("--insecure-tls", action="store_true")

    parser.add_argument("--poll-interval", type=float, default=2.0, help="Seconds between polls")
    parser.add_argument("--lookback-seconds", type=float, default=15.0, help="Initial lookback window")
    parser.add_argument("--dry-run", action="store_true", help="Print outgoing payloads without publishing")

    args = parser.parse_args()
    if not args.influx_token or not args.influx_org:
        raise ValueError("Missing Influx credentials: set INFLUXDB_TOKEN and INFLUXDB_ORG")

    if args.mqtt_port is None:
        env_port = os.getenv("MQTT_PORT")
        if env_port:
            args.mqtt_port = int(env_port)
        elif "amazonaws.com" in args.mqtt_host:
            args.mqtt_port = 8883
        else:
            args.mqtt_port = 1883

    return args


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _concat_frames(raw: object):
    import pandas as pd

    if isinstance(raw, list):
        frames = [f for f in raw if isinstance(f, pd.DataFrame) and not f.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    if isinstance(raw, pd.DataFrame):
        return raw.copy()
    return pd.DataFrame()


def _query_long_rows(query_api, bucket: str, start_ts: datetime, stop_ts: datetime):
    flux = f'''
from(bucket: "{bucket}")
  |> range(start: time(v: "{_iso_utc(start_ts)}"), stop: time(v: "{_iso_utc(stop_ts)}"))
'''.strip()
    raw = query_api.query_data_frame(flux)
    return _concat_frames(raw)


def _extract_tags(row: Dict[str, object]) -> Dict[str, str]:
    tags: Dict[str, str] = {}
    for key, value in row.items():
        if key in META_COLS or key.startswith("_"):
            continue
        if value is None:
            continue
        # Treat non-numeric columns as tags.
        if isinstance(value, str):
            tags[key] = value
    return tags


def _iter_payloads(df, default_measurement: str) -> Iterable[Dict[str, object]]:
    if df.empty:
        return

    grouped: Dict[tuple, Dict[str, object]] = {}

    for _, series in df.iterrows():
        row = series.to_dict()
        if "_field" not in row or "_value" not in row:
            continue

        ts_raw = row.get("_time")
        if hasattr(ts_raw, "to_pydatetime"):
            ts = ts_raw.to_pydatetime().astimezone(timezone.utc)
        elif isinstance(ts_raw, datetime):
            ts = ts_raw.astimezone(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        measurement = str(row.get("_measurement") or default_measurement or "solar_rtu")
        if measurement.lower() in {"", "null", "none", "nan"}:
            measurement = default_measurement or "solar_rtu"

        tags = _extract_tags(row)
        node_id = str(tags.get("node_id", row.get("node_id", "")))
        group_key = (measurement, _iso_utc(ts), node_id)

        payload = grouped.setdefault(
            group_key,
            {
                "timestamp": _iso_utc(ts),
                "measurement": measurement,
                "tags": tags,
                "fields": {},
            },
        )
        payload["fields"][str(row.get("_field"))] = row.get("_value")

    for payload in grouped.values():
        yield payload


def _mqtt_client(args: argparse.Namespace) -> mqtt.Client:
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=args.mqtt_client_id)
    else:
        client = mqtt.Client(client_id=args.mqtt_client_id, clean_session=True)
    if args.mqtt_username:
        client.username_pw_set(args.mqtt_username, args.mqtt_password)

    use_tls = bool(args.use_tls or "amazonaws.com" in args.mqtt_host)
    if use_tls:
        if not args.ca_cert or not args.client_cert or not args.client_key:
            raise ValueError("AWS IoT TLS requires --ca-cert, --client-cert, and --client-key")
        client.tls_set(
            ca_certs=args.ca_cert,
            certfile=args.client_cert,
            keyfile=args.client_key,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        if args.insecure_tls:
            client.tls_insecure_set(True)

    try:
        client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)
    except OSError as exc:
        hint = (
            f"Unable to connect to MQTT at {args.mqtt_host}:{args.mqtt_port}. "
            "For local broker use --mqtt-port 1883. For AWS IoT use --mqtt-host <endpoint> --mqtt-port 8883 --use-tls and certs."
        )
        raise ConnectionError(hint) from exc

    client.loop_start()
    return client


def _publish_batch(client: mqtt.Client, topic: str, payloads: Iterable[Dict[str, object]], qos: int, dry_run: bool) -> int:
    count = 0
    for payload in payloads:
        body = json.dumps(payload, separators=(",", ":"))
        if dry_run:
            print(f"DRY-RUN topic={topic} payload={body}")
        else:
            print(f"LIVE topic={topic} payload={body}", flush=True)
            result = client.publish(topic, payload=body, qos=qos)
            result.wait_for_publish()
        count += 1
    return count


def main() -> None:
    args = parse_args()
    print(f"Fog IoT publisher starting | mqtt={args.mqtt_host}:{args.mqtt_port}")
    print(f"Source buckets: metrics={args.metrics_bucket} l2={args.l2_bucket}")
    print(f"Topics: metrics={args.metrics_topic} l2_prefix={args.l2_topic_prefix}")

    client = _mqtt_client(args)
    last_metrics = datetime.now(timezone.utc) - timedelta(seconds=args.lookback_seconds)
    last_l2 = datetime.now(timezone.utc) - timedelta(seconds=args.lookback_seconds)

    with InfluxDBClient(url=args.influx_url, token=args.influx_token, org=args.influx_org) as influx:
        query_api = influx.query_api()
        try:
            while True:
                now = datetime.now(timezone.utc)

                metrics_df = _query_long_rows(query_api, args.metrics_bucket, last_metrics, now)
                metrics_count = _publish_batch(
                    client,
                    args.metrics_topic,
                    _iter_payloads(metrics_df, default_measurement="solar_rtu"),
                    qos=args.qos,
                    dry_run=args.dry_run,
                )
                last_metrics = now

                l2_df = _query_long_rows(query_api, args.l2_bucket, last_l2, now)
                l2_count = 0
                for payload in _iter_payloads(l2_df, default_measurement="fog_inference"):
                    measurement = str(payload.get("measurement") or "unknown")
                    topic = f"{args.l2_topic_prefix}/{measurement}".replace("//", "/")
                    l2_count += _publish_batch(
                        client,
                        topic,
                        [payload],
                        qos=args.qos,
                        dry_run=args.dry_run,
                    )
                last_l2 = now

                if metrics_count or l2_count:
                    print(f"Published metrics={metrics_count} l2={l2_count}")

                time.sleep(max(0.1, args.poll_interval))
        finally:
            client.loop_stop()
            client.disconnect()


if __name__ == "__main__":
    main()
