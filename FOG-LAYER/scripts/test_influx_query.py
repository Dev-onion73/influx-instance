#!/usr/bin/env python3
"""Small helper to run an InfluxDB Flux query and print returned records."""

from __future__ import annotations

import argparse
import os

from influxdb_client import InfluxDBClient


def _flux_time_expr(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("Empty Flux time value")
    if value.startswith("-") or value.startswith("now(") or value.startswith("time("):
        return value
    return f'time(v: "{value}")'


def build_query(bucket: str, measurement: str, start: str, stop: str | None, node_id: str, field: str) -> str:
    lines = [
        f'from(bucket: "{bucket}")',
        f'  |> range(start: {_flux_time_expr(start)}{f", stop: {_flux_time_expr(stop)}" if stop else ""})',
    ]
    if measurement:
        lines.append(f'  |> filter(fn: (r) => r["_measurement"] == "{measurement}")')
    if node_id:
        lines.append(f'  |> filter(fn: (r) => r["node_id"] == "{node_id}")')
    if field:
        lines.append(f'  |> filter(fn: (r) => r["_field"] == "{field}")')
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test Flux query against InfluxDB and print records")
    parser.add_argument("--url", default=os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    parser.add_argument("--token", default=os.getenv("INFLUXDB_TOKEN"))
    parser.add_argument("--org", default=os.getenv("INFLUXDB_ORG", "NEO-SOLAR"))
    parser.add_argument("--bucket", default=os.getenv("INFLUX_INPUT_BUCKET", "metrics"))
    parser.add_argument("--measurement", default=os.getenv("INFLUX_INPUT_MEASUREMENT", ""), help="Optional _measurement filter")
    parser.add_argument("--node-id", default=os.getenv("INFLUX_NODE_ID_FILTER", ""))
    parser.add_argument("--field", default="", help="Optional _field filter")
    parser.add_argument("--start", default="-10m", help='Flux range start, for example "-10m" or "2026-04-16T00:00:00Z"')
    parser.add_argument("--stop", default="", help='Optional Flux range stop, for example "now()" or an ISO timestamp')
    args = parser.parse_args()

    if not args.token:
        raise ValueError("Missing Influx token. Pass --token or set INFLUX_TOKEN")

    query = build_query(
        bucket=args.bucket,
        measurement=args.measurement,
        start=args.start,
        stop=args.stop or None,
        node_id=args.node_id,
        field=args.field,
    )

    print("Flux query:\n")
    print(query)
    print("\nResults:\n")

    with InfluxDBClient(url=args.url, token=args.token, org=args.org) as client:
        query_api = client.query_api()
        tables = query_api.query(query, org=args.org)

        row_count = 0
        for table in tables:
            for record in table.records:
                row_count += 1
                values = record.values
                ts = values.get("_time")
                measurement = values.get("_measurement")
                field_name = values.get("_field")
                field_value = values.get("_value")
                node_id = values.get("node_id", "")
                print(
                    f"{row_count:04d} | time={ts} | measurement={measurement} | node_id={node_id} | field={field_name} | value={field_value}"
                )

        print(f"\nTotal records: {row_count}")


if __name__ == "__main__":
    main()