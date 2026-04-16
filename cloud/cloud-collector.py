#!/usr/bin/env python3
"""
Fog-layer runtime pipeline:
- Read telemetry from InfluxDB input bucket
- Run logistic-regression risk inference
- Derive maintenance decision labels
- Write node and site decisions back to a separate InfluxDB bucket

This script is designed for long-running deployment on a Debian VM.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import joblib
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

IMAGE_COLS = [
    "img_dusty_score",
    "img_cracked_score",
    "img_bird_drop_score",
    "img_panel_score",
]

RAW_NODE_ID_COL = "node_id"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fog Influx->LogReg->Influx pipeline")

    parser.add_argument("--influx-url", default=os.getenv("INFLUX_URL", "http://localhost:8086"))
    parser.add_argument("--influx-token", default=os.getenv("INFLUXDB_TOKEN"), help="InfluxDB API token")
    parser.add_argument("--influx-org", default=os.getenv("INFLUXDB_ORG"), help="InfluxDB org name")

    parser.add_argument("--input-bucket", default=os.getenv("INFLUX_INPUT_BUCKET", "metrics"))
    parser.add_argument("--output-bucket", default=os.getenv("INFLUX_OUTPUT_BUCKET", "inference"))

    parser.add_argument(
        "--input-measurement",
        default=os.getenv("INFLUX_INPUT_MEASUREMENT", ""),
        help="Optional Influx _measurement filter; leave blank when the bucket has no measurement group",
    )
    parser.add_argument("--node-measurement", default=os.getenv("INFLUX_NODE_MEASUREMENT", "logreg_node_decision"))
    parser.add_argument("--site-measurement", default=os.getenv("INFLUX_SITE_MEASUREMENT", "logreg_site_summary"))
    parser.add_argument(
        "--site-id",
        default=os.getenv("FOG_SITE_ID", ""),
        help="Optional fog-level site ID tag. If omitted, generated as site-<uuid>",
    )

    parser.add_argument("--model", type=Path, default=Path("models/logreg_risk_model.joblib"))
    parser.add_argument("--config", type=Path, default=Path("models/model_config.json"))

    parser.add_argument(
        "--node-id-filter",
        default=os.getenv("INFLUX_NODE_ID_FILTER", ""),
        help="Optional node_id tag filter for the input query",
    )

    parser.add_argument("--poll-interval", type=float, default=5.0, help="Seconds between polling Influx")
    parser.add_argument(
        "--bootstrap-lookback-minutes",
        type=float,
        default=10.0,
        help="Initial lookback range when script starts",
    )
    parser.add_argument(
        "--alpha",
        type=float,
        default=float(os.getenv("FOG_L2_ALPHA", "0.6")),
        help="L2 smoothing factor: Rs(t)=alpha*Rs_now+(1-alpha)*Rs_prev",
    )
    parser.add_argument(
        "--trend-window-size",
        type=int,
        default=int(os.getenv("FOG_L2_TREND_WINDOW_SIZE", "20")),
        help="Number of historical Rs_now values retained per site for dRs/dt",
    )
    parser.add_argument("--high", type=float, default=0.6, help="High-risk threshold")
    parser.add_argument("--critical", type=float, default=0.8, help="Critical-risk threshold")
    parser.add_argument(
        "--tag-columns",
        default="node_id",
        help="Comma-separated columns preserved as Influx tags in output",
    )

    parser.add_argument("--once", action="store_true", help="Process one polling window and exit")
    parser.add_argument("--dry-run", action="store_true", help="Run inference without writing to Influx")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    if args.critical <= args.high:
        raise ValueError("critical threshold must be greater than high threshold")
    if not args.influx_token:
        raise ValueError("Missing Influx token. Use --influx-token or INFLUX_TOKEN")
    if not args.influx_org:
        raise ValueError("Missing Influx org. Use --influx-org or INFLUX_ORG")
    if not args.site_id:
        args.site_id = f"site-{uuid.uuid4()}"

    return args


def validate_columns(df: pd.DataFrame, cols: List[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing {name} columns: {missing}")


def detect_issue(row: pd.Series) -> str:
    available = [c for c in IMAGE_COLS if c in row.index]
    if not available:
        return "unknown"
    scores = {c.replace("img_", "").replace("_score", ""): float(row[c]) for c in available}
    return max(scores, key=scores.get)


def issue_confidence(row: pd.Series, issue: str) -> float:
    key = f"img_{issue}_score"
    if key not in row.index:
        return 0.0
    return float(row[key])


def action_from_issue_and_risk(issue: str, risk: float, high_thr: float, critical_thr: float) -> Dict[str, str]:
    if risk >= critical_thr:
        severity = "critical"
        priority = "P1"
    elif risk >= high_thr:
        severity = "high"
        priority = "P2"
    elif risk >= 0.4:
        severity = "medium"
        priority = "P3"
    else:
        severity = "low"
        priority = "P4"

    if risk < 0.4:
        return {
            "severity": severity,
            "priority": priority,
            "action": "Monitor only",
            "maintenance_window": "Routine",
        }

    issue_actions = {
        "dusty": "Cleaning recommended",
        "bird_drop": "Spot cleaning + inspect recurring droppings",
        "cracked": "Panel inspection and likely replacement",
        "panel": "Electrical inspection (possible non-visual anomaly)",
        "hotspot": "Immediate thermal inspection",
        "unknown": "Manual inspection recommended",
    }
    action = issue_actions.get(issue, issue_actions["unknown"])
    window = "Immediate" if severity == "critical" else "Within 3 months" if severity == "high" else "This week"
    return {
        "severity": severity,
        "priority": priority,
        "action": action,
        "maintenance_window": window,
    }


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _concat_query_frames(raw: object) -> pd.DataFrame:
    if isinstance(raw, list):
        frames = [f for f in raw if isinstance(f, pd.DataFrame) and not f.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    if isinstance(raw, pd.DataFrame):
        return raw.copy()
    return pd.DataFrame()


def _to_wide(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    drop_meta = ["result", "table", "_start", "_stop"]
    for col in drop_meta:
        if col in df.columns:
            df = df.drop(columns=col)

    if "_field" in df.columns and "_value" in df.columns:
        id_cols = [c for c in df.columns if c not in {"_field", "_value"}]
        wide = (
            df.pivot_table(index=id_cols, columns="_field", values="_value", aggfunc="last")
            .reset_index()
            .copy()
        )
        wide.columns = [str(c) for c in wide.columns]
        return wide

    return df.copy()


def _build_flux_query(bucket: str, measurement: str, start: datetime, stop: datetime) -> str:
    return _build_flux_query_with_filters(bucket, measurement, start, stop, node_id_filter="", field_filters=[])


def _build_flux_query_with_filters(
    bucket: str,
    measurement: str,
    start: datetime,
    stop: datetime,
    node_id_filter: str,
    field_filters: List[str],
) -> str:
    bucket = bucket.replace('"', '\\"')
    measurement = str(measurement or "").replace('"', '\\"')
    node_id_filter = str(node_id_filter or "").replace('"', '\\"')
    field_filters = [str(field).replace('"', '\\"') for field in field_filters if str(field).strip()]

    field_filter_expr = ""
    if field_filters:
        checks = [f'r["_field"] == "{field}"' for field in field_filters]
        field_filter_expr = f"\n  |> filter(fn: (r) => {' or '.join(checks)})"

    node_filter_expr = ""
    if node_id_filter:
        node_filter_expr = f'\n  |> filter(fn: (r) => r["node_id"] == "{node_id_filter}")'

    measurement_filter_expr = ""
    if measurement:
        measurement_filter_expr = f'\n  |> filter(fn: (r) => r["_measurement"] == "{measurement}")'

    return f'''
from(bucket: "{bucket}")
  |> range(start: time(v: "{_iso_utc(start)}"), stop: time(v: "{_iso_utc(stop)}"))
{measurement_filter_expr}{node_filter_expr}{field_filter_expr}
    |> pivot(rowKey:["_time", "node_id"], columnKey: ["_field"], valueColumn: "_value")
'''.strip()


def _compute_site_summary_l2(
    scored_df: pd.DataFrame,
    site_risk_history: Dict[str, List[float]],
    alpha: float,
    trend_window_size: int,
    site_id: str,
) -> pd.DataFrame:
    if scored_df.empty:
        return pd.DataFrame()

    trend_window_size = max(2, int(trend_window_size))
    rs_now = float(scored_df["node_risk"].mean())
    key = str(site_id)
    site_risk_history.setdefault(key, []).append(rs_now)
    site_risk_history[key] = site_risk_history[key][-trend_window_size:]
    hist = site_risk_history[key]

    rs_prev = float(sum(hist[:-1]) / max(1, len(hist) - 1)) if len(hist) > 1 else rs_now
    d_rs_dt = float((hist[-1] - hist[0]) / max(1, len(hist) - 1)) if len(hist) > 1 else 0.0
    node_count = (
        int(scored_df[RAW_NODE_ID_COL].nunique())
        if RAW_NODE_ID_COL in scored_df.columns
        else int(len(scored_df))
    )

    summary = {
        "site": key,
        "Rs_now": rs_now,
        "Rs_prev": rs_prev,
        "Rs_smoothed": alpha * rs_now + (1.0 - alpha) * rs_prev,
        "dRs_dt": d_rs_dt,
        "Rs_peak": float(scored_df["node_risk"].max()),
        "nodes": node_count,
    }

    issue_share = scored_df["dominant_issue"].value_counts(normalize=True)
    for issue_name, share in issue_share.items():
        summary[str(issue_name)] = float(share)

    return pd.DataFrame([summary])


def _cast_and_filter_features(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    typed = df.copy()
    for col in feature_cols:
        typed[col] = pd.to_numeric(typed[col], errors="coerce")
    return typed.dropna(subset=feature_cols).copy()


def _write_node_points(
    write_api,
    org: str,
    bucket: str,
    measurement: str,
    rows: pd.DataFrame,
    tag_cols: List[str],
) -> int:
    points: List[Point] = []
    for _, row in rows.iterrows():
        ts = row["_time"] if "_time" in row.index else datetime.now(timezone.utc)
        point = Point(measurement).time(ts, WritePrecision.NS)

        for tag in tag_cols:
            if tag in row.index and pd.notna(row[tag]):
                point = point.tag(tag, str(row[tag]))

        point = point.field("node_risk", float(row["node_risk"]))
        point = point.field("binary_fault", int(row["binary_fault"]))
        point = point.field("dominant_issue", str(row["dominant_issue"]))
        point = point.field("issue_confidence", float(row["issue_confidence"]))
        point = point.field("severity", str(row["severity"]))
        point = point.field("priority", str(row["priority"]))
        point = point.field("action", str(row["action"]))
        point = point.field("maintenance_window", str(row["maintenance_window"]))

        points.append(point)

    if points:
        write_api.write(bucket=bucket, org=org, record=points)
    return len(points)


def _write_site_points(
    write_api,
    org: str,
    bucket: str,
    measurement: str,
    site_df: pd.DataFrame,
    event_time: datetime,
) -> int:
    points: List[Point] = []
    for _, row in site_df.iterrows():
        point = Point(measurement).time(event_time, WritePrecision.NS)
        point = point.tag("site", str(row.get("site", "single_site")))

        point = point.field("Rs_now", float(row["Rs_now"]))
        point = point.field("Rs_prev", float(row.get("Rs_prev", row["Rs_now"])))
        point = point.field("Rs_smoothed", float(row.get("Rs_smoothed", row["Rs_now"])))
        point = point.field("dRs_dt", float(row.get("dRs_dt", 0.0)))
        point = point.field("Rs_peak", float(row["Rs_peak"]))
        point = point.field("nodes", int(row["nodes"]))

        for col in site_df.columns:
            if col in {"site", "Rs_now", "Rs_prev", "Rs_smoothed", "dRs_dt", "Rs_peak", "nodes"}:
                continue
            value = row[col]
            if pd.notna(value):
                point = point.field(f"issue_share_{col}", float(value))

        points.append(point)

    if points:
        write_api.write(bucket=bucket, org=org, record=points)
    return len(points)


def _score_batch(
    frame: pd.DataFrame,
    model,
    feature_cols: List[str],
    threshold: float,
    high_thr: float,
    critical_thr: float,
) -> pd.DataFrame:
    validate_columns(frame, feature_cols, "feature")

    clean = _cast_and_filter_features(frame, feature_cols)
    if clean.empty:
        return clean

    X = clean[feature_cols].values
    clean["node_risk"] = model.predict_proba(X)[:, 1]
    clean["binary_fault"] = (clean["node_risk"] >= threshold).astype(int)

    clean["dominant_issue"] = clean.apply(detect_issue, axis=1)
    clean["issue_confidence"] = clean.apply(lambda r: issue_confidence(r, r["dominant_issue"]), axis=1)

    action_cols = clean.apply(
        lambda r: action_from_issue_and_risk(
            issue=r["dominant_issue"],
            risk=float(r["node_risk"]),
            high_thr=high_thr,
            critical_thr=critical_thr,
        ),
        axis=1,
        result_type="expand",
    )

    return pd.concat([clean, action_cols], axis=1)


def _safe_tag_cols(raw: str) -> List[str]:
    return [s.strip() for s in raw.split(",") if s.strip()]


def run_once(
    query_api,
    write_api,
    args: argparse.Namespace,
    model,
    feature_cols: List[str],
    threshold: float,
    start_ts: datetime,
    stop_ts: datetime,
    tag_cols: List[str],
    site_risk_history: Dict[str, List[float]],
) -> int:
    query_fields = list(dict.fromkeys(feature_cols + IMAGE_COLS + ["PR_DEV", "PR_SLOPE"]))
    flux = _build_flux_query_with_filters(
        args.input_bucket,
        args.input_measurement,
        start_ts,
        stop_ts,
        node_id_filter=args.node_id_filter,
        field_filters=query_fields,
    )
    raw = query_api.query_data_frame(flux)
    long_df = _concat_query_frames(raw)
    wide_df = _to_wide(long_df)

    if wide_df.empty:
        logging.info("No telemetry rows returned for window %s -> %s", _iso_utc(start_ts), _iso_utc(stop_ts))
        return 0

    if "_time" in wide_df.columns:
        wide_df["_time"] = pd.to_datetime(wide_df["_time"], utc=True, errors="coerce")
        wide_df = wide_df.dropna(subset=["_time"]).copy()

    scored_df = _score_batch(
        frame=wide_df,
        model=model,
        feature_cols=feature_cols,
        threshold=threshold,
        high_thr=args.high,
        critical_thr=args.critical,
    )
    if scored_df.empty:
        logging.info("Rows received, but none had complete numeric feature set for inference")
        return 0

    site_df = _compute_site_summary_l2(
        scored_df=scored_df,
        site_risk_history=site_risk_history,
        alpha=args.alpha,
        trend_window_size=args.trend_window_size,
        site_id=args.site_id,
    )
    event_time = stop_ts

    if args.dry_run:
        logging.info(
            "DRY-RUN: scored %d node rows, generated %d site rows (no writes)",
            len(scored_df),
            len(site_df),
        )
        return len(scored_df)

    node_written = _write_node_points(
        write_api=write_api,
        org=args.influx_org,
        bucket=args.output_bucket,
        measurement=args.node_measurement,
        rows=scored_df,
        tag_cols=tag_cols,
    )
    site_written = _write_site_points(
        write_api=write_api,
        org=args.influx_org,
        bucket=args.output_bucket,
        measurement=args.site_measurement,
        site_df=site_df,
        event_time=event_time,
    )

    logging.info(
        "Wrote %d node decisions and %d site summary points to bucket '%s'",
        node_written,
        site_written,
        args.output_bucket,
    )
    return node_written


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    with args.config.open("r", encoding="utf-8") as f:
        model_cfg = json.load(f)

    feature_cols = model_cfg.get("feature_cols", [])
    if not feature_cols:
        raise ValueError("model config missing 'feature_cols'")

    threshold = float(model_cfg.get("recommended_threshold", 0.5))
    model = joblib.load(args.model)

    tag_cols = _safe_tag_cols(args.tag_columns)

    logging.info("Fog pipeline starting")
    logging.info("Influx URL: %s", args.influx_url)
    if args.input_measurement:
        logging.info("Input: bucket=%s measurement=%s", args.input_bucket, args.input_measurement)
    else:
        logging.info("Input: bucket=%s measurement=<none>", args.input_bucket)
    logging.info("Output: bucket=%s node=%s site=%s", args.output_bucket, args.node_measurement, args.site_measurement)
    logging.info("Fog site ID: %s", args.site_id)
    logging.info("L2 params: alpha=%.3f trend_window_size=%d", args.alpha, args.trend_window_size)

    last_processed = datetime.now(timezone.utc) - timedelta(minutes=args.bootstrap_lookback_minutes)
    site_risk_history: Dict[str, List[float]] = {}

    with InfluxDBClient(url=args.influx_url, token=args.influx_token, org=args.influx_org) as client:
        query_api = client.query_api()
        write_api = client.write_api(write_options=SYNCHRONOUS)

        while True:
            now_ts = datetime.now(timezone.utc)
            try:
                processed = run_once(
                    query_api=query_api,
                    write_api=write_api,
                    args=args,
                    model=model,
                    feature_cols=feature_cols,
                    threshold=threshold,
                    start_ts=last_processed,
                    stop_ts=now_ts,
                    tag_cols=tag_cols,
                    site_risk_history=site_risk_history,
                )
                logging.info("Polling window complete; scored rows: %d", processed)
                last_processed = now_ts
            except Exception:
                logging.exception("Pipeline iteration failed")

            if args.once:
                break
            time.sleep(max(0.1, args.poll_interval))


if __name__ == "__main__":
    main()
