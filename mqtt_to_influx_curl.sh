#!/usr/bin/env bash
set -euo pipefail

# MQTT source
MQTT_BROKER="${MQTT_BROKER:-10.0.40.101}"
MQTT_PORT="${MQTT_PORT:-1883}"
MQTT_TOPIC="${MQTT_TOPIC:-solar/rtu/#}"

# Influx destination
INFLUXDB_URL="${INFLUXDB_URL:-http://10.0.40.101:8086}"
INFLUXDB_ORG="${INFLUXDB_ORG:-SOLAR-FOG}"
INFLUXDB_BUCKET="${INFLUXDB_BUCKET:-metrics}"
MEASUREMENT="${MEASUREMENT:-solar_rtu}"

if [[ -z "${INFLUXDB_TOKEN:-}" ]]; then
  echo "INFLUXDB_TOKEN is required" >&2
  exit 1
fi

if ! command -v mosquitto_sub >/dev/null 2>&1; then
  echo "mosquitto_sub not found. Install mosquitto-clients." >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq not found. Install jq." >&2
  exit 1
fi

echo "Subscribing MQTT: ${MQTT_BROKER}:${MQTT_PORT} topic=${MQTT_TOPIC}"
echo "Writing Influx: ${INFLUXDB_URL} org=${INFLUXDB_ORG} bucket=${INFLUXDB_BUCKET} measurement=${MEASUREMENT}"

jq_filter='def sane_key: gsub("[ ,=]"; "_");
def esc_str: gsub("\\\\"; "\\\\\\\\") | gsub("\""; "\\\"");
def to_ns:
  if . == null then (now * 1000000000 | floor)
  elif (type == "number") then
    if . > 1000000000000 then . else (. * 1000000000 | floor) end
  elif (type == "string") then
    (if test("Z$|[+-][0-9]{2}:[0-9]{2}$") then . else . + "Z" end) as $t
    | (try ($t | fromdateiso8601 * 1000000000 | floor) catch (now * 1000000000 | floor))
  else
    (now * 1000000000 | floor)
  end;

. as $p
| ($p.node_id // "unknown" | tostring | gsub("[ ,=]"; "_")) as $node
| ($p.timestamp // null | to_ns) as $ts
| [
    ($p | to_entries[]
      | select(.key != "node_id" and .key != "timestamp" and .key != "measurement" and .value != null)
      | if (.value | type) == "number" then
          "\(.key | sane_key)=\(.value)"
        elif (.value | type) == "boolean" then
          "\(.key | sane_key)=\(.value)"
        elif (.value | type) == "string" then
          "\(.key | sane_key)=\"\(.value | esc_str)\""
        else
          empty
        end)
  ] as $fields
| ($fields + (if ($fields | length) == 0 then ["ingest_ok=1i"] else [] end)) as $safe_fields
| "\($p.measurement // env.MEASUREMENT),node_id=\($node) \($safe_fields | join(",")) \($ts)"'

mosquitto_sub -h "$MQTT_BROKER" -p "$MQTT_PORT" -t "$MQTT_TOPIC" | while IFS= read -r payload; do
  [[ -z "$payload" ]] && continue

  if ! line_protocol="$(printf '%s' "$payload" | jq -er "$jq_filter" 2>/dev/null)"; then
    echo "skip invalid JSON payload: $payload" >&2
    continue
  fi

  resp="$(curl -sS -w "\n%{http_code}" -X POST \
    "${INFLUXDB_URL}/api/v2/write?org=${INFLUXDB_ORG}&bucket=${INFLUXDB_BUCKET}&precision=ns" \
    -H "Authorization: Token ${INFLUXDB_TOKEN}" \
    -H "Content-Type: text/plain; charset=utf-8" \
    --data-binary "$line_protocol")"

  status="${resp##*$'\n'}"
  body="${resp%$'\n'*}"

  if [[ "$status" =~ ^2 ]]; then
    echo "written: $line_protocol"
  else
    echo "write failed (HTTP $status): $body" >&2
    echo "line: $line_protocol" >&2
  fi
done
