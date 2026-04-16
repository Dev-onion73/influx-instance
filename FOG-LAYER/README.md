# FOG-LAYER

Deployment-ready files for the fog VM (Debian 13).

## What This Does

The pipeline script:
1. Reads telemetry points from InfluxDB input bucket.
2. Runs your logistic regression risk model loaded from a `.joblib` file.
3. Applies post-model decision logic (severity, action, maintenance window).
4. Writes results to a separate InfluxDB bucket so decisions never mix with raw metrics.

## Clean Folder Layout

```
FOG-LAYER/
├── config/
│   └── .env.example
├── models/
│   ├── logreg_risk_model.joblib
│   └── model_config.json
├── scripts/
│   └── run_fog_pipeline.sh
├── src/
│   └── fog_influx_logreg_pipeline.py
├── README.md
└── requirements.txt
```

- `src/fog_influx_logreg_pipeline.py`: Main runtime script.
- `scripts/run_fog_pipeline.sh`: One-command runner for deployment.
- `config/.env.example`: Environment variable template for Influx settings.
- `models/`: Local model artifacts used by the script.
- `requirements.txt`: Python dependencies for fog VM.

## Setup on Debian 13 VM

```bash
cd FOG-LAYER
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config/.env.example config/.env
```

## Run

Update `config/.env` values, then run:

```bash
bash scripts/run_fog_pipeline.sh
```

Useful modes:

```bash
# Single poll window then exit (good for quick verification)
bash scripts/run_fog_pipeline.sh --once

# Validate inference path without writing points
bash scripts/run_fog_pipeline.sh --once --dry-run
```

## Influx Separation

- Input bucket: `INFLUX_INPUT_BUCKET` (raw metrics)
- Output bucket: `INFLUX_OUTPUT_BUCKET` (node/site decision results)

Keep these different in production.

## Notes

- Default input measurement: `rtu_telemetry`
- Default output measurements:
  - `logreg_node_decision`
  - `logreg_site_summary`
- Tag columns preserved on node output by default: `PLANT_ID,SOURCE_KEY`
