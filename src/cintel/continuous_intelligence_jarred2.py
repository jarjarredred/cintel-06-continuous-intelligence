"""
continuous_intelligence_case.py - Project script (example).

Author: Denise Case & Jarred Gastreich
Date: 2026-03

System Metrics Data

- Data represents recent observations from a monitored system.
- Each row represents one observation of system activity.

- The CSV file includes these columns:
  - requests: number of requests handled
  - errors: number of failed requests
  - total_latency_ms: total response time in milliseconds

Purpose

- Read system metrics from a CSV file.
- Apply multiple continuous intelligence techniques learned earlier:
  - anomaly detection
  - signal design
  - simple drift-style reasoning
- Summarize the system's current state.
- Save the resulting system assessment as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- What signals best summarize the health of a system?
- When multiple indicators change at once, how should we interpret the system's state?
- How can monitoring data support operational decisions?

Paths (relative to repo root)

    INPUT FILE: data/system_metrics_case.csv
    OUTPUT FILE: artifacts/system_assessment_case.csv

Terminal command to run this file from the root project folder

    uv run python -m cintel.continuous_intelligence_case

OBS:
  Don't edit this file - it should remain a working example.
  Use as much of this code as you can when creating your own pipeline script,
  and change the logic to match the needs of your project.
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P6", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

DATA_FILE: Final[Path] = DATA_DIR / "system_metrics_case.csv"
OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "system_assessment_jarred2.csv"

# === DEFINE THRESHOLDS ===

# Thresholds for Efficiency (ms per request)
IDEAL_COST: Final[float] = 30.0
CRITICAL_COST: Final[float] = 45.0


def main() -> None:
    log_header(LOG, "SYSTEM EFFICIENCY PIPELINE")

    # 1. Load the provided dataset
    # (Simulating reading the data you provided)
    data = {
        "requests": [
            120,
            122,
            125,
            128,
            130,
            132,
            135,
            138,
            140,
            142,
            145,
            148,
            150,
            152,
            155,
            158,
            160,
            162,
            165,
            168,
            170,
            172,
            175,
            178,
            180,
            182,
            185,
            188,
            190,
            195,
            200,
            205,
            210,
            205,
            200,
            195,
            190,
            185,
            180,
            175,
            170,
            168,
            165,
            160,
            155,
            150,
            148,
            145,
            142,
            140,
            138,
            135,
            132,
            130,
            128,
            125,
            122,
            120,
            118,
            115,
        ],
        "errors": [
            2,
            2,
            2,
            2,
            3,
            3,
            3,
            3,
            3,
            3,
            3,
            3,
            4,
            4,
            4,
            4,
            4,
            4,
            5,
            5,
            5,
            5,
            6,
            6,
            6,
            7,
            7,
            8,
            8,
            9,
            10,
            11,
            12,
            10,
            9,
            8,
            7,
            6,
            5,
            5,
            4,
            4,
            4,
            3,
            3,
            3,
            3,
            3,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            1,
        ],
        "total_latency_ms": [
            3600,
            3650,
            3720,
            3800,
            3900,
            3950,
            4000,
            4100,
            4200,
            4300,
            4400,
            4500,
            4700,
            4800,
            4900,
            5000,
            5100,
            5200,
            5400,
            5600,
            5800,
            5900,
            6100,
            6300,
            6500,
            6800,
            7000,
            7300,
            7600,
            8000,
            8500,
            9000,
            9600,
            9000,
            8500,
            8000,
            7600,
            7200,
            6800,
            6500,
            6200,
            6000,
            5800,
            5500,
            5200,
            5000,
            4800,
            4600,
            4400,
            4200,
            4050,
            3900,
            3800,
            3700,
            3650,
            3600,
            3550,
            3500,
            3450,
            3400,
        ],
    }
    df = pl.DataFrame(data)

    # 2. Design Efficiency Signals
    # We calculate how many ms of latency each request 'costs'
    LOG.info("STEP 2: Calculating latency cost and rolling trends...")
    df = df.with_columns(
        [
            (pl.col("total_latency_ms") / pl.col("requests")).alias(
                "latency_cost_per_req"
            ),
            (pl.col("errors") / pl.col("requests")).alias("error_rate"),
        ]
    )

    # 3. Detect Drift (Rolling Average)
    # This helps see if the system is getting progressively worse
    df = df.with_columns(
        [
            pl.col("latency_cost_per_req")
            .rolling_mean(window_size=3)
            .alias("rolling_cost_avg")
        ]
    )

    # 4. Complex Assessment Logic
    LOG.info("STEP 4: Categorizing system health based on load efficiency...")
    df = df.with_columns(
        pl.when(pl.col("latency_cost_per_req") > CRITICAL_COST)
        .then(pl.lit("OVERLOADED"))
        .when(pl.col("latency_cost_per_req") > IDEAL_COST)
        .then(pl.lit("STRETCHED"))
        .otherwise(pl.lit("OPTIMAL"))
        .alias("efficiency_state")
    )

    # 5. Summarize Results
    peak_load = df.filter(pl.col("requests") == pl.col("requests").max())
    LOG.info(f"Analysis complete. Peak load state: {peak_load['efficiency_state'][0]}")

    # Save artifact
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_csv(OUTPUT_FILE)
    LOG.info(f"Assessment saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
