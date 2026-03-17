"""main.py — Pipeline orchestrator.

Executes BR_01 → BR_02 → BR_03 → BR_04 in sequence.
Daily 6:00 AM ET via GitHub Actions cron (0 10 * * *).

Usage: python -m src.main
"""
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

def run_pipeline():
    """Execute the full daily intelligence digest pipeline."""
    et = ZoneInfo("America/New_York")
    pipeline_start = datetime.now(et)
    print(f"Pipeline started: {pipeline_start.isoformat()}")

    # TODO: Implement BR_01 → BR_02 → BR_03 → BR_04 sequence
    # Each BR returns runtime counters and error lists
    # BR_04 aggregates all counters and sends completion email

    pipeline_end = datetime.now(et)
    print(f"Pipeline completed: {pipeline_end.isoformat()}")
    return 0  # exit code: 0 = clean, 1 = errors

if __name__ == "__main__":
    sys.exit(run_pipeline())
