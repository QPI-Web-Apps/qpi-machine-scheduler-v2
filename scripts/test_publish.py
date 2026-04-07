"""End-to-end test of the publish endpoint.

Generates a schedule from the test Excel, stuffs it into the in-memory store
the same way the create_schedule endpoint does, then calls publish_schedule()
twice and inspects the DB after each call.

Run from repo root with .venv active:
    python scripts/test_publish.py
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Make src importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import api
from src.scheduler import generate_schedule
from src.scheduler_io import SchedulerConfig

TEST_XLSX = Path(__file__).resolve().parents[1] / "test_schedules" / "STF schedule 03.31 130PMToBeFilled_Updated.xlsx"


def main() -> int:
    if not TEST_XLSX.exists():
        print(f"FAIL: test xlsx missing at {TEST_XLSX}")
        return 1

    cfg = SchedulerConfig(
        schedule_start=datetime(2026, 4, 2, 6, 30),
        include_yellow=True,   # so the yellow_pink table actually receives rows
        include_pink=True,
    )
    print("Generating schedule from test Excel…")
    result = generate_schedule(str(TEST_XLSX), cfg, max_concurrent=5)
    print(f"  solver_status={result.solver_status}  entries={len(result.entries)}")

    schedule_id = "test0001"
    api._results[schedule_id] = {"result": result, "cfg": cfg, "data": {}}

    # Clean any leftover test data so the assertions math is meaningful
    db_pre = api._get_prisma()
    db_pre.scheduler_yellow_pink_jobs.delete_many(where={"processed_indicator": {"not": ""}})
    db_pre.scheduler_published_schedule.delete_many(where={"processed_indicator": {"not": ""}})

    import json as _json

    def _check_publish_ok(resp, label):
        body = _json.loads(resp.body.decode())
        print(f"  response: {body}")
        if not body.get("ok"):
            print(f"  FAIL [{label}]: publish did not return ok=true")
            sys.exit(2)
        return body

    # ── First publish ──
    print("\n[1st publish]")
    resp = api.publish_schedule(schedule_id)
    body1 = _check_publish_ok(resp, "1st")

    # Inspect DB
    db = api._get_prisma()
    yp_y = db.scheduler_yellow_pink_jobs.count(where={"processed_indicator": "y"})
    yp_n = db.scheduler_yellow_pink_jobs.count(where={"processed_indicator": "n"})
    sch_y = db.scheduler_published_schedule.count(where={"processed_indicator": "y"})
    sch_n = db.scheduler_published_schedule.count(where={"processed_indicator": "n"})
    print(f"  scheduler_yellow_pink_jobs:   y={yp_y}  n={yp_n}")
    print(f"  scheduler_published_schedule: y={sch_y}  n={sch_n}")

    # ── Second publish — verifies the previous batch is flipped to 'n' ──
    print("\n[2nd publish]")
    resp = api.publish_schedule(schedule_id)
    body2 = _check_publish_ok(resp, "2nd")

    yp_y2 = db.scheduler_yellow_pink_jobs.count(where={"processed_indicator": "y"})
    yp_n2 = db.scheduler_yellow_pink_jobs.count(where={"processed_indicator": "n"})
    sch_y2 = db.scheduler_published_schedule.count(where={"processed_indicator": "y"})
    sch_n2 = db.scheduler_published_schedule.count(where={"processed_indicator": "n"})
    print(f"  scheduler_yellow_pink_jobs:   y={yp_y2}  n={yp_n2}")
    print(f"  scheduler_published_schedule: y={sch_y2}  n={sch_n2}")

    # ── Sanity assertions ──
    ok = True
    if sch_y == 0:
        print(f"  FAIL: 1st publish wrote zero schedule rows (result had {len(result.entries)} entries)")
        ok = False
    if body1["schedule_count"] != sch_y:
        print(f"  FAIL: response said schedule_count={body1['schedule_count']} but DB has {sch_y}")
        ok = False
    if body1["yellow_pink_count"] != yp_y:
        print(f"  FAIL: response said yellow_pink_count={body1['yellow_pink_count']} but DB has {yp_y}")
        ok = False
    if yp_y2 != yp_y:
        print(f"  FAIL: y-count for yellow_pink should match between publishes ({yp_y} vs {yp_y2})")
        ok = False
    if yp_n2 != yp_y:
        print(f"  FAIL: after 2nd publish, n-count should equal previous y-count ({yp_y} vs {yp_n2})")
        ok = False
    if sch_y2 != sch_y:
        print(f"  FAIL: y-count for schedule should match between publishes ({sch_y} vs {sch_y2})")
        ok = False
    if sch_n2 != sch_y:
        print(f"  FAIL: after 2nd publish, n-count should equal previous y-count ({sch_y} vs {sch_n2})")
        ok = False

    # Spot-check a row
    print("\nSample yellow_pink row (latest publish):")
    sample = db.scheduler_yellow_pink_jobs.find_first(where={"processed_indicator": "y"})
    if sample:
        print(f"  so={sample.so_number}  part={sample.part_number}  machine={sample.machine}  color={sample.ticket_color}  type={sample.job_type}")
    else:
        print("  (no yellow/pink rows found — test data may have none)")

    print("\nSample schedule row (latest publish):")
    sample = db.scheduler_published_schedule.find_first(
        where={"processed_indicator": "y", "entry_type": "JOB"}
    )
    if sample:
        print(f"  machine={sample.machine_id}  so={sample.so_number}  start={sample.start_time}  hc={sample.headcount}")

    db.disconnect()
    print("\n" + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
