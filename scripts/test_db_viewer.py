"""End-to-end test of the DB viewer endpoints.

Publishes a schedule, then exercises every viewer endpoint and asserts
the results. Also confirms the table allowlist blocks foreign tables.
"""
from __future__ import annotations

import json as _json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import api
from src.scheduler import generate_schedule
from src.scheduler_io import SchedulerConfig

TEST_XLSX = Path(__file__).resolve().parents[1] / "test_schedules" / "STF schedule 03.31 130PMToBeFilled_Updated.xlsx"


def _body(resp):
    return _json.loads(resp.body.decode())


def main() -> int:
    cfg = SchedulerConfig(
        schedule_start=datetime(2026, 4, 2, 6, 30),
        include_yellow=True,
        include_pink=True,
    )
    print("Generating + publishing a fresh schedule…")
    result = generate_schedule(str(TEST_XLSX), cfg, max_concurrent=5)
    schedule_id = "viewtest"
    api._results[schedule_id] = {"result": result, "cfg": cfg, "data": {}}

    db = api._get_prisma()
    db.scheduler_yellow_pink_jobs.delete_many(where={"id": {"gt": 0}})
    db.scheduler_published_schedule.delete_many(where={"id": {"gt": 0}})

    pub = _body(api.publish_schedule(schedule_id))
    print(f"  publish: {pub}")

    ok = True

    # ── list_tables ──
    print("\n[list_tables]")
    listed = _body(api.db_list_tables())
    print(f"  {listed}")
    by_name = {t["name"]: t for t in listed["tables"]}
    if set(by_name) != {"scheduler_yellow_pink_jobs", "scheduler_published_schedule"}:
        print(f"  FAIL: unexpected table set: {set(by_name)}")
        ok = False
    if by_name["scheduler_yellow_pink_jobs"]["rows_y"] != pub["yellow_pink_count"]:
        print("  FAIL: yp rows_y mismatch")
        ok = False
    if by_name["scheduler_published_schedule"]["rows_y"] != pub["schedule_count"]:
        print("  FAIL: sched rows_y mismatch")
        ok = False

    # ── get_rows: filter=y ──
    print("\n[get_rows yp filter=y]")
    rows = _body(api.db_get_rows("scheduler_yellow_pink_jobs", filter="y", limit=10))
    print(f"  total={rows['total']}  returned={rows['returned']}  columns={len(rows['columns'])}")
    if rows["total"] != pub["yellow_pink_count"]:
        print("  FAIL: yp filter=y total mismatch")
        ok = False
    if rows["returned"] != min(10, rows["total"]):
        print(f"  FAIL: yp returned should be min(10, total)")
        ok = False
    if rows["rows"]:
        first = rows["rows"][0]
        print(f"  sample row keys: {sorted(first.keys())}")
        if first.get("processed_indicator") != "y":
            print(f"  FAIL: filter=y returned a non-y row")
            ok = False

    # ── get_rows: filter=all ──
    print("\n[get_rows yp filter=all]")
    rows_all = _body(api.db_get_rows("scheduler_yellow_pink_jobs", filter="all", limit=99999))
    print(f"  total={rows_all['total']}  returned={rows_all['returned']}")

    # ── get_rows: invalid table (allowlist check) ──
    print("\n[get_rows allowlist check]")
    blocked = api.db_get_rows("po_acknowledgement", filter="all", limit=10)
    print(f"  status={blocked.status_code}  body={_body(blocked)}")
    if blocked.status_code != 404:
        print("  FAIL: allowlist did not block po_acknowledgement read")
        ok = False

    # ── delete by id ──
    print("\n[delete some yp rows by id]")
    yp_rows_y = api.db_get_rows("scheduler_yellow_pink_jobs", filter="y", limit=99999)
    yp_data = _body(yp_rows_y)
    if yp_data["rows"]:
        ids_to_delete = [r["id"] for r in yp_data["rows"][:3]]
        del_resp = _body(api.db_delete_rows("scheduler_yellow_pink_jobs", body={"ids": ids_to_delete}))
        print(f"  deleted by ids {ids_to_delete}: {del_resp}")
        if del_resp.get("deleted") != 3:
            print("  FAIL: expected to delete 3 rows")
            ok = False
        # Verify they're gone
        remaining = api.db_get_rows("scheduler_yellow_pink_jobs", filter="y", limit=99999)
        rem = _body(remaining)
        remaining_ids = {r["id"] for r in rem["rows"]}
        if any(i in remaining_ids for i in ids_to_delete):
            print("  FAIL: deleted ids still present")
            ok = False

    # ── delete blocked table ──
    print("\n[delete allowlist check]")
    blocked = api.db_delete_rows("po_acknowledgement", body={"all": True})
    print(f"  status={blocked.status_code}  body={_body(blocked)}")
    if blocked.status_code != 404:
        print("  FAIL: allowlist did not block po_acknowledgement delete")
        ok = False

    # ── delete all on yp table ──
    print("\n[delete all yp rows]")
    del_all = _body(api.db_delete_rows("scheduler_yellow_pink_jobs", body={"all": True}))
    print(f"  {del_all}")
    after = _body(api.db_get_rows("scheduler_yellow_pink_jobs", filter="all", limit=10))
    if after["total"] != 0:
        print(f"  FAIL: after delete-all, total should be 0, got {after['total']}")
        ok = False

    # Cleanup the schedule table too
    api.db_delete_rows("scheduler_published_schedule", body={"all": True})

    db.disconnect()
    print("\n" + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
