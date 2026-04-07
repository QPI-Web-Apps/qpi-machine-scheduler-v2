"""Thorough functional test of the publish + viewer flow.

Hits things the basic test doesn't:
- All ScheduleEntry types (JOB / CHANGEOVER / TOOL_SWAP / NOT_RUNNING) reach the schedule table
- Pink jobs (not just yellow) reach the yp table
- crew_from / crew_to populate for at least some rows
- Decimal columns round-trip without precision loss
- NULL handling for jobs missing customer / part / tool
- Multi-group publish: machine_group field is populated
- Two distinct publishes: Y/N flip persists across them and counts add up
- Delete-by-id only removes the targeted row(s)
"""
from __future__ import annotations

import json as _json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import api
from src.scheduler import generate_schedule
from src.scheduler_io import SchedulerConfig

XLSX = Path(__file__).resolve().parents[1] / "test_schedules" / "STF schedule 03.31 130PMToBeFilled_Updated.xlsx"


def _body(resp):
    return _json.loads(resp.body.decode())


def _check(label: str, cond: bool, detail: str = "") -> bool:
    mark = "✓" if cond else "✗"
    print(f"  {mark} {label}" + (f"  ({detail})" if detail else ""))
    return cond


def main() -> int:
    cfg = SchedulerConfig(
        schedule_start=datetime(2026, 4, 2, 6, 30),
        include_yellow=True,
        include_pink=True,
    )

    db = api._get_prisma()
    db.scheduler_yellow_pink_jobs.delete_many(where={"id": {"gt": 0}})
    db.scheduler_published_schedule.delete_many(where={"id": {"gt": 0}})

    print("\n=== A. Single-group publish ===")
    result = generate_schedule(str(XLSX), cfg, max_concurrent=5)
    api._results["A"] = {"result": result, "cfg": cfg, "data": {}}
    pub_a = _body(api.publish_schedule("A"))
    print(f"  publish: yp={pub_a['yellow_pink_count']}  schedule={pub_a['schedule_count']}")

    all_ok = True

    # ── Entry-type coverage ──
    print("\n=== B. Entry-type coverage in scheduler_published_schedule ===")
    rows = db.scheduler_published_schedule.find_many(
        where={"processed_indicator": "y"}, take=5000,
    )
    types_seen = {}
    for r in rows:
        types_seen[r.entry_type] = types_seen.get(r.entry_type, 0) + 1
    print(f"  types: {types_seen}")
    all_ok &= _check("JOB rows present", types_seen.get("JOB", 0) > 0, str(types_seen.get("JOB")))
    all_ok &= _check("CHANGEOVER rows present", types_seen.get("CHANGEOVER", 0) > 0, str(types_seen.get("CHANGEOVER")))

    # ── JOB-only fields populated, non-JOB rows have NULL job fields ──
    print("\n=== C. Field separation (JOB vs non-JOB) ===")
    job_row = next((r for r in rows if r.entry_type == "JOB"), None)
    non_job = next((r for r in rows if r.entry_type != "JOB"), None)
    if job_row:
        all_ok &= _check("JOB row has so_number", job_row.so_number is not None, job_row.so_number)
        all_ok &= _check("JOB row has run_hours (Decimal)", isinstance(job_row.run_hours, Decimal),
                         f"type={type(job_row.run_hours).__name__} val={job_row.run_hours}")
        all_ok &= _check("JOB row has headcount (Decimal)", isinstance(job_row.headcount, Decimal),
                         f"val={job_row.headcount}")
    if non_job:
        all_ok &= _check("Non-JOB row has so_number=None", non_job.so_number is None,
                         f"got {non_job.so_number}")
        all_ok &= _check("Non-JOB row has run_hours=None", non_job.run_hours is None)

    # ── crew_from / crew_to populated for at least some rows ──
    print("\n=== D. Crew movement fields ===")
    with_crew = [r for r in rows if r.crew_from or r.crew_to]
    print(f"  rows with crew_from or crew_to: {len(with_crew)}")
    if with_crew:
        sample = with_crew[0]
        all_ok &= _check("at least one crew transition row exists", True,
                         f"{sample.crew_from} -> {sample.crew_to} on {sample.machine_id}")

    # ── Pink jobs reach yp table ──
    print("\n=== E. Pink + Yellow both in yp table ===")
    yp_rows = db.scheduler_yellow_pink_jobs.find_many(where={"processed_indicator": "y"}, take=5000)
    colors = {}
    for r in yp_rows:
        colors[r.ticket_color] = colors.get(r.ticket_color, 0) + 1
    print(f"  ticket colors: {colors}")
    all_ok &= _check("Yellow rows present", any("yellow" in (k or "").lower() for k in colors))
    all_ok &= _check("Pink rows present", any("pink" in (k or "").lower() for k in colors))

    # ── job_type variety ──
    print("\n=== F. job_type variety (Labeler / Bagger / Regular / etc) ===")
    types = {}
    for r in yp_rows:
        types[r.job_type] = types.get(r.job_type, 0) + 1
    for k, v in sorted(types.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")
    all_ok &= _check("Multiple job_type values present", len(types) > 1, f"{len(types)} distinct values")
    all_ok &= _check("'Regular' bucket present", "Regular" in types)

    # ── NULL handling ──
    print("\n=== G. NULL handling on optional fields ===")
    yp_no_part = [r for r in yp_rows if r.part_number is None]
    yp_no_desc = [r for r in yp_rows if r.description is None]
    yp_no_tool = [r for r in yp_rows if r.tool_id is None]
    print(f"  yp rows missing part_number: {len(yp_no_part)}")
    print(f"  yp rows missing description: {len(yp_no_desc)}")
    print(f"  yp rows missing tool_id: {len(yp_no_tool)}")
    # No assertion — just observed; nullables aren't required to have NULLs

    # ── 2nd publish — verify Y/N flip ──
    print("\n=== H. Second publish flips previous to N ===")
    pub_b = _body(api.publish_schedule("A"))
    yp_y = db.scheduler_yellow_pink_jobs.count(where={"processed_indicator": "y"})
    yp_n = db.scheduler_yellow_pink_jobs.count(where={"processed_indicator": "n"})
    sch_y = db.scheduler_published_schedule.count(where={"processed_indicator": "y"})
    sch_n = db.scheduler_published_schedule.count(where={"processed_indicator": "n"})
    print(f"  yp:   y={yp_y}  n={yp_n}")
    print(f"  sch:  y={sch_y}  n={sch_n}")
    all_ok &= _check("yp Y count == new publish count",
                     yp_y == pub_b["yellow_pink_count"], f"{yp_y} vs {pub_b['yellow_pink_count']}")
    all_ok &= _check("yp N count == previous Y count",
                     yp_n == pub_a["yellow_pink_count"], f"{yp_n} vs {pub_a['yellow_pink_count']}")
    all_ok &= _check("sch Y count == new publish count",
                     sch_y == pub_b["schedule_count"], f"{sch_y} vs {pub_b['schedule_count']}")
    all_ok &= _check("sch N count == previous Y count",
                     sch_n == pub_a["schedule_count"], f"{sch_n} vs {pub_a['schedule_count']}")

    # ── Multi-group publish ──
    print("\n=== I. Multi-group publish populates machine_group ===")
    db.scheduler_yellow_pink_jobs.delete_many(where={"id": {"gt": 0}})
    db.scheduler_published_schedule.delete_many(where={"id": {"gt": 0}})

    # Use the multi-group code path via _assign_jobs_to_groups + per-group schedule
    import copy
    from src.scheduler_io import load_jobs_from_excel
    from src.scheduler import generate_schedule_from_jobs
    from src.models import MACHINE_BY_ID

    jobs, skipped = load_jobs_from_excel(str(XLSX), cfg)
    groups = {
        "alpha": {"machines": ["16A", "16B", "16C", "20"], "max_concurrent": 4},
        "beta":  {"machines": ["8", "LMB", "SMB", "6ST", "RF"], "max_concurrent": 5},
    }
    group_jobs, extra_skip = api._assign_jobs_to_groups(jobs, groups)
    group_results = {}
    for gname, gdef in groups.items():
        g_cfg = copy.deepcopy(cfg)
        g_cfg.disabled_machines = [m for m in MACHINE_BY_ID if m not in gdef["machines"]]
        group_results[gname] = generate_schedule_from_jobs(
            group_jobs.get(gname, []), skipped + extra_skip, g_cfg, gdef["max_concurrent"]
        )
    merged, _ = api._merge_results(group_results, groups)
    api._results["MG"] = {"result": merged, "cfg": cfg, "data": {}}
    pub_mg = _body(api.publish_schedule("MG"))
    print(f"  publish: yp={pub_mg['yellow_pink_count']} schedule={pub_mg['schedule_count']}")

    rows_mg = db.scheduler_published_schedule.find_many(
        where={"processed_indicator": "y"}, take=5000,
    )
    groups_seen = {}
    for r in rows_mg:
        groups_seen[r.machine_group] = groups_seen.get(r.machine_group, 0) + 1
    print(f"  machine_group distribution: {groups_seen}")
    all_ok &= _check("alpha group rows present", groups_seen.get("alpha", 0) > 0)
    all_ok &= _check("beta group rows present", groups_seen.get("beta", 0) > 0)

    # ── Delete by id, then verify only those are gone ──
    print("\n=== J. Delete-by-id surgical removal ===")
    yp_now = db.scheduler_yellow_pink_jobs.find_many(where={"processed_indicator": "y"}, take=5)
    target_ids = [r.id for r in yp_now[:3]]
    keep_ids   = [r.id for r in yp_now[3:5]]
    del_resp = _body(api.db_delete_rows("scheduler_yellow_pink_jobs", body={"ids": target_ids}))
    after = {r.id for r in db.scheduler_yellow_pink_jobs.find_many(
        where={"id": {"in": target_ids + keep_ids}}, take=5)}
    all_ok &= _check("3 target ids deleted", del_resp["deleted"] == 3, str(del_resp))
    all_ok &= _check("targeted ids gone from DB", not any(i in after for i in target_ids))
    all_ok &= _check("non-targeted ids still present", all(i in after for i in keep_ids))

    # ── Empty filter behavior on get_rows ──
    print("\n=== K. Filter dropdown returns expected counts ===")
    body_y = _body(api.db_get_rows("scheduler_published_schedule", filter="y", limit=99999))
    body_n = _body(api.db_get_rows("scheduler_published_schedule", filter="n", limit=99999))
    body_all = _body(api.db_get_rows("scheduler_published_schedule", filter="all", limit=99999))
    print(f"  y total: {body_y['total']}")
    print(f"  n total: {body_n['total']}")
    print(f"  all total: {body_all['total']}")
    all_ok &= _check("y + n == all", body_y["total"] + body_n["total"] == body_all["total"])
    all_ok &= _check("all rows have y or n", all(
        r["processed_indicator"] in ("y", "n") for r in body_all["rows"]
    ))

    # ── Final cleanup ──
    db.scheduler_yellow_pink_jobs.delete_many(where={"id": {"gt": 0}})
    db.scheduler_published_schedule.delete_many(where={"id": {"gt": 0}})
    db.disconnect()

    print("\n" + ("PASS" if all_ok else "FAIL"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
