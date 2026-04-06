"""FastAPI backend — schedule generation and data serving."""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

import copy

from .export import generate_schedule_excel
from .models import MACHINE_BY_ID
from .scheduler import ScheduleResult, compute_machine_summary, generate_schedule, generate_schedule_from_jobs
from .scheduler_io import SchedulerConfig, load_jobs_from_excel

app = FastAPI(title="QPI Machine Scheduler V2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory store for schedule results (keyed by schedule_id).
# Bounded to prevent memory growth on long-running deployments.
_MAX_RESULTS = 20
_results: dict[str, dict] = {}


# ── Helpers ─────────────────────────────────────────────────────────

def _result_to_json(result: ScheduleResult, cfg: SchedulerConfig) -> dict:
    """Serialize a ScheduleResult into JSON-friendly dict."""
    entries_json = []
    for e in result.entries:
        entry = {
            "machine_id": e.machine_id,
            "type": e.entry_type,
            "start": e.start.isoformat(),
            "end": e.end.isoformat(),
            "tool_id": e.tool_id,
            "shift": e.shift,
            "group": e.group,
        }
        if e.entry_type == "JOB" and e.job_data:
            entry.update({
                "so_number": e.so_number,
                "customer": e.job_data.get("customer", ""),
                "description": e.job_data.get("description", ""),
                "finished_item": e.job_data.get("finished_item", ""),
                "run_hours": e.job_data.get("run_hours", 0),
                "headcount": e.headcount,
                "remaining_qty": e.job_data.get("remaining_qty", 0),
                "due_date": e.job_data["due_date"].isoformat() if e.job_data.get("due_date") else None,
                "priority_class": e.job_data.get("priority_class", 3),
                "ticket_color": e.job_data.get("ticket_color", ""),
                "is_labeler": e.job_data.get("is_labeler", False),
                "is_bagger": e.job_data.get("is_bagger", False),
                "is_picked": e.job_data.get("is_picked", False),
                "is_in_progress": e.job_data.get("is_in_progress", False),
            })
        if e.entry_type == "NOT_RUNNING":
            entry["idle_type"] = e.idle_type
        if e.crew_from:
            entry["crew_from"] = e.crew_from
        if e.crew_to:
            entry["crew_to"] = e.crew_to
        entries_json.append(entry)

    # Per-machine summary (shared helper eliminates duplication with export.py)
    machine_ids = sorted(set(e.machine_id for e in result.entries))
    machine_summary = {}
    for mid in machine_ids:
        s = compute_machine_summary(result.entries, mid, cfg)
        machine_summary[mid] = {
            "jobs": s.jobs,
            "changeovers": s.changeovers,
            "job_hours": s.job_hours,
            "changeover_hours": s.changeover_hours,
            "no_crew_hours": s.no_crew_hours,
            "total_hours": s.total_hours,
            "utilization": s.utilization,
            "start": s.start.isoformat() if s.start else None,
            "end": s.end.isoformat() if s.end else None,
        }

    crew_json = [
        {
            "time": cm.time.isoformat(),
            "from_machine": cm.from_machine,
            "to_machine": cm.to_machine,
            "headcount": cm.headcount,
            "reason": cm.reason,
        }
        for cm in result.crew_movements
    ]

    return {
        "solver_status": result.solver_status,
        "makespan_hours": round(result.makespan_hours, 1),
        "total_jobs": len([e for e in result.entries if e.entry_type == "JOB"]),
        "total_entries": len(result.entries),
        "skipped_count": len(result.skipped_jobs),
        "schedule": entries_json,
        "machines": machine_summary,
        "crew_movements": crew_json,
        "skipped_jobs": result.skipped_jobs[:50],  # cap for response size
    }


# ── Multi-group helpers ────────────────────────────────────────────

def _assign_jobs_to_groups(
    jobs: list[dict],
    group_defs: dict[str, dict],
) -> tuple[dict[str, list[dict]], list[dict]]:
    """Partition jobs into machine groups by majority eligible machines.

    Returns (group_jobs, extra_skipped).
    """
    # Build machine → group lookup
    machine_to_group: dict[str, str] = {}
    for gname, gdef in group_defs.items():
        for mid in gdef.get("machines", []):
            machine_to_group[mid] = gname

    group_jobs: dict[str, list[dict]] = {g: [] for g in group_defs}
    extra_skipped: list[dict] = []

    for job in jobs:
        eligible = job["eligible_machines"]
        # Count eligible machines per group
        counts: dict[str, int] = {}
        for mid in eligible:
            g = machine_to_group.get(mid)
            if g:
                counts[g] = counts.get(g, 0) + 1

        if not counts:
            extra_skipped.append({
                "reason": "no eligible machines in any group",
                "so_number": job.get("so_number"),
            })
            continue

        # Assign to group with most eligible machines (alphabetical tiebreak)
        best_group = max(counts, key=lambda g: (counts[g], g))
        # Filter eligible_machines to only this group's machines
        group_machines = set(group_defs[best_group].get("machines", []))
        job["eligible_machines"] = [m for m in eligible if m in group_machines]

        if not job["eligible_machines"]:
            extra_skipped.append({
                "reason": "no eligible machines after group filtering",
                "so_number": job.get("so_number"),
            })
            continue

        group_jobs[best_group].append(job)

    return group_jobs, extra_skipped


_STATUS_RANK = {"OPTIMAL": 0, "FEASIBLE": 1, "NO_JOBS": 2, "INFEASIBLE": 3, "UNKNOWN": 4}


def _merge_results(
    group_results: dict[str, ScheduleResult],
    group_defs: dict[str, dict],
) -> tuple[ScheduleResult, dict]:
    """Merge per-group ScheduleResults into one combined result.

    Returns (merged_result, groups_metadata).
    """
    all_entries = []
    all_crew = []
    all_skipped = []
    max_makespan = 0.0
    worst_status = "OPTIMAL"

    groups_meta: dict[str, dict] = {}

    for gname, result in group_results.items():
        # Tag entries with group name for frontend
        for e in result.entries:
            e.group = gname  # type: ignore[attr-defined]
        all_entries.extend(result.entries)
        all_crew.extend(result.crew_movements)
        all_skipped.extend(result.skipped_jobs)

        if result.makespan_hours > max_makespan:
            max_makespan = result.makespan_hours

        if _STATUS_RANK.get(result.solver_status, 4) > _STATUS_RANK.get(worst_status, 0):
            worst_status = result.solver_status

        groups_meta[gname] = {
            "machines": group_defs[gname].get("machines", []),
            "max_concurrent": group_defs[gname].get("max_concurrent", 5),
            "solver_status": result.solver_status,
            "makespan_hours": round(result.makespan_hours, 1),
            "total_jobs": len([e for e in result.entries if e.entry_type == "JOB"]),
        }

    all_entries.sort(key=lambda e: (e.start, e.machine_id))
    all_crew.sort(key=lambda m: m.time)
    # Deduplicate skipped (same job can appear in multiple groups' skipped lists)
    seen_so = set()
    deduped_skipped = []
    for s in all_skipped:
        key = s.get("so_number", "")
        if key and key in seen_so:
            continue
        if key:
            seen_so.add(key)
        deduped_skipped.append(s)

    merged = ScheduleResult(
        entries=all_entries,
        crew_movements=all_crew,
        skipped_jobs=deduped_skipped,
        makespan_hours=max_makespan,
        solver_status=worst_status,
    )
    return merged, groups_meta


# ── Endpoints ───────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/schedule")
async def create_schedule(
    schedule_file: UploadFile = File(...),
    reference_date: str = Form(default=""),
    reference_time: str = Form(default="06:30"),
    max_concurrent: int = Form(default=5),
    include_yellow: bool = Form(default=False),
    include_pink: bool = Form(default=False),
    include_white: bool = Form(default=False),
    shift_config: str = Form(default=""),
    initial_tools: str = Form(default=""),
    priority_boost: bool = Form(default=False),
    minimize_late: bool = Form(default=False),
    disabled_machines: str = Form(default=""),
    hc_penalty_weight: float = Form(default=30),
    total_crew: int = Form(default=0),
    machine_groups: str = Form(default=""),
):
    """Upload Excel, generate schedule, return JSON."""
    # Parse reference date/time
    if reference_date:
        date_part = datetime.strptime(reference_date, "%Y-%m-%d").date()
    else:
        date_part = datetime.now().date()

    time_parts = reference_time.split(":")
    hour, minute = int(time_parts[0]), int(time_parts[1]) if len(time_parts) > 1 else 0
    schedule_start = datetime.combine(date_part, datetime.min.time().replace(hour=hour, minute=minute))

    # Parse disabled machines
    disabled_machines_list = []
    if disabled_machines:
        try:
            disabled_machines_list = json.loads(disabled_machines)
        except json.JSONDecodeError:
            pass

    cfg = SchedulerConfig(
        schedule_start=schedule_start,
        include_yellow=include_yellow,
        include_pink=include_pink,
        include_white=include_white,
        priority_boost=priority_boost,
        minimize_late=minimize_late,
        disabled_machines=disabled_machines_list,
        hc_penalty_weight=hc_penalty_weight,
        total_crew=total_crew,
    )

    # Parse per-machine-per-day shift config if provided
    if shift_config:
        try:
            cfg.shift_schedule = json.loads(shift_config)
            print(f"[shift_config] Received config for {len(cfg.shift_schedule)} machines")
            for mid, days in cfg.shift_schedule.items():
                off_days = [d for d, s in days.items() if not s]
                if off_days:
                    print(f"  {mid}: OFF on {off_days}")
        except json.JSONDecodeError:
            print("[shift_config] Failed to parse JSON")

    if initial_tools:
        try:
            from .helpers import normalize_tool
            raw_tools = json.loads(initial_tools)
            cfg.initial_tools = {}
            for mid, val in raw_tools.items():
                val = str(val).strip()
                if val:
                    try:
                        cfg.initial_tools[mid] = normalize_tool(val)
                    except ValueError:
                        cfg.initial_tools[mid] = val
        except json.JSONDecodeError:
            pass

    # Parse machine groups
    parsed_groups: dict[str, dict] = {}
    if machine_groups:
        try:
            parsed_groups = json.loads(machine_groups)
        except json.JSONDecodeError:
            pass

    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        content = await schedule_file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        if parsed_groups:
            # Multi-group: load once, partition jobs, schedule each group
            jobs, skipped = load_jobs_from_excel(tmp_path, cfg)
            group_jobs, extra_skipped = _assign_jobs_to_groups(jobs, parsed_groups)
            all_skipped = skipped + extra_skipped

            group_results: dict[str, ScheduleResult] = {}
            for gname, gdef in parsed_groups.items():
                g_machines = gdef.get("machines", [])
                g_max = gdef.get("max_concurrent", max_concurrent)
                # Build per-group config: disable all machines NOT in this group
                g_cfg = copy.deepcopy(cfg)
                g_cfg.disabled_machines = [
                    m for m in MACHINE_BY_ID if m not in g_machines
                ]
                g_jobs = group_jobs.get(gname, [])
                group_results[gname] = generate_schedule_from_jobs(
                    g_jobs, all_skipped, g_cfg, g_max
                )

            result, groups_meta = _merge_results(group_results, parsed_groups)
        else:
            # Single-group: existing path
            result = generate_schedule(tmp_path, cfg, max_concurrent=max_concurrent)
            groups_meta = None
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    schedule_id = str(uuid.uuid4())[:8]
    response_data = _result_to_json(result, cfg)
    response_data["schedule_id"] = schedule_id
    if groups_meta:
        response_data["groups"] = groups_meta

    # Store for download (evict oldest if at capacity)
    if len(_results) >= _MAX_RESULTS:
        oldest = next(iter(_results))
        del _results[oldest]
    _results[schedule_id] = {
        "result": result,
        "cfg": cfg,
        "data": response_data,
    }

    return JSONResponse(response_data)


@app.get("/api/schedule/{schedule_id}")
def get_schedule(schedule_id: str):
    """Retrieve a previously generated schedule."""
    stored = _results.get(schedule_id)
    if not stored:
        return JSONResponse({"error": "Schedule not found"}, status_code=404)
    return JSONResponse(stored["data"])


@app.get("/api/schedule/{schedule_id}/export")
def export_schedule(schedule_id: str):
    """Download schedule as an Excel workbook (one sheet per machine)."""
    stored = _results.get(schedule_id)
    if not stored:
        return JSONResponse({"error": "Schedule not found"}, status_code=404)

    wb = generate_schedule_excel(stored["result"], stored["cfg"])
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    wb.save(tmp.name)
    tmp.close()

    return FileResponse(
        tmp.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"schedule_{schedule_id}.xlsx",
        background=BackgroundTask(os.unlink, tmp.name),
    )


# Serve frontend
_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")
