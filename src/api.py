"""FastAPI backend — schedule generation and data serving."""

from __future__ import annotations

import json
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .calendar_utils import staffed_hours_between
from .scheduler import ScheduleResult, generate_schedule
from .scheduler_io import SchedulerConfig

app = FastAPI(title="QPI Machine Scheduler V2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory store for schedule results (keyed by schedule_id)
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
            })
        if e.entry_type == "NOT_RUNNING":
            entry["idle_type"] = e.idle_type
        if e.crew_from:
            entry["crew_from"] = e.crew_from
        if e.crew_to:
            entry["crew_to"] = e.crew_to
        entries_json.append(entry)

    # Per-machine summary
    from collections import defaultdict
    machine_summary = {}
    by_machine = defaultdict(list)
    for e in result.entries:
        by_machine[e.machine_id].append(e)

    # Build a set of (machine, time) where crew is actually present.
    # Crew is present when: running a JOB, or doing a TOOL_SWAP (self-service).
    # Crew is NOT present during: CHANGEOVER (crew freed), NOT_RUNNING (no crew).
    # Idle = crew present but no job. Currently only IDLE_CREW entries
    # or gaps where crew_from is set would count.

    for mid in sorted(by_machine):
        entries = by_machine[mid]
        spd = cfg.get_day_shift_map(mid)
        jobs = [e for e in entries if e.entry_type == "JOB"]
        cos = [e for e in entries if e.entry_type in ("CHANGEOVER", "TOOL_SWAP")]
        # Only count idle when crew is present but has no work (IDLE_CREW type)
        idle_crew = [
            e for e in entries
            if e.entry_type == "NOT_RUNNING" and e.idle_type == "CREW_WAITING"
        ]
        no_crew = [
            e for e in entries
            if e.entry_type == "NOT_RUNNING" and e.idle_type != "CREW_WAITING"
        ]

        job_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in jobs)
        co_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in cos)
        idle_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in idle_crew)
        no_crew_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in no_crew)
        total = job_hrs + co_hrs + idle_hrs

        sorted_entries = sorted(entries, key=lambda e: e.start)
        machine_summary[mid] = {
            "jobs": len(jobs),
            "changeovers": len(cos),
            "idle_blocks": len(idle_crew),
            "job_hours": round(job_hrs, 1),
            "changeover_hours": round(co_hrs, 1),
            "idle_hours": round(idle_hrs, 1),
            "no_crew_hours": round(no_crew_hrs, 1),
            "total_hours": round(total, 1),
            "utilization": round(job_hrs / total * 100, 1) if total > 0 else 0,
            "start": sorted_entries[0].start.isoformat() if sorted_entries else None,
            "end": sorted_entries[-1].end.isoformat() if sorted_entries else None,
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

    cfg = SchedulerConfig(
        schedule_start=schedule_start,
        include_yellow=include_yellow,
        include_pink=include_pink,
        include_white=include_white,
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

    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        content = await schedule_file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        result = generate_schedule(tmp_path, cfg, max_concurrent=max_concurrent)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    schedule_id = str(uuid.uuid4())[:8]
    response_data = _result_to_json(result, cfg)
    response_data["schedule_id"] = schedule_id

    # Store for download
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


# Serve frontend
_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")
