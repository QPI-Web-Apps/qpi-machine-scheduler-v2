"""FastAPI backend — schedule generation and data serving."""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import Body, FastAPI, File, Form, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask

import copy

from .export import generate_schedule_excel
from .models import MACHINE_BY_ID
from .scheduler import ScheduleEntry, ScheduleResult, compute_machine_summary, generate_schedule, generate_schedule_from_jobs
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

# Used by both create_schedule (full yp list) and publish_schedule (filtering
# scheduled JOB entries to yellow/pink ones).
_YELLOW_PINK_RE = re.compile(r"yellow|pink", re.IGNORECASE)


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
    priority_boost: bool = Form(default=True),
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

        # Always load the FULL yellow/pink list regardless of the user's
        # filter choice. The publish flow writes every yellow/pink job to
        # scheduler_yellow_pink_jobs (Sullivan ETL feed) — even ones the user
        # excluded from the running schedule. Loaded with no machine
        # restrictions so the list reflects all yellow/pink work in the input.
        yp_cfg = copy.deepcopy(cfg)
        yp_cfg.include_yellow = True
        yp_cfg.include_pink = True
        yp_cfg.disabled_machines = []
        yp_cfg.disabled_stations = []
        all_yp_raw, _ = load_jobs_from_excel(tmp_path, yp_cfg)
        all_yp_jobs = [
            j for j in all_yp_raw
            if j.get("ticket_color")
            and _YELLOW_PINK_RE.search(j["ticket_color"])
        ]
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
        "all_yp_jobs": all_yp_jobs,
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


# ── Publish to Portal_QPI ──────────────────────────────────────────
#
# Single "Publish Schedule" button (Matt, 2026-04-03 meeting) writes both:
#   • scheduler_yellow_pink_jobs   — yellow/pink jobs for the Sullivan ETL
#   • scheduler_published_schedule — full schedule for plan-vs-actual analysis
#
# Both tables use a processed_indicator (Y/N) column copied from the existing
# po_acknowledgement convention. On each publish: every existing row is flipped
# to 'n', then the new rows are inserted with 'y'. Wrapped in one Prisma
# transaction so the indicator state can never end up half-y / half-n.

_prisma = None  # lazy singleton — connects on first publish

# httpx timeout for the Python <-> prisma engine HTTP channel (NOT the
# Azure SQL connection itself). The prisma engine talks to Azure SQL via
# Tiberius and then returns results to Python over local HTTP. From
# Coolify's network position the underlying SQL queries can take longer
# than httpx's default 5s, surfacing as `httpx.ReadTimeout` in the engine
# HTTP layer (see prisma/_sync_http.py and prisma/engine/_http.py). 60s
# gives generous headroom for batched create_many on the publish path.
_PRISMA_HTTP_TIMEOUT = 60.0

# Maximum rows per create_many call on the publish path. Smaller chunks
# mean shorter individual SQL transactions = less lock contention on the
# shared Portal_QPI database, at the cost of more round trips per publish.
# 100 is a balance: a typical publish (~80 yp + ~250 schedule rows) takes
# ~5 chunked round trips instead of one giant 30s transaction.
_PUBLISH_CHUNK_SIZE = 100


def _chunks(items: list, size: int):
    """Yield successive chunks of `items` of length `size`."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _get_prisma():
    """Return a connected Prisma client. Raises if connection fails."""
    global _prisma
    if _prisma is None:
        from prisma import Prisma  # local import: keeps server bootable without prisma
        _prisma = Prisma(http={"timeout": _PRISMA_HTTP_TIMEOUT})
    if not _prisma.is_connected():
        _prisma.connect()
    return _prisma


def _derive_job_type(job_data: dict) -> str:
    """Match the existing CSV export's Type column (frontend/index.html:1259)."""
    tags = []
    if job_data.get("is_labeler"):
        tags.append("Labeler")
    if job_data.get("is_bagger"):
        tags.append("Bagger")
    if job_data.get("is_in_progress"):
        tags.append("In Progress")
    if job_data.get("is_picked"):
        tags.append("Picked")
    return ", ".join(tags) if tags else "Regular"


def _yellow_pink_row(entry: ScheduleEntry, published_at: datetime) -> dict:
    """Build one scheduler_yellow_pink_jobs row from a scheduled JOB entry.

    Column mapping mirrors the CSV Venky currently sends to Sullivan
    (frontend/index.html:1267). Note that "Part #" in that CSV is the
    finished_item field, so we store finished_item in part_number.
    """
    j = entry.job_data or {}
    return {
        "published_at": published_at,
        "so_number": entry.so_number or "",
        "part_number": j.get("finished_item") or None,
        "description": j.get("description") or None,
        "tool_id": entry.tool_id,
        "machine": entry.machine_id,
        "due_date": j.get("due_date"),
        "scheduled_start": entry.start,
        "ticket_color": j.get("ticket_color") or "",
        "job_type": _derive_job_type(j),
        "processed_indicator": "y",
    }


def _yellow_pink_row_unscheduled(j: dict, published_at: datetime) -> dict:
    """Build one row for a yellow/pink job that was NOT in the running schedule.

    Used for jobs that the user excluded via the include_yellow / include_pink
    filters but that still need to land in scheduler_yellow_pink_jobs for the
    Sullivan ETL feed (per the 2026-04-03 meeting).

    machine and scheduled_start are NULL because there is no schedule slot.
    preferred_machine is used as a fallback hint when the EQP code pinned
    the job to a specific machine.
    """
    return {
        "published_at": published_at,
        "so_number": j.get("so_number") or "",
        "part_number": j.get("finished_item") or None,
        "description": j.get("description") or None,
        "tool_id": j.get("tool_id"),
        "machine": j.get("preferred_machine"),
        "due_date": j.get("due_date"),
        "scheduled_start": None,
        "ticket_color": j.get("ticket_color") or "",
        "job_type": _derive_job_type(j),
        "processed_indicator": "y",
    }


def _schedule_row(entry: ScheduleEntry, published_at: datetime) -> dict:
    """Build one scheduler_published_schedule row from any ScheduleEntry."""
    row = {
        "published_at": published_at,
        "machine_id": entry.machine_id,
        "entry_type": entry.entry_type,
        "start_time": entry.start,
        "end_time": entry.end,
        "shift": entry.shift,
        "machine_group": entry.group,
        "tool_id": entry.tool_id,
        "crew_from": entry.crew_from,
        "crew_to": entry.crew_to,
        "idle_type": entry.idle_type,
        "headcount": entry.headcount,
        "processed_indicator": "y",
    }
    if entry.entry_type == "JOB" and entry.job_data:
        j = entry.job_data
        row.update({
            "so_number": entry.so_number,
            "finished_item": j.get("finished_item"),
            "description": j.get("description"),
            "customer": j.get("customer"),
            "remaining_qty": j.get("remaining_qty"),
            "run_hours": j.get("run_hours"),
            "due_date": j.get("due_date"),
            "priority_class": j.get("priority_class"),
            "ticket_color": j.get("ticket_color"),
            "is_labeler": bool(j.get("is_labeler")),
            "is_bagger": bool(j.get("is_bagger")),
            "is_in_progress": bool(j.get("is_in_progress")),
            "is_picked": bool(j.get("is_picked")),
        })
    return row


@app.post("/api/schedule/{schedule_id}/publish")
def publish_schedule(schedule_id: str):
    """Publish the schedule to Portal_QPI.

    Behavior, in one transaction:
      1. UPDATE scheduler_yellow_pink_jobs   SET processed_indicator='n'
      2. UPDATE scheduler_published_schedule SET processed_indicator='n'
      3. INSERT new yellow/pink rows  with processed_indicator='y'
      4. INSERT new schedule rows     with processed_indicator='y'
    Returns row counts and the published_at timestamp.
    """
    stored = _results.get(schedule_id)
    if not stored:
        return JSONResponse({"error": "Schedule not found"}, status_code=404)

    result: ScheduleResult = stored["result"]
    all_yp_jobs: list[dict] = stored.get("all_yp_jobs", [])
    published_at = datetime.utcnow()

    # Build payloads up front so a failure here doesn't half-publish.
    #
    # Per the 2026-04-03 meeting, the yellow/pink table feeds the Sullivan ETL
    # report and must contain EVERY yellow/pink job from the input — including
    # ones the user excluded from the running schedule via the include_yellow /
    # include_pink checkboxes. We therefore enumerate `all_yp_jobs` (loaded
    # unconditionally in create_schedule) and override with scheduled-entry
    # data when a job is also in the schedule.
    scheduled_yp_by_so: dict[str, ScheduleEntry] = {}
    for e in result.entries:
        if (
            e.entry_type == "JOB"
            and e.job_data
            and e.job_data.get("ticket_color")
            and _YELLOW_PINK_RE.search(e.job_data["ticket_color"])
            and e.so_number
        ):
            # If the same SO appears in multiple JOB rows (split batches),
            # keep the earliest one — that's the one Sullivan cares about.
            existing = scheduled_yp_by_so.get(e.so_number)
            if existing is None or e.start < existing.start:
                scheduled_yp_by_so[e.so_number] = e

    yp_rows: list[dict] = []
    seen_yp_so: set[str] = set()
    for j in all_yp_jobs:
        so = j.get("so_number") or ""
        if not so or so in seen_yp_so:
            continue
        seen_yp_so.add(so)
        scheduled = scheduled_yp_by_so.get(so)
        if scheduled is not None:
            yp_rows.append(_yellow_pink_row(scheduled, published_at))
        else:
            yp_rows.append(_yellow_pink_row_unscheduled(j, published_at))

    schedule_rows = [_schedule_row(e, published_at) for e in result.entries]

    try:
        db = _get_prisma()
    except Exception as exc:
        return JSONResponse(
            {"error": "Database connection failed", "detail": str(exc)},
            status_code=503,
        )

    # ── Publish in three steps to keep individual transactions short ──
    #
    # The previous "everything in one batch_()" approach held a single
    # transaction open for 10–30s on the shared Portal_QPI database, which
    # is rough on lock contention with the two other projects writing to
    # this DB. Splitting into a fast flip step + chunked inserts trades
    # whole-publish atomicity for short lock duration.
    #
    # Retry semantics:
    #   • Step 1 (flip) is idempotent — re-running just sets already-'n'
    #     rows to 'n' again (the {"not": "n"} filter makes this a no-op).
    #   • Steps 2/3 (chunked inserts) are NOT atomic across chunks — a
    #     mid-step failure leaves a partial set of new 'y' rows. Retrying
    #     the whole publish is safe though: the next publish's flip step
    #     marks those partial 'y' rows as 'n' (becoming historical noise)
    #     and inserts a fresh, complete set as 'y'. The current snapshot
    #     after a successful publish is always correct.
    #
    # Note on prisma-client-py: db.batch_() is used (not db.tx()) because
    # 0.15.0 has a sync-interface bug where create_many inside db.tx()
    # raises a generic 422 regardless of batch size. The where={"not":"n"}
    # filter on the updates dodges the same code path with empty where={}.
    try:
        # Step 1 — flip both tables to 'n' in one short transaction.
        # Index on processed_indicator makes this a fast indexed UPDATE.
        flip = db.batch_()
        flip.scheduler_yellow_pink_jobs.update_many(
            where={"processed_indicator": {"not": "n"}},
            data={"processed_indicator": "n"},
        )
        flip.scheduler_published_schedule.update_many(
            where={"processed_indicator": {"not": "n"}},
            data={"processed_indicator": "n"},
        )
        flip.commit()

        # Step 2 — insert yellow/pink rows in chunks.
        for chunk in _chunks(yp_rows, _PUBLISH_CHUNK_SIZE):
            db.scheduler_yellow_pink_jobs.create_many(data=chunk)

        # Step 3 — insert schedule rows in chunks.
        for chunk in _chunks(schedule_rows, _PUBLISH_CHUNK_SIZE):
            db.scheduler_published_schedule.create_many(data=chunk)
    except Exception as exc:
        return JSONResponse(
            {
                "error": "Publish failed",
                "detail": str(exc),
                "exc_type": type(exc).__name__,
            },
            status_code=500,
        )

    return JSONResponse({
        "ok": True,
        "schedule_id": schedule_id,
        "published_at": published_at.isoformat(),
        "yellow_pink_count": len(yp_rows),
        "schedule_count": len(schedule_rows),
    })


# ── Database viewer ────────────────────────────────────────────────
#
# Read + delete UI over the scheduler_* tables. Hard allowlist on the table
# name so the 14 tables owned by the other two projects sharing Portal_QPI
# can never be touched through these endpoints.

# table name → (model accessor, ordered column list, default sort field)
_VIEWABLE_TABLES: dict[str, dict] = {
    "scheduler_yellow_pink_jobs": {
        "accessor": lambda db: db.scheduler_yellow_pink_jobs,
        "columns": [
            "id", "published_at", "processed_indicator", "so_number",
            "part_number", "description", "tool_id", "machine",
            "due_date", "scheduled_start", "ticket_color", "job_type",
        ],
        "order_by": {"id": "desc"},
    },
    "scheduler_published_schedule": {
        "accessor": lambda db: db.scheduler_published_schedule,
        "columns": [
            "id", "published_at", "processed_indicator", "machine_id",
            "entry_type", "start_time", "end_time", "shift", "machine_group",
            "tool_id", "so_number", "finished_item", "description", "customer",
            "remaining_qty", "run_hours", "headcount", "due_date",
            "priority_class", "ticket_color", "is_labeler", "is_bagger",
            "is_in_progress", "is_picked", "crew_from", "crew_to", "idle_type",
        ],
        "order_by": {"id": "desc"},
    },
}

_ROW_LIMIT = 5000  # cap to prevent dumping huge tables in one shot


def _table_or_404(name: str):
    """Return the spec for an allowed table, or None if not allowed."""
    return _VIEWABLE_TABLES.get(name)


@app.get("/api/db/tables")
def db_list_tables():
    """List the viewable scheduler tables with row counts."""
    try:
        db = _get_prisma()
    except Exception as exc:
        return JSONResponse({"error": "Database connection failed", "detail": str(exc)}, status_code=503)

    out = []
    for name, spec in _VIEWABLE_TABLES.items():
        accessor = spec["accessor"](db)
        total = accessor.count()
        rows_y = accessor.count(where={"processed_indicator": "y"})
        rows_n = accessor.count(where={"processed_indicator": "n"})
        out.append({
            "name": name,
            "total": total,
            "rows_y": rows_y,
            "rows_n": rows_n,
            "columns": spec["columns"],
        })
    return JSONResponse({"tables": out})


@app.get("/api/db/tables/{name}/rows")
def db_get_rows(name: str, filter: str = "all", limit: int = _ROW_LIMIT):
    """Fetch rows for a viewable table.

    filter: 'y' | 'n' | 'all'  (processed_indicator filter)
    limit:  capped at _ROW_LIMIT to keep payloads bounded
    """
    spec = _table_or_404(name)
    if not spec:
        return JSONResponse({"error": f"Table {name!r} is not viewable"}, status_code=404)

    try:
        db = _get_prisma()
    except Exception as exc:
        return JSONResponse({"error": "Database connection failed", "detail": str(exc)}, status_code=503)

    accessor = spec["accessor"](db)

    where: dict = {}
    if filter == "y":
        where = {"processed_indicator": "y"}
    elif filter == "n":
        where = {"processed_indicator": "n"}
    # 'all' → no filter; we use a dummy non-empty filter to dodge the
    # prisma-client-py empty-where bug only on the count side. find_many is OK.

    if where:
        total = accessor.count(where=where)
    else:
        total = accessor.count()

    take = max(1, min(int(limit), _ROW_LIMIT))
    find_kwargs = {"take": take, "order": spec["order_by"]}
    if where:
        find_kwargs["where"] = where
    rows = accessor.find_many(**find_kwargs)

    # Serialize via Pydantic .model_dump() then jsonable_encoder for safety
    serialized = [jsonable_encoder(r.model_dump()) for r in rows]

    return JSONResponse({
        "name": name,
        "columns": spec["columns"],
        "total": total,
        "returned": len(serialized),
        "limit": take,
        "rows": serialized,
    })


@app.post("/api/db/tables/{name}/delete")
def db_delete_rows(name: str, body: dict = Body(...)):
    """Delete rows from a viewable scheduler_* table.

    Body: {"ids": [1,2,3]} for row deletes, or {"all": true} for full wipe.
    Returns the number of rows deleted.
    """
    spec = _table_or_404(name)
    if not spec:
        return JSONResponse({"error": f"Table {name!r} is not deletable"}, status_code=404)

    try:
        db = _get_prisma()
    except Exception as exc:
        return JSONResponse({"error": "Database connection failed", "detail": str(exc)}, status_code=503)

    accessor = spec["accessor"](db)

    if body.get("all") is True:
        # Same prisma-client-py empty-where workaround used in publish: filter
        # to "id is not the impossible value" instead of where={}.
        try:
            count = accessor.delete_many(where={"id": {"gt": 0}})
        except Exception as exc:
            return JSONResponse({"error": "Delete failed", "detail": str(exc)}, status_code=500)
        return JSONResponse({"deleted": count})

    ids = body.get("ids") or []
    if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
        return JSONResponse({"error": "ids must be a list of integers"}, status_code=400)
    if not ids:
        return JSONResponse({"deleted": 0})

    try:
        count = accessor.delete_many(where={"id": {"in": ids}})
    except Exception as exc:
        return JSONResponse({"error": "Delete failed", "detail": str(exc)}, status_code=500)
    return JSONResponse({"deleted": count})


# Serve frontend
_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")
