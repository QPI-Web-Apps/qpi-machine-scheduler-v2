"""Schedule orchestrator: load → solve → assemble → crew annotation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from .calendar_utils import (
    add_staffed_hours,
    align_to_working_time,
    shift_end_for_time,
    staffed_hours_between,
    which_shift,
)
from .models import MACHINE_BY_ID
from .scheduler_io import SchedulerConfig, load_jobs_from_excel
from .solver import (
    SolverResult,
    ScheduledBatch,
    assign_jobs_to_machines,
    build_tool_batches,
    solve_schedule,
)


# ── Schedule entry types ────────────────────────────────────────────

@dataclass
class ScheduleEntry:
    machine_id: str
    entry_type: str  # JOB, CHANGEOVER, TOOL_SWAP, NOT_RUNNING
    start: datetime
    end: datetime
    tool_id: Optional[str] = None
    # Job fields (only for JOB entries)
    so_number: Optional[str] = None
    job_data: Optional[dict] = None
    # Crew fields
    headcount: Optional[float] = None
    crew_from: Optional[str] = None  # machine that donated crew
    crew_to: Optional[str] = None    # machine crew jumped to
    # Metadata
    shift: Optional[int] = None
    idle_type: Optional[str] = None  # NO_CREW for NOT_RUNNING


@dataclass
class CrewMovement:
    time: datetime
    from_machine: str
    to_machine: str
    headcount: float
    reason: str  # "changeover", "shift_start"


@dataclass
class ScheduleResult:
    entries: list[ScheduleEntry]
    crew_movements: list[CrewMovement]
    skipped_jobs: list[dict]
    makespan_hours: float
    solver_status: str


# ── Main entry point ────────────────────────────────────────────────

def generate_schedule(
    excel_path: str,
    cfg: SchedulerConfig,
    max_concurrent: int = 5,
) -> ScheduleResult:
    """Full pipeline: load Excel → optimize → assemble schedule."""
    # Load
    jobs, skipped = load_jobs_from_excel(excel_path, cfg)
    if not jobs:
        return ScheduleResult([], [], skipped, 0.0, "NO_JOBS")

    # Assign jobs to machines
    machine_jobs = assign_jobs_to_machines(jobs, cfg)

    # Build tool batches
    batches = build_tool_batches(machine_jobs)

    # Solve
    result = solve_schedule(batches, cfg, max_concurrent=max_concurrent)
    if result.status not in ("OPTIMAL", "FEASIBLE"):
        return ScheduleResult([], [], skipped, 0.0, result.status)

    # Assemble: solver output → real schedule entries
    entries = _assemble_schedule(result, cfg)

    # Annotate crew movements
    crew_movements = _compute_crew_movements(entries, cfg)

    return ScheduleResult(
        entries=entries,
        crew_movements=crew_movements,
        skipped_jobs=skipped,
        makespan_hours=result.makespan_minutes / 60.0,
        solver_status=result.status,
    )


# ── Assembly: solver batches → schedule entries ─────────────────────

def _assemble_schedule(
    result: SolverResult, cfg: SchedulerConfig
) -> list[ScheduleEntry]:
    """Convert scheduled batches into concrete ScheduleEntry list."""
    entries: list[ScheduleEntry] = []
    schedule_start = cfg.schedule_start

    # Group by machine and sort by start time
    by_machine: dict[str, list[ScheduledBatch]] = {}
    for sb in result.scheduled_batches:
        by_machine.setdefault(sb.batch.machine_id, []).append(sb)

    for machine_id, m_batches in by_machine.items():
        m_batches.sort(key=lambda sb: sb.start_minute)
        spec = MACHINE_BY_ID[machine_id]
        spd = cfg.get_day_shift_map(machine_id)
        prev_tool: Optional[str] = None
        prev_end: Optional[datetime] = None

        for sb in m_batches:
            batch = sb.batch
            batch_start_dt = _staffed_minute_to_datetime(
                sb.start_minute, schedule_start, spd
            )

            # Insert changeover before this batch if tool changed
            if prev_tool is not None and prev_tool != batch.tool_id and spec.has_changeovers:
                co_start = prev_end if prev_end else batch_start_dt
                co_hours = spec.changeover_hours
                # Changeover is wall-clock time
                co_end = co_start + timedelta(hours=co_hours)
                # Align co_end to working time for the next job
                co_end_aligned = align_to_working_time(co_end, spd)

                entry_type = "TOOL_SWAP" if spec.self_service_changeover else "CHANGEOVER"
                entries.append(ScheduleEntry(
                    machine_id=machine_id,
                    entry_type=entry_type,
                    start=co_start,
                    end=co_end,
                    tool_id=f"{prev_tool} -> {batch.tool_id}",
                    shift=which_shift(co_start, spd),
                ))

                # If there's a gap between changeover end and batch start,
                # insert NOT_RUNNING
                if co_end_aligned < batch_start_dt:
                    gap_hours = staffed_hours_between(co_end_aligned, batch_start_dt, spd)
                    if gap_hours > 0.08:
                        entries.append(ScheduleEntry(
                            machine_id=machine_id,
                            entry_type="NOT_RUNNING",
                            start=co_end_aligned,
                            end=batch_start_dt,
                            idle_type="NO_CREW",
                            shift=which_shift(co_end_aligned, spd),
                        ))

            # Expand batch into individual JOB entries
            cursor = batch_start_dt
            for job in batch.jobs:
                job_start = align_to_working_time(cursor, spd)
                job_end = add_staffed_hours(job_start, job["run_hours"], spd)

                entries.append(ScheduleEntry(
                    machine_id=machine_id,
                    entry_type="JOB",
                    start=job_start,
                    end=job_end,
                    tool_id=batch.tool_id,
                    so_number=job["so_number"],
                    job_data=job,
                    headcount=job["headcount"],
                    shift=which_shift(job_start, spd),
                ))
                cursor = job_end

            prev_tool = batch.tool_id
            prev_end = cursor

    entries.sort(key=lambda e: (e.start, e.machine_id))
    return entries


def _staffed_minute_to_datetime(
    minute: int, schedule_start: datetime, shifts_per_day
) -> datetime:
    """Convert a staffed-minutes offset to a real datetime."""
    return add_staffed_hours(schedule_start, minute / 60.0, shifts_per_day)


# ── Crew movement annotation ───────────────────────────────────────

def _compute_crew_movements(
    entries: list[ScheduleEntry], cfg: SchedulerConfig
) -> list[CrewMovement]:
    """Walk the schedule chronologically and track crew movements.

    When a machine enters CHANGEOVER, its crew is freed.
    We find the next machine that starts a JOB around the same time
    and annotate the crew bridge.
    """
    movements: list[CrewMovement] = []

    # Collect changeover events (crew freed)
    changeovers = [
        e for e in entries if e.entry_type == "CHANGEOVER"
    ]

    # Collect job starts for quick lookup
    job_starts = [
        e for e in entries if e.entry_type == "JOB"
    ]

    for co in changeovers:
        co_start = co.start
        co_machine = co.machine_id

        # Find the headcount of the last job on this machine before changeover
        prev_jobs = [
            e for e in entries
            if e.machine_id == co_machine
            and e.entry_type == "JOB"
            and e.end <= co_start + timedelta(minutes=5)
        ]
        hc = prev_jobs[-1].headcount if prev_jobs else 0

        if not hc:
            continue

        # Find the best landing spot: a JOB starting on another machine
        # within a window around the changeover start
        window = timedelta(hours=3)
        candidates = [
            e for e in job_starts
            if e.machine_id != co_machine
            and co_start - timedelta(minutes=30) <= e.start <= co_start + window
        ]

        if candidates:
            # Pick the one starting closest to changeover time
            target = min(candidates, key=lambda e: abs((e.start - co_start).total_seconds()))

            co.crew_to = target.machine_id
            target.crew_from = co_machine

            movements.append(CrewMovement(
                time=co_start,
                from_machine=co_machine,
                to_machine=target.machine_id,
                headcount=hc,
                reason="changeover",
            ))

    return movements
