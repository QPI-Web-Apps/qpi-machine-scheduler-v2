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
        prev_tool: Optional[str] = cfg.initial_tools.get(machine_id)
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
                # Changeover consumes staffed time (skips non-working periods)
                co_end = add_staffed_hours(co_start, co_hours, spd)
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

    Crew is freed in two scenarios:
    1. A CHANGEOVER starts — the operator is no longer needed on that machine.
    2. A machine's last job ends with no follow-on work (no next job/changeover).

    All freed-crew events are collected, sorted chronologically, then processed
    in order so each crew claims the nearest *unclaimed* target job.
    """
    movements: list[CrewMovement] = []

    # Collect job starts that actually need crew from another machine.
    # A job preceded by another JOB on the same machine already has crew
    # (crew carries over), so it should not be a candidate for crew jumps.
    by_machine: dict[str, list[ScheduleEntry]] = {}
    for e in entries:
        by_machine.setdefault(e.machine_id, []).append(e)
    for mid in by_machine:
        by_machine[mid].sort(key=lambda e: e.start)

    needs_crew: set[tuple[str, datetime]] = set()
    for mid, m_entries in by_machine.items():
        for i, e in enumerate(m_entries):
            if e.entry_type != "JOB":
                continue
            prev = m_entries[i - 1] if i > 0 else None
            # Job needs external crew if it's the first entry, or preceded by
            # a CHANGEOVER, NOT_RUNNING, or TOOL_SWAP (i.e. no carry-over crew)
            if prev is None or prev.entry_type != "JOB":
                needs_crew.add((e.machine_id, e.start))

    job_starts = [
        e for e in entries
        if e.entry_type == "JOB" and (e.machine_id, e.start) in needs_crew
    ]

    # ── Collect all "crew freed" events ──────────────────────────────
    # Each event: (freed_time, freed_machine, headcount, reason, source_entry)
    freed_events: list[tuple[datetime, str, float, str, Optional[ScheduleEntry]]] = []

    # From changeovers
    for co in entries:
        if co.entry_type != "CHANGEOVER":
            continue
        prev_jobs = [
            e for e in entries
            if e.machine_id == co.machine_id
            and e.entry_type == "JOB"
            and e.end <= co.start + timedelta(minutes=5)
        ]
        hc = prev_jobs[-1].headcount if prev_jobs else 0
        if hc:
            freed_events.append((co.start, co.machine_id, hc, "changeover", co))

    # From end-of-work (job followed by nothing, NOT_RUNNING, or TOOL_SWAP)
    for machine_id, m_entries in by_machine.items():
        for i, e in enumerate(m_entries):
            if e.entry_type != "JOB":
                continue
            next_entry = m_entries[i + 1] if i + 1 < len(m_entries) else None
            if next_entry is None or next_entry.entry_type == "NOT_RUNNING":
                hc = e.headcount or 0
                if hc:
                    freed_events.append((e.end, machine_id, hc, "end_of_work", e))
            elif next_entry.entry_type == "TOOL_SWAP":
                # Self-service: crew does the swap themselves, freed after it completes
                hc = e.headcount or 0
                if hc:
                    freed_events.append((next_entry.end, machine_id, hc, "end_of_work", next_entry))

    # ── Sort chronologically, then assign to nearest unclaimed target ─
    freed_events.sort(key=lambda ev: ev[0])

    used_sources: set[tuple[str, datetime]] = set()
    claimed_targets: set[tuple[str, datetime]] = set()

    for _freed_time_orig, freed_machine, hc, reason, source_entry in freed_events:
        # Re-read freed_time from the source entry — earlier pushes may have
        # shifted it forward (e.g. a TOOL_SWAP whose preceding JOB was pushed).
        if source_entry:
            freed_time = source_entry.start if reason == "changeover" else source_entry.end
        else:
            freed_time = _freed_time_orig

        if (freed_machine, freed_time) in used_sources:
            continue

        window = timedelta(hours=3)
        candidates = [
            e for e in job_starts
            if e.machine_id != freed_machine
            and freed_time - timedelta(minutes=30) <= e.start <= freed_time + window
            and (e.machine_id, e.start) not in claimed_targets
        ]
        if not candidates:
            continue

        target = min(candidates, key=lambda e: abs((e.start - freed_time).total_seconds()))

        # If crew arrives after the target job was supposed to start,
        # push the target and subsequent entries forward so timing aligns.
        if freed_time > target.start:
            delta = freed_time - target.start
            m_ents = by_machine[target.machine_id]
            t_idx = m_ents.index(target)
            # Extend preceding NOT_RUNNING to fill the gap
            if t_idx > 0 and m_ents[t_idx - 1].entry_type == "NOT_RUNNING":
                m_ents[t_idx - 1].end = m_ents[t_idx - 1].end + delta
            for ent in m_ents[t_idx:]:
                if ent.entry_type == "NOT_RUNNING":
                    # NOT_RUNNING gap absorbs the delay: shift start only
                    ent.start = ent.start + delta
                    break
                ent.start = ent.start + delta
                ent.end = ent.end + delta

        # Use actual job start as the movement time (crew may arrive
        # slightly early and wait for the job to begin).
        move_time = max(freed_time, target.start)

        if source_entry:
            source_entry.crew_to = target.machine_id
        target.crew_from = freed_machine

        used_sources.add((freed_machine, freed_time))
        claimed_targets.add((target.machine_id, target.start))

        movements.append(CrewMovement(
            time=move_time,
            from_machine=freed_machine,
            to_machine=target.machine_id,
            headcount=hc,
            reason=reason,
        ))

    movements.sort(key=lambda m: m.time)
    return movements
