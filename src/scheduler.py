"""Schedule orchestrator: load → solve → assemble → crew annotation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from .calendar_utils import (
    add_staffed_hours,
    align_to_working_time,
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


# Minimum gap (hours) to generate a NOT_RUNNING entry.  Gaps smaller
# than this are rounding noise and not worth showing on the timeline.
MIN_GAP_HOURS = 0.08  # ~5 minutes


@dataclass
class MachineSummary:
    """Per-machine hour breakdown for API and export."""
    jobs: int
    changeovers: int
    job_hours: float
    changeover_hours: float
    no_crew_hours: float
    total_hours: float
    utilization: float
    start: Optional[datetime] = None
    end: Optional[datetime] = None


def compute_machine_summary(
    entries: list[ScheduleEntry],
    machine_id: str,
    cfg: "SchedulerConfig",
) -> MachineSummary:
    """Compute hour breakdown for a single machine."""
    spd = cfg.get_day_shift_map(machine_id)
    m_entries = [e for e in entries if e.machine_id == machine_id]

    jobs = [e for e in m_entries if e.entry_type == "JOB"]
    cos = [e for e in m_entries if e.entry_type in ("CHANGEOVER", "TOOL_SWAP")]
    no_crew = [e for e in m_entries if e.entry_type == "NOT_RUNNING"]

    job_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in jobs)
    co_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in cos)
    no_crew_hrs = sum(staffed_hours_between(e.start, e.end, spd) for e in no_crew)
    total = job_hrs + co_hrs

    sorted_ents = sorted(m_entries, key=lambda e: e.start) if m_entries else []
    return MachineSummary(
        jobs=len(jobs),
        changeovers=len(cos),
        job_hours=round(job_hrs, 1),
        changeover_hours=round(co_hrs, 1),
        no_crew_hours=round(no_crew_hrs, 1),
        total_hours=round(total, 1),
        utilization=round(job_hrs / total * 100, 1) if total > 0 else 0,
        start=sorted_ents[0].start if sorted_ents else None,
        end=sorted_ents[-1].end if sorted_ents else None,
    )


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

    # Stagger changeovers so no two maintenance COs overlap
    if cfg.h3_enabled:
        _stagger_changeovers(entries, cfg)

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

        # If the first batch doesn't start at minute 0, emit a NOT_RUNNING
        # gap so the frontend timeline shows why the machine was idle.
        if m_batches and m_batches[0].start_minute > 0:
            first_start = _staffed_minute_to_datetime(
                m_batches[0].start_minute, schedule_start, spd
            )
            aligned_start = align_to_working_time(schedule_start, spd)
            gap_hours = staffed_hours_between(aligned_start, first_start, spd)
            if gap_hours > MIN_GAP_HOURS:
                entries.append(ScheduleEntry(
                    machine_id=machine_id,
                    entry_type="NOT_RUNNING",
                    start=aligned_start,
                    end=first_start,
                    idle_type="NO_CREW",
                    shift=which_shift(aligned_start, spd),
                ))

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

                # Snap changeover end to batch start when the difference is
                # just rounding noise (solver uses integer minutes, assembly
                # uses fractional hours).  Eliminates tiny visual gaps/overlaps.
                rounding_gap = abs((co_end - batch_start_dt).total_seconds())
                if rounding_gap < 120:  # < 2 min = rounding noise
                    co_end = batch_start_dt

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
                    if gap_hours > MIN_GAP_HOURS:
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


# ── Changeover staggering (H3) ───────────────────────────────────

def _stagger_changeovers(
    entries: list[ScheduleEntry], cfg: SchedulerConfig
) -> None:
    """Slide maintenance changeovers within their windows to avoid simultaneous overlaps.

    Each changeover has a window: [prev_job_end, next_job_start].  The changeover
    must fit inside that window.  We greedily place each changeover at the earliest
    time that doesn't conflict with already-placed changeovers on other machines.

    Only affects CHANGEOVER entries (maintenance crew needed).  TOOL_SWAP entries
    (self-service on LMB/SMB) are exempt — they don't use the maintenance crew.
    """
    # Build per-machine sorted entries
    by_machine: dict[str, list[ScheduleEntry]] = {}
    for e in entries:
        by_machine.setdefault(e.machine_id, []).append(e)
    for mid in by_machine:
        by_machine[mid].sort(key=lambda e: e.start)

    # Collect changeover entries with their slidable windows
    cos: list[tuple[ScheduleEntry, datetime, datetime, timedelta]] = []
    for e in entries:
        if e.entry_type != "CHANGEOVER":
            continue
        m_ents = by_machine[e.machine_id]
        idx = m_ents.index(e)

        window_start = e.start  # = prev entry end (earliest possible)

        # Window end = next JOB start on this machine
        window_end = e.end
        for nxt in m_ents[idx + 1:]:
            if nxt.entry_type == "JOB":
                window_end = nxt.start
                break

        duration = e.end - e.start
        cos.append((e, window_start, window_end, duration))

    # Sort by slack (tightest window first) so inflexible changeovers get
    # placed first and flexible ones can slide around them.
    cos.sort(key=lambda x: (x[2] - x[1]) - x[3])

    # Greedily place each changeover at the earliest non-conflicting time
    placed: list[tuple[datetime, datetime]] = []

    for co_entry, window_start, window_end, duration in cos:
        candidate = window_start

        # Slide past any conflicting already-placed changeover
        changed = True
        while changed:
            changed = False
            for p_start, p_end in placed:
                if candidate < p_end and (candidate + duration) > p_start:
                    candidate = p_end
                    changed = True

        # Check if it still fits in the window
        if candidate + duration > window_end + timedelta(minutes=2):
            candidate = co_entry.start  # can't fit, keep original

        co_entry.start = candidate
        co_entry.end = candidate + duration
        co_entry.shift = which_shift(
            candidate, cfg.get_day_shift_map(co_entry.machine_id)
        )

        placed.append((co_entry.start, co_entry.end))

    # Rebuild NOT_RUNNING gaps on machines that have changeovers
    affected = set(co[0].machine_id for co in cos)
    _rebuild_idle_gaps(entries, affected, cfg)

    entries.sort(key=lambda e: (e.start, e.machine_id))


def _rebuild_idle_gaps(
    entries: list[ScheduleEntry],
    machines: set[str],
    cfg: SchedulerConfig,
) -> None:
    """Remove and recreate NOT_RUNNING entries for the specified machines.

    Scans consecutive entries on each machine and fills gaps with NOT_RUNNING.
    Also recreates the initial gap (schedule_start → first entry) if needed.
    """
    # Remove existing NOT_RUNNING for affected machines
    to_remove = [
        e for e in entries
        if e.machine_id in machines and e.entry_type == "NOT_RUNNING"
    ]
    for e in to_remove:
        entries.remove(e)

    # Collect remaining entries per machine
    by_machine: dict[str, list[ScheduleEntry]] = {}
    for e in entries:
        if e.machine_id in machines:
            by_machine.setdefault(e.machine_id, []).append(e)

    new_gaps: list[ScheduleEntry] = []

    for mid in machines:
        m_ents = sorted(by_machine.get(mid, []), key=lambda e: e.start)
        if not m_ents:
            continue
        spd = cfg.get_day_shift_map(mid)

        # Initial gap: schedule_start → first entry
        aligned_start = align_to_working_time(cfg.schedule_start, spd)
        if aligned_start < m_ents[0].start:
            gap_hrs = staffed_hours_between(aligned_start, m_ents[0].start, spd)
            if gap_hrs > MIN_GAP_HOURS:
                new_gaps.append(ScheduleEntry(
                    machine_id=mid,
                    entry_type="NOT_RUNNING",
                    start=aligned_start,
                    end=m_ents[0].start,
                    idle_type="NO_CREW",
                    shift=which_shift(aligned_start, spd),
                ))

        # Inter-entry gaps
        for i in range(len(m_ents) - 1):
            gap_start = m_ents[i].end
            gap_end = m_ents[i + 1].start
            if gap_start < gap_end:
                gap_hrs = staffed_hours_between(gap_start, gap_end, spd)
                if gap_hrs > MIN_GAP_HOURS:
                    new_gaps.append(ScheduleEntry(
                        machine_id=mid,
                        entry_type="NOT_RUNNING",
                        start=gap_start,
                        end=gap_end,
                        idle_type="NO_CREW",
                        shift=which_shift(gap_start, spd),
                    ))

    entries.extend(new_gaps)


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
            # a CHANGEOVER or NOT_RUNNING.  TOOL_SWAP is self-service — crew
            # stays on the machine and continues into the next job.
            if prev is None or prev.entry_type not in ("JOB", "TOOL_SWAP"):
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
                # Self-service: crew does the swap themselves.
                # Only free crew if there's no follow-on job after the swap
                # (crew stays on the machine to run the next batch).
                after_swap = m_entries[i + 2] if i + 2 < len(m_entries) else None
                if after_swap is None or after_swap.entry_type != "JOB":
                    hc = e.headcount or 0
                    if hc:
                        freed_events.append((next_entry.end, machine_id, hc, "end_of_work", next_entry))

    # ── Sort chronologically, then assign to nearest unclaimed target ─
    freed_events.sort(key=lambda ev: ev[0])

    used_sources: set[tuple[str, datetime]] = set()
    claimed_targets: set[tuple[str, datetime]] = set()

    for freed_time, freed_machine, hc, reason, source_entry in freed_events:
        if (freed_machine, freed_time) in used_sources:
            continue

        # Only match jobs the crew can reach on time — never mutate the
        # solver's schedule.  A small tolerance (5 min) covers rounding
        # between staffed-minute conversion and datetime arithmetic.
        tolerance = timedelta(minutes=5)
        window = timedelta(hours=3)
        candidates = [
            e for e in job_starts
            if e.machine_id != freed_machine
            and e.start >= freed_time - tolerance
            and e.start <= freed_time + window
            and (e.machine_id, e.start) not in claimed_targets
        ]
        if not candidates:
            continue

        # Score candidates by time proximity AND headcount compatibility.
        # Lower score = better match.
        #   time_score:  seconds until target starts (0 = simultaneous)
        #   hc_score:    headcount mismatch as fraction of freed crew
        # Headcount match is weighted more: wasting 7 out of 11 people
        # is worse than waiting 5 extra minutes.
        max_window_sec = window.total_seconds()
        def _crew_score(e):
            time_score = abs((e.start - freed_time).total_seconds()) / max_window_sec
            target_hc = e.headcount or hc
            hc_gap = abs(hc - target_hc) / max(hc, 1)
            return hc_gap * 2.0 + time_score

        target = min(candidates, key=_crew_score)

        # Anchor the arrow to the target job's start so the visual
        # indicator aligns with the bar on the Gantt chart.
        move_time = target.start

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
