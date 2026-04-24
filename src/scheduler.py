"""Schedule orchestrator: load → solve → assemble → crew annotation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from .calendar_utils import (
    add_staffed_hours,
    align_to_working_time,
    staffed_hours_between,
    which_shift,
)
from .models import MACHINE_BY_ID


def _snap_to_minute(dt: datetime) -> datetime:
    """Truncate sub-minute precision.  The solver works in integer
    staffed-minutes; fractional-hour arithmetic in add_staffed_hours
    introduces sub-second drift that causes phantom crew overlaps
    at batch boundaries."""
    return dt.replace(second=0, microsecond=0)
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
    group: Optional[str] = None      # machine group name (multi-group mode)


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
    germantown_jobs: list[dict] = field(default_factory=list)
    crew_cap: int = 0                # configured cap at solve time (0 = unlimited)
    crew_peak_solver: int = 0        # peak the solver projected
    crew_peak_actual: float = 0.0    # peak recomputed from assembled entries
    crew_peak_time: Optional[datetime] = None   # when the actual peak occurred


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
    jobs, skipped, germantown_jobs = load_jobs_from_excel(excel_path, cfg)
    result = generate_schedule_from_jobs(jobs, skipped, cfg, max_concurrent)
    result.germantown_jobs = germantown_jobs
    return result


def generate_schedule_from_jobs(
    jobs: list[dict],
    skipped: list[dict],
    cfg: SchedulerConfig,
    max_concurrent: int = 5,
) -> ScheduleResult:
    """Core pipeline: optimize → assemble → annotate crew. No Excel loading."""
    if not jobs:
        return ScheduleResult([], [], skipped, 0.0, "NO_JOBS", crew_cap=cfg.total_crew)

    machine_jobs = assign_jobs_to_machines(jobs, cfg)
    batches = build_tool_batches(machine_jobs)
    result = solve_schedule(batches, cfg, max_concurrent=max_concurrent)
    if result.status not in ("OPTIMAL", "FEASIBLE"):
        return ScheduleResult(
            [], [], skipped, 0.0, result.status,
            crew_cap=result.crew_cap,
        )

    entries = _assemble_schedule(result, cfg)
    # H3 stagger disabled — solver-level NoOverlap on changeover intervals
    # now handles separation.  The post-hoc stagger was re-introducing
    # overlaps by sliding changeovers without the full constraint picture.
    crew_movements = _compute_crew_movements(entries, cfg)

    peak_hc, peak_time = compute_crew_peak(entries)

    return ScheduleResult(
        entries=entries,
        crew_movements=crew_movements,
        skipped_jobs=skipped,
        makespan_hours=result.makespan_minutes / 60.0,
        solver_status=result.status,
        crew_cap=result.crew_cap,
        crew_peak_solver=result.crew_peak_solver,
        crew_peak_actual=peak_hc,
        crew_peak_time=peak_time,
    )


def compute_crew_peak(
    entries: list[ScheduleEntry],
) -> tuple[float, Optional[datetime]]:
    """Walk the assembled timeline and find the peak concurrent HC.

    Counts JOB, CHANGEOVER, and TOOL_SWAP entries — every one of those
    consumes crew.  Returns (peak_hc, peak_time).  peak_time is the
    first moment the peak is reached.

    Timestamps are rounded to the nearest minute before event-sorting.
    The solver works in integer staffed-minutes, but assembly uses
    fractional hours via add_staffed_hours; that accumulates sub-minute
    rounding error (e.g., a JOB ending at 12:23:01.68 while the next CO
    starts at 12:23:00 sharp).  Those 1-2 second "overlaps" aren't real
    — they're just two different rounding paths landing near the same
    minute boundary — so we collapse them.
    """
    def _to_minute(dt: datetime) -> datetime:
        # Round to nearest minute — 30+ seconds rounds up.
        rounded = dt.replace(second=0, microsecond=0)
        if dt.second >= 30 or (dt.second == 29 and dt.microsecond >= 500000):
            rounded += timedelta(minutes=1)
        return rounded

    events: list[tuple[datetime, int, float]] = []
    # Sort key: (time, kind) — end events (kind=0) fire before start (kind=1)
    # at coincident times, so an exiting batch doesn't fake-inflate the peak
    # when another batch starts at the same instant.
    for e in entries:
        if e.entry_type not in ("JOB", "CHANGEOVER", "TOOL_SWAP"):
            continue
        hc = e.headcount or 0
        if hc <= 0:
            continue
        events.append((_to_minute(e.start), 1, hc))
        events.append((_to_minute(e.end), 0, -hc))

    if not events:
        return 0.0, None

    events.sort(key=lambda ev: (ev[0], ev[1]))
    peak = 0.0
    peak_t: Optional[datetime] = None
    cur = 0.0
    for t, _kind, delta in events:
        cur += delta
        if cur > peak:
            peak = cur
            peak_t = t
    return peak, peak_t


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
        init_tool: Optional[str] = cfg.initial_tools.get(machine_id)

        # In-progress jobs override the user-supplied initial_tool: the IP
        # job is the real "previous state" of the machine, so the solver
        # skips the init_tool constraint (solver.py:309-313) and we have to
        # match that here, otherwise we'd render a phantom upfront CO.
        machine_has_ip = any(sb.batch.has_in_progress for sb in m_batches)
        if machine_has_ip:
            init_tool = None

        prev_tool: Optional[str] = init_tool
        prev_end: Optional[datetime] = None

        # If the first batch doesn't start at minute 0, we have a leading
        # gap. Two ways to fill it:
        #   • If the user set an init_tool that *doesn't* match the first
        #     batch, render an upfront CHANGEOVER ending exactly at the
        #     first JOB's start (and shrink any NOT_RUNNING accordingly).
        #   • Otherwise, fill the whole gap with NOT_RUNNING.
        if m_batches and m_batches[0].start_minute > 0:
            first_sb = m_batches[0]
            first_start = _snap_to_minute(_staffed_minute_to_datetime(
                first_sb.start_minute, schedule_start, spd
            ))
            aligned_start = _snap_to_minute(align_to_working_time(schedule_start, spd))
            gap_hours = staffed_hours_between(aligned_start, first_start, spd)

            needs_upfront_co = (
                init_tool is not None
                and spec.has_changeovers
                and init_tool != first_sb.batch.tool_id
            )
            machine_co_hours = spec.changeover_hours if needs_upfront_co else 0.0

            if needs_upfront_co and gap_hours + 1e-3 >= machine_co_hours:
                # Use solver's changeover start if available
                if first_sb.co_start_minute is not None:
                    co_start_minute = first_sb.co_start_minute
                else:
                    machine_co_min = round(machine_co_hours * 60)
                    co_start_minute = max(0, first_sb.start_minute - machine_co_min)
                co_start_dt = _snap_to_minute(_staffed_minute_to_datetime(
                    co_start_minute, schedule_start, spd
                ))

                # NOT_RUNNING fills the leading portion (if any) before the CO.
                pre_co_gap_hours = staffed_hours_between(
                    aligned_start, co_start_dt, spd
                )
                if pre_co_gap_hours > MIN_GAP_HOURS:
                    entries.append(ScheduleEntry(
                        machine_id=machine_id,
                        entry_type="NOT_RUNNING",
                        start=aligned_start,
                        end=co_start_dt,
                        idle_type="NO_CREW",
                        shift=which_shift(aligned_start, spd),
                    ))

                # Upfront CO entry, ending exactly at the first JOB's start.
                entry_type = "TOOL_SWAP" if spec.self_service_changeover else "CHANGEOVER"
                # CO crew = max HC of the batch being set up (matches solver model)
                co_hc = max((j["headcount"] for j in first_sb.batch.jobs), default=0)
                entries.append(ScheduleEntry(
                    machine_id=machine_id,
                    entry_type=entry_type,
                    start=co_start_dt,
                    end=first_start,
                    tool_id=f"{init_tool} -> {first_sb.batch.tool_id}",
                    shift=which_shift(co_start_dt, spd),
                    headcount=co_hc,
                ))

                # The first batch is now "preceded" by the upfront CO, so the
                # in-loop CO check below shouldn't fire for it.
                prev_tool = first_sb.batch.tool_id
            elif gap_hours > MIN_GAP_HOURS:
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
            batch_start_dt = _snap_to_minute(_staffed_minute_to_datetime(
                sb.start_minute, schedule_start, spd
            ))

            # Insert changeover before this batch if tool changed
            if prev_tool is not None and prev_tool != batch.tool_id and spec.has_changeovers:
                # Use solver's changeover start if available, else fall back
                if sb.co_start_minute is not None:
                    co_start = _snap_to_minute(_staffed_minute_to_datetime(
                        sb.co_start_minute, schedule_start, spd
                    ))
                else:
                    co_start = prev_end if prev_end else batch_start_dt
                co_hours = spec.changeover_hours
                co_end = _snap_to_minute(add_staffed_hours(co_start, co_hours, spd))

                # Snap changeover end to batch start when the difference is
                # just rounding noise (solver uses integer minutes, assembly
                # uses fractional hours).  Eliminates tiny visual gaps/overlaps.
                rounding_gap = abs((co_end - batch_start_dt).total_seconds())
                if rounding_gap < 120:  # < 2 min = rounding noise
                    co_end = batch_start_dt

                co_end_aligned = align_to_working_time(co_end, spd)

                entry_type = "TOOL_SWAP" if spec.self_service_changeover else "CHANGEOVER"
                # CO crew = max HC of the batch being set up (matches solver model)
                co_hc = max((j["headcount"] for j in batch.jobs), default=0)
                entries.append(ScheduleEntry(
                    machine_id=machine_id,
                    entry_type=entry_type,
                    start=co_start,
                    end=co_end,
                    tool_id=f"{prev_tool} -> {batch.tool_id}",
                    shift=which_shift(co_start, spd),
                    headcount=co_hc,
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
                job_start = _snap_to_minute(align_to_working_time(cursor, spd))
                job_end = _snap_to_minute(add_staffed_hours(job_start, job["run_hours"], spd))

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

    # Greedily place each changeover at the earliest non-conflicting time.
    # A spacing buffer between consecutive changeovers gives crew time
    # to settle into their new assignment before the next disruption.
    placed: list[tuple[datetime, datetime]] = []
    spacing_buffer = timedelta(minutes=30)

    for co_entry, window_start, window_end, duration in cos:
        candidate = window_start

        # Slide past any conflicting already-placed changeover (+ buffer)
        changed = True
        while changed:
            changed = False
            for p_start, p_end in placed:
                buffered_end = p_end + spacing_buffer
                if candidate < buffered_end and (candidate + duration) > p_start:
                    candidate = buffered_end
                    changed = True

        # Check if it still fits in the window (fall back without buffer)
        if candidate + duration > window_end + timedelta(minutes=2):
            # Try again without buffer — better to have tight changeovers
            # than to give up on staggering entirely
            candidate = window_start
            changed = True
            while changed:
                changed = False
                for p_start, p_end in placed:
                    if candidate < p_end and (candidate + duration) > p_start:
                        candidate = p_end
                        changed = True
            if candidate + duration > window_end + timedelta(minutes=2):
                candidate = co_entry.start  # can't fit at all, keep original

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

def _shift_boundaries_in_range(
    t_min: datetime, t_max: datetime, cfg: SchedulerConfig
) -> list[datetime]:
    """Return shift boundary times (06:30, 14:30 on weekdays) in [t_min, t_max]."""
    boundaries: set[datetime] = set()
    boundaries.add(cfg.schedule_start)

    current_date = t_min.date()
    end_date = t_max.date()
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Mon–Fri
            for h, m in [(6, 30), (14, 30)]:
                bt = datetime.combine(
                    current_date, datetime.min.time().replace(hour=h, minute=m)
                )
                if t_min <= bt <= t_max:
                    boundaries.add(bt)
        current_date += timedelta(days=1)

    return sorted(boundaries)


def _collect_crew_events(
    entries: list[ScheduleEntry],
    cfg: Optional[SchedulerConfig] = None,
) -> tuple[list[tuple], list[ScheduleEntry]]:
    """Collect freed-crew events and target job starts.

    Returns (freed_events, job_starts) where:
    - freed_events: [(freed_time, freed_machine, hc, reason, source_entry), ...]
    - job_starts: [ScheduleEntry, ...] jobs that need external crew
    """
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
            if prev is None or prev.entry_type not in ("JOB", "TOOL_SWAP"):
                needs_crew.add((e.machine_id, e.start))

    job_starts = [
        e for e in entries
        if e.entry_type == "JOB" and (e.machine_id, e.start) in needs_crew
    ]

    freed_events: list[tuple[datetime, str, float, str, Optional[ScheduleEntry]]] = []

    # Track which (machine, time) pairs have already been freed via
    # end_of_work so that changeover events don't double-count crew
    # that was already released.
    freed_by_eow: set[tuple[str, datetime]] = set()

    for machine_id, m_entries in by_machine.items():
        for i, e in enumerate(m_entries):
            if e.entry_type != "JOB":
                continue
            next_entry = m_entries[i + 1] if i + 1 < len(m_entries) else None
            if next_entry is None or next_entry.entry_type == "NOT_RUNNING":
                hc = e.headcount or 0
                if hc:
                    freed_events.append((e.end, machine_id, hc, "end_of_work", e))
                    freed_by_eow.add((machine_id, e.end))
            elif next_entry.entry_type == "TOOL_SWAP":
                after_swap = m_entries[i + 2] if i + 2 < len(m_entries) else None
                if after_swap is None or after_swap.entry_type != "JOB":
                    hc = e.headcount or 0
                    if hc:
                        freed_events.append((next_entry.end, machine_id, hc, "end_of_work", next_entry))
                        freed_by_eow.add((machine_id, next_entry.end))

    for co in entries:
        if co.entry_type != "CHANGEOVER":
            continue
        # Find the job immediately before this changeover
        prev_job = None
        for e in by_machine.get(co.machine_id, []):
            if (e.entry_type == "JOB"
                    and e.end <= co.start + timedelta(minutes=5)):
                prev_job = e
        if prev_job is None:
            continue
        hc = prev_job.headcount or 0
        if not hc:
            continue
        # Skip if this crew was already freed by end_of_work (the job
        # ended into NOT_RUNNING before this changeover started).
        if (co.machine_id, prev_job.end) in freed_by_eow:
            continue
        # Free crew when the job ends, not when the changeover starts.
        # The solver may slide changeovers later to avoid overlaps,
        # leaving a gap where the crew is idle and can jump elsewhere.
        freed_events.append((prev_job.end, co.machine_id, hc, "changeover", e))

    # ── Seed crews at shift boundaries ──────────────────────────
    #
    # At each shift start (06:30, 14:30) fresh crews arrive. Jobs that
    # begin at a boundary need crew but have no "freed" source — nobody
    # has finished work yet. For each such job, create a synthetic freed
    # event from CREW_POOL so the optimizer can assign it.
    if cfg and entries:
        t_min = min(e.start for e in entries)
        t_max = max(e.end for e in entries)
        boundaries = _shift_boundaries_in_range(t_min, t_max, cfg)
        shift_tol = timedelta(minutes=5)

        for js in job_starts:
            for bt in boundaries:
                if abs((js.start - bt).total_seconds()) <= shift_tol.total_seconds():
                    freed_events.append(
                        (bt, "CREW_POOL", js.headcount or 11.0, "shift_start", None)
                    )
                    break

    return freed_events, job_starts


def _build_feasible_pairings(
    freed_events: list[tuple], job_starts: list[ScheduleEntry],
) -> list[list[int]]:
    """For each freed event, list indices of feasible target job starts.

    Returns feasible[i] = [target_index, ...] for freed_events[i].
    """
    tolerance = timedelta(minutes=30)
    window = timedelta(hours=3)

    feasible: list[list[int]] = []
    for freed_time, freed_machine, hc, reason, source_entry in freed_events:
        targets = []
        for j, e in enumerate(job_starts):
            if (e.machine_id != freed_machine
                    and e.start >= freed_time - tolerance
                    and e.start <= freed_time + window):
                targets.append(j)
        feasible.append(targets)
    return feasible


def _apply_assignments(
    assignments: list[int],  # assignments[i] = target index or -1
    freed_events: list[tuple],
    job_starts: list[ScheduleEntry],
) -> list[CrewMovement]:
    """Convert an assignment vector to CrewMovement list and annotate entries."""
    movements: list[CrewMovement] = []
    for i, target_idx in enumerate(assignments):
        if target_idx < 0:
            continue
        freed_time, freed_machine, hc, reason, source_entry = freed_events[i]
        target = job_starts[target_idx]

        if source_entry:
            source_entry.crew_to = target.machine_id
        target.crew_from = freed_machine

        movements.append(CrewMovement(
            time=target.start,
            from_machine=freed_machine,
            to_machine=target.machine_id,
            headcount=hc,
            reason=reason,
        ))

    movements.sort(key=lambda m: m.time)
    return movements



def _optimize_crew_cpsat(
    freed_events: list[tuple],
    job_starts: list[ScheduleEntry],
    feasible: list[list[int]],
) -> list[int]:
    """Use CP-SAT to find optimal crew assignments.

    Exact solver replacing the approximate NSGA-II approach.  The model
    is tiny (~90 booleans for a typical schedule) and solves in ms.

    Objective (scalarized, priority order):
      1. Minimize ping-pong movements (A→B then B→A within 2h)
      2. Minimize total idle time (gap between freed time and target start)
      3. Minimize HC mismatch
      4. Maximize total assignments (prefer matching over leaving idle)
    """
    from ortools.sat.python import cp_model as crew_cp

    n_sources = len(freed_events)
    n_targets = len(job_starts)

    if n_sources == 0 or n_targets == 0:
        return [-1] * n_sources

    model = crew_cp.CpModel()

    # x[i][j] = 1 if freed event i assigned to job start j
    x: dict[tuple[int, int], crew_cp.IntVar] = {}
    source_vars: dict[int, list[crew_cp.IntVar]] = {}
    target_vars: dict[int, list[crew_cp.IntVar]] = {}

    for i in range(n_sources):
        source_vars[i] = []
        for j in feasible[i]:
            v = model.new_bool_var(f"c_{i}_{j}")
            x[(i, j)] = v
            source_vars[i].append(v)
            target_vars.setdefault(j, []).append(v)

    # At most one target per freed event
    for i in range(n_sources):
        if source_vars[i]:
            model.add(sum(source_vars[i]) <= 1)

    # At most one source per job start
    for j, tvars in target_vars.items():
        if tvars:
            model.add(sum(tvars) <= 1)

    # ── Ping-pong constraints ───────────────────────────────────
    PING_PONG_PENALTY = 10000
    pp_terms: list = []

    for i1 in range(n_sources):
        if len(feasible[i1]) <= 1:
            continue
        ft1, fm1, _, _, _ = freed_events[i1]
        for i2 in range(i1 + 1, n_sources):
            if len(feasible[i2]) <= 1:
                continue
            ft2, fm2, _, _, _ = freed_events[i2]
            if abs((ft2 - ft1).total_seconds()) >= 7200:
                continue
            for j1 in feasible[i1]:
                t1_mid = job_starts[j1].machine_id
                for j2 in feasible[i2]:
                    t2_mid = job_starts[j2].machine_id
                    if fm1 == t2_mid and t1_mid == fm2:
                        pp = model.new_bool_var(f"pp_{i1}_{j1}_{i2}_{j2}")
                        model.add(x[(i1, j1)] + x[(i2, j2)] == 2).only_enforce_if(pp)
                        model.add(x[(i1, j1)] + x[(i2, j2)] < 2).only_enforce_if(~pp)
                        pp_terms.append(pp)

    # ── Idle time cost (minutes) ────────────────────────────────
    idle_terms: list = []
    for i in range(n_sources):
        ft = freed_events[i][0]
        for j in feasible[i]:
            idle_min = round(abs((job_starts[j].start - ft).total_seconds()) / 60.0)
            if idle_min > 0:
                idle_terms.append(idle_min * x[(i, j)])

    # ── HC mismatch cost ────────────────────────────────────────
    hc_terms: list = []
    for i in range(n_sources):
        hc = freed_events[i][2]
        for j in feasible[i]:
            target_hc = job_starts[j].headcount or hc
            hc_diff = round(abs(hc - target_hc))
            if hc_diff > 0:
                hc_terms.append(hc_diff * x[(i, j)])

    # ── Unassigned penalty ──────────────────────────────────────
    UNASSIGNED_PENALTY = 60
    total_assigned = sum(v for vl in source_vars.values() for v in vl)

    # ── Objective ───────────────────────────────────────────────
    obj = UNASSIGNED_PENALTY * (n_sources - total_assigned)
    if pp_terms:
        obj += PING_PONG_PENALTY * sum(pp_terms)
    if idle_terms:
        obj += sum(idle_terms)
    if hc_terms:
        obj += sum(hc_terms)
    model.minimize(obj)

    # ── Solve ───────────────────────────────────────────────────
    solver = crew_cp.CpSolver()
    solver.parameters.max_time_in_seconds = 2.0
    solver.parameters.num_workers = 2
    status = solver.solve(model)

    if status not in (crew_cp.OPTIMAL, crew_cp.FEASIBLE):
        return _greedy_assignment(freed_events, job_starts, feasible)

    assignments = [-1] * n_sources
    for i in range(n_sources):
        for j in feasible[i]:
            if solver.value(x[(i, j)]):
                assignments[i] = j
                break
    return assignments


def _greedy_assignment(
    freed_events: list[tuple],
    job_starts: list[ScheduleEntry],
    feasible: list[list[int]],
) -> list[int]:
    """Greedy fallback: assign by best score with anti-ping-pong."""
    tolerance = timedelta(minutes=30)
    window = timedelta(hours=3)
    max_window_sec = window.total_seconds()

    def _score(freed_time, hc, target):
        time_score = abs((target.start - freed_time).total_seconds()) / max_window_sec
        target_hc = target.headcount or hc
        hc_gap = abs(hc - target_hc) / max(hc, 1)
        return time_score + hc_gap * 0.1

    # Build scored pairings
    pairings: list[tuple[float, int, int]] = []  # (score, freed_idx, target_idx)
    for i, (freed_time, freed_machine, hc, reason, source_entry) in enumerate(freed_events):
        for j in feasible[i]:
            pairings.append((_score(freed_time, hc, job_starts[j]), i, j))

    pairings.sort(key=lambda p: p[0])

    assignments = [-1] * len(freed_events)
    used_targets: set[int] = set()
    recent_flows: dict[tuple[str, str], datetime] = {}
    ping_pong_window = timedelta(hours=2)

    for _score_val, idx, target_idx in pairings:
        if assignments[idx] >= 0:
            continue
        if target_idx in used_targets:
            continue

        freed_time, freed_machine, hc, reason, source_entry = freed_events[idx]
        target = job_starts[target_idx]

        # Skip ping-pong check if this is a forced assignment (only one
        # feasible target) — crew with no alternative should always jump.
        if len(feasible[idx]) > 1:
            reverse_key = (target.machine_id, freed_machine)
            if reverse_key in recent_flows:
                prev_time = recent_flows[reverse_key]
                if abs((freed_time - prev_time).total_seconds()) < ping_pong_window.total_seconds():
                    continue

        assignments[idx] = target_idx
        used_targets.add(target_idx)
        recent_flows[(freed_machine, target.machine_id)] = freed_time

    return assignments


def _compute_crew_movements(
    entries: list[ScheduleEntry], cfg: SchedulerConfig
) -> list[CrewMovement]:
    """Optimized crew movement assignment.

    Uses CP-SAT to find exact-optimal crew assignments that minimize
    idle time, ping-pong movements, and HC mismatch.
    Falls back to greedy assignment if the solver fails.
    """
    freed_events, job_starts = _collect_crew_events(entries, cfg)

    if not freed_events or not job_starts:
        return []

    feasible = _build_feasible_pairings(freed_events, job_starts)

    try:
        assignments = _optimize_crew_cpsat(freed_events, job_starts, feasible)
    except Exception:
        assignments = _greedy_assignment(freed_events, job_starts, feasible)

    return _apply_assignments(assignments, freed_events, job_starts)
