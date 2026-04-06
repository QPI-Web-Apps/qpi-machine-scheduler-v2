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
    jobs, skipped = load_jobs_from_excel(excel_path, cfg)
    return generate_schedule_from_jobs(jobs, skipped, cfg, max_concurrent)


def generate_schedule_from_jobs(
    jobs: list[dict],
    skipped: list[dict],
    cfg: SchedulerConfig,
    max_concurrent: int = 5,
) -> ScheduleResult:
    """Core pipeline: optimize → assemble → annotate crew. No Excel loading."""
    if not jobs:
        return ScheduleResult([], [], skipped, 0.0, "NO_JOBS")

    machine_jobs = assign_jobs_to_machines(jobs, cfg)
    batches = build_tool_batches(machine_jobs)
    result = solve_schedule(batches, cfg, max_concurrent=max_concurrent)
    if result.status not in ("OPTIMAL", "FEASIBLE"):
        return ScheduleResult([], [], skipped, 0.0, result.status)

    entries = _assemble_schedule(result, cfg)
    if cfg.h3_enabled:
        _stagger_changeovers(entries, cfg)
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

def _collect_crew_events(
    entries: list[ScheduleEntry],
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
                after_swap = m_entries[i + 2] if i + 2 < len(m_entries) else None
                if after_swap is None or after_swap.entry_type != "JOB":
                    hc = e.headcount or 0
                    if hc:
                        freed_events.append((next_entry.end, machine_id, hc, "end_of_work", next_entry))

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


def _score_assignment(
    assignments: list[int],
    freed_events: list[tuple],
    job_starts: list[ScheduleEntry],
    feasible: list[list[int]],
) -> tuple[float, float, float]:
    """Score an assignment vector on three objectives (all minimize).

    Returns (idle_time, ping_pong_count, hc_mismatch).
    """
    total_idle = 0.0
    total_hc_mismatch = 0.0
    ping_pongs = 0

    # Track flows for ping-pong detection
    flows: list[tuple[str, str, datetime]] = []  # (from, to, time)

    for i, target_idx in enumerate(assignments):
        if target_idx < 0:
            # Unassigned freed crew = wasted idle
            total_idle += 3600.0  # 1-hour penalty for unassigned
            continue

        freed_time, freed_machine, hc, reason, source_entry = freed_events[i]
        target = job_starts[target_idx]

        # Idle time: gap between freed and target start
        idle_sec = abs((target.start - freed_time).total_seconds())
        total_idle += idle_sec

        # HC mismatch
        target_hc = target.headcount or hc
        total_hc_mismatch += abs(hc - target_hc)

        flows.append((freed_machine, target.machine_id, freed_time))

    # Count ping-pongs: A→B followed by B→A within 2 hours
    for i, (f1, t1, time1) in enumerate(flows):
        for f2, t2, time2 in flows[i + 1:]:
            if (f1 == t2 and t1 == f2
                    and abs((time2 - time1).total_seconds()) < 7200):
                ping_pongs += 1

    return total_idle, float(ping_pongs), total_hc_mismatch


def _optimize_crew_pymoo(
    freed_events: list[tuple],
    job_starts: list[ScheduleEntry],
    feasible: list[list[int]],
) -> list[int]:
    """Use pymoo NSGA-II to find Pareto-optimal crew assignments.

    Falls back to greedy if pymoo fails or problem is trivial.
    """
    try:
        import numpy as np
        from pymoo.algorithms.moo.nsga2 import NSGA2
        from pymoo.core.problem import Problem
        from pymoo.core.sampling import Sampling
        from pymoo.core.crossover import Crossover
        from pymoo.core.mutation import Mutation
        from pymoo.optimize import minimize as pymoo_minimize
    except ImportError:
        return _greedy_assignment(freed_events, job_starts, feasible)

    n_sources = len(freed_events)
    n_targets = len(job_starts)

    if n_sources == 0 or n_targets == 0:
        return [-1] * n_sources

    # ── Custom operators for integer assignment vectors ──────────

    class CrewSampling(Sampling):
        def _do(self, problem, n_samples, **kwargs):
            X = np.full((n_samples, n_sources), -1, dtype=int)
            for s in range(n_samples):
                used_targets = set()
                order = list(range(n_sources))
                np.random.shuffle(order)
                for i in order:
                    candidates = [t for t in feasible[i] if t not in used_targets]
                    if candidates:
                        t = candidates[np.random.randint(len(candidates))]
                        X[s, i] = t
                        used_targets.add(t)
            return X

    class CrewCrossover(Crossover):
        def __init__(self):
            super().__init__(n_parents=2, n_offsprings=2)

        def _do(self, problem, X, **kwargs):
            n_matings = X.shape[0]
            Y = np.full((n_matings, 2, n_sources), -1, dtype=int)
            for k in range(n_matings):
                p1, p2 = X[k, 0], X[k, 1]
                # Uniform crossover with feasibility repair
                for off in range(2):
                    child = np.full(n_sources, -1, dtype=int)
                    used = set()
                    order = list(range(n_sources))
                    np.random.shuffle(order)
                    for i in order:
                        parent_val = p1[i] if (np.random.random() < 0.5) else p2[i]
                        if parent_val >= 0 and parent_val in feasible[i] and parent_val not in used:
                            child[i] = parent_val
                            used.add(parent_val)
                        else:
                            candidates = [t for t in feasible[i] if t not in used]
                            if candidates:
                                child[i] = candidates[np.random.randint(len(candidates))]
                                used.add(child[i])
                    Y[k, off] = child
            return Y

    class CrewMutation(Mutation):
        def _do(self, problem, X, **kwargs):
            for i in range(X.shape[0]):
                if np.random.random() < 0.3:
                    # Pick a random source and reassign it
                    src = np.random.randint(n_sources)
                    used = set(X[i]) - {-1, X[i, src]}
                    candidates = [t for t in feasible[src] if t not in used]
                    if candidates:
                        X[i, src] = candidates[np.random.randint(len(candidates))]
                    else:
                        X[i, src] = -1
            return X

    class CrewProblem(Problem):
        def __init__(self):
            super().__init__(
                n_var=n_sources,
                n_obj=3,
                xl=-1,
                xu=n_targets - 1,
            )

        def _evaluate(self, X, out, *args, **kwargs):
            F = np.zeros((X.shape[0], 3))
            for k in range(X.shape[0]):
                assignments = X[k].astype(int).tolist()
                F[k] = _score_assignment(
                    assignments, freed_events, job_starts, feasible
                )
            out["F"] = F

    algorithm = NSGA2(
        pop_size=50,
        sampling=CrewSampling(),
        crossover=CrewCrossover(),
        mutation=CrewMutation(),
        eliminate_duplicates=True,
    )

    result = pymoo_minimize(
        CrewProblem(),
        algorithm,
        termination=("n_gen", 80),
        seed=42,
        verbose=False,
    )

    if result.X is None:
        return _greedy_assignment(freed_events, job_starts, feasible)

    # Pick solution with lowest ping-pong count, then lowest idle time
    F = result.F
    best_idx = 0
    best_key = (F[0, 1], F[0, 0], F[0, 2])  # (ping_pong, idle, hc_mismatch)
    for k in range(1, F.shape[0]):
        key = (F[k, 1], F[k, 0], F[k, 2])
        if key < best_key:
            best_key = key
            best_idx = k

    return result.X[best_idx].astype(int).tolist()


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

    Uses pymoo NSGA-II multi-objective optimizer to find crew assignments
    that minimize idle time, ping-pong movements, and HC mismatch.
    Falls back to greedy assignment if pymoo is unavailable.
    """
    freed_events, job_starts = _collect_crew_events(entries)

    if not freed_events or not job_starts:
        return []

    feasible = _build_feasible_pairings(freed_events, job_starts)

    # Try pymoo optimizer, fall back to greedy
    try:
        assignments = _optimize_crew_pymoo(freed_events, job_starts, feasible)
    except Exception:
        assignments = _greedy_assignment(freed_events, job_starts, feasible)

    return _apply_assignments(assignments, freed_events, job_starts)
