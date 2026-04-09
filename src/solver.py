"""CP-SAT based schedule optimizer.

Three stages:
1. assign_jobs_to_machines — route every job to a specific machine
2. build_tool_batches     — group (machine, tool) into sequential batches
3. solve_schedule         — CP-SAT finds optimal batch ordering & timing
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from ortools.sat.python import cp_model

from .calendar_utils import add_staffed_hours, datetime_to_staffed_minute
from .models import MACHINE_BY_ID
from .scheduler_io import SchedulerConfig


# ── Data structures ─────────────────────────────────────────────────

@dataclass
class ToolBatch:
    batch_id: int
    machine_id: str
    tool_id: str
    jobs: list[dict]
    total_minutes: int  # staffed minutes
    has_in_progress: bool = False  # True if any job is currently running
    dominant_headcount: float = 0.0  # time-weighted average HC for the batch


@dataclass
class ScheduledBatch:
    """A tool batch with its solved start time."""
    batch: ToolBatch
    start_minute: int  # in staffed-minutes timeline
    end_minute: int


@dataclass
class SolverResult:
    scheduled_batches: list[ScheduledBatch]
    makespan_minutes: int
    status: str  # "OPTIMAL", "FEASIBLE", "INFEASIBLE", etc.


# ── Stage 1: Assign jobs to machines ────────────────────────────────

def assign_jobs_to_machines(
    jobs: list[dict], cfg: SchedulerConfig
) -> dict[str, list[dict]]:
    """Route every job to a specific machine_id.

    Single-eligible jobs go directly.  Multi-eligible jobs are grouped
    by their eligible set and load-balanced across those machines.
    All jobs sharing a tool are assigned to the same machine.
    """
    machine_jobs: dict[str, list[dict]] = {m: [] for m in MACHINE_BY_ID}
    # Collect multi-eligible jobs keyed by their eligible set (as frozenset)
    multi_eligible: dict[frozenset[str], list[dict]] = {}

    for job in jobs:
        eligible = job["eligible_machines"]
        if len(eligible) == 1:
            machine_jobs[eligible[0]].append(job)
        else:
            key = frozenset(eligible)
            multi_eligible.setdefault(key, []).append(job)

    # Load-balance each multi-eligible group
    for _eligible_set, group_jobs in multi_eligible.items():
        _assign_multi_machine_group(
            group_jobs, machine_jobs,
            minimize_changeovers=cfg.priority_boost,
        )

    return machine_jobs


def _assign_multi_machine_group(
    jobs: list[dict], machine_jobs: dict[str, list[dict]],
    minimize_changeovers: bool = False,
) -> None:
    """Assign jobs to machines within a multi-eligible group, balancing total hours.

    Eligible machines and changeover costs are derived from each job's
    eligible_machines list and the machine registry.

    When minimize_changeovers is True, adding a new tool to a machine incurs
    a virtual penalty equal to that machine's changeover cost, encouraging
    consolidation of tools on fewer machines.
    """
    # Group jobs by tool
    tool_jobs: dict[str, list[dict]] = {}
    for job in jobs:
        tool_jobs.setdefault(job["tool_id"], []).append(job)

    # Build tool bundles: (tool_id, total_hours, jobs, eligible_machines)
    bundles: list[tuple[str, float, list[dict], list[str]]] = []
    for tool_id, tjobs in tool_jobs.items():
        total_hrs = sum(j["run_hours"] for j in tjobs)
        eligible = tjobs[0]["eligible_machines"]
        bundles.append((tool_id, total_hrs, tjobs, eligible))

    # Sort largest first for better load balance
    bundles.sort(key=lambda b: -b[1])

    # Derive the full set of machines this group can use
    all_machines: set[str] = set()
    for _, _, _, eligible in bundles:
        all_machines.update(eligible)

    # Track load and tool count per machine
    load: dict[str, float] = {}
    tools_on: dict[str, int] = {}
    for mid in all_machines:
        load[mid] = sum(j["run_hours"] for j in machine_jobs[mid])
        tools_on[mid] = len(set(j["tool_id"] for j in machine_jobs[mid]))

    for tool_id, total_hrs, tjobs, eligible in bundles:
        if minimize_changeovers:
            best = min(eligible, key=lambda m: (
                load[m] + MACHINE_BY_ID[m].changeover_hours * tools_on[m]
            ))
        else:
            best = min(eligible, key=lambda m: load[m])
        for job in tjobs:
            job["assigned_machine"] = best
            machine_jobs[best].append(job)
        load[best] += total_hrs
        tools_on[best] += 1


# ── Headcount helper ───────────────────────────────────────────────

def _batch_dominant_headcount(jobs: list[dict]) -> float:
    """Time-weighted average headcount for a batch's jobs."""
    total_min = sum(max(1, round(j["run_hours"] * 60)) for j in jobs)
    if total_min == 0:
        return 11.0
    weighted = sum(
        j["headcount"] * max(1, round(j["run_hours"] * 60))
        for j in jobs
    )
    return round(weighted / total_min, 1)


# ── Stage 2: Build tool batches ─────────────────────────────────────

def _hc_bucket(hc: float) -> str:
    """Bucket headcount into low/mid/high/very_high for batch splitting."""
    if hc <= 6:
        return "low"
    elif hc <= 9:
        return "mid"
    elif hc <= 11:
        return "high"
    else:
        return "very_high"


def build_tool_batches(
    machine_jobs: dict[str, list[dict]],
) -> list[ToolBatch]:
    """Group jobs by (machine, tool, hc_bucket) into batches.

    Jobs sharing a tool but with different headcount levels are split
    into separate batches so the solver can sequence them with HC-aware
    ordering.  Within each batch, jobs are sorted by: priority_class ASC,
    due_date ASC (NaT last), so_number ASC.
    """
    batches: list[ToolBatch] = []
    batch_id = 0

    for machine_id, jobs in machine_jobs.items():
        if not jobs:
            continue

        # Group by (tool, hc_bucket)
        groups: dict[tuple[str, str], list[dict]] = {}
        for job in jobs:
            key = (job["tool_id"], _hc_bucket(job["headcount"]))
            groups.setdefault(key, []).append(job)

        for (tool_id, _bucket), group_jobs in groups.items():
            # Sort within batch
            group_jobs.sort(key=lambda j: (
                j["priority_class"],
                j["due_date"] or datetime.max,
                j["so_number"],
            ))

            total_hrs = sum(j["run_hours"] for j in group_jobs)
            total_min = max(1, round(total_hrs * 60))

            has_ip = any(j.get("is_in_progress") for j in group_jobs)

            batches.append(ToolBatch(
                batch_id=batch_id,
                machine_id=machine_id,
                tool_id=tool_id,
                jobs=group_jobs,
                total_minutes=total_min,
                has_in_progress=has_ip,
                dominant_headcount=_batch_dominant_headcount(group_jobs),
            ))
            batch_id += 1

    return batches


# ── Stage 3: CP-SAT solver ─────────────────────────────────────────

def solve_schedule(
    batches: list[ToolBatch],
    cfg: SchedulerConfig,
    max_concurrent: int = 5,
    time_limit_seconds: float = 120.0,
) -> SolverResult:
    """Find optimal batch ordering and timing using CP-SAT.

    Model:
    - Interval variable per batch (in staffed minutes)
    - No-overlap per machine, with changeover gaps between different tools
    - Max concurrent machines running (cumulative)
    - Objective: minimize makespan + HC transitions + changeovers (P+ mode)
    """
    if not batches:
        return SolverResult([], 0, "OPTIMAL")

    model = cp_model.CpModel()

    # Horizon: sum of all work + worst-case changeovers (generous upper bound)
    total_work = sum(b.total_minutes for b in batches)
    # Each machine can have at most (num_batches - 1) changeovers
    machine_batch_counts: dict[str, int] = {}
    for b in batches:
        machine_batch_counts[b.machine_id] = machine_batch_counts.get(b.machine_id, 0) + 1
    total_co = sum(
        max(0, count - 1) * round(MACHINE_BY_ID[mid].changeover_hours * 60)
        for mid, count in machine_batch_counts.items()
        if MACHINE_BY_ID[mid].has_changeovers
    )
    horizon = total_work + total_co + 480  # extra shift buffer

    # ── Variables ────────────────────────────────────────────────

    starts: dict[int, cp_model.IntVar] = {}
    ends: dict[int, cp_model.IntVar] = {}
    intervals: dict[int, cp_model.IntervalVar] = {}

    for b in batches:
        s = model.new_int_var(0, horizon, f"start_{b.batch_id}")
        e = model.new_int_var(0, horizon, f"end_{b.batch_id}")
        iv = model.new_interval_var(s, b.total_minutes, e, f"interval_{b.batch_id}")
        starts[b.batch_id] = s
        ends[b.batch_id] = e
        intervals[b.batch_id] = iv

    # ── Pin in-progress batches to start at minute 0 ─────────
    for b in batches:
        if b.has_in_progress:
            model.add(starts[b.batch_id] == 0)

    # ── No-overlap per machine (circuit constraint) ────────────
    #
    # Each machine's batches are sequenced via a circuit (Hamiltonian
    # path through a depot node).  Arc literals give "immediate
    # predecessor" identity, enabling:
    #   - Tool changeover gaps (time constraint on arcs)
    #   - Headcount transition penalties (soft cost in objective)
    #   - Initial tool handling (changeover if first batch differs)

    machine_batches: dict[str, list[ToolBatch]] = {}
    for b in batches:
        machine_batches.setdefault(b.machine_id, []).append(b)

    hc_penalty_terms: list[tuple] = []  # (literal, penalty_value)
    co_penalty_terms: list[tuple] = []  # (literal, penalty_value) — changeover penalties

    for machine_id, m_batches in machine_batches.items():
        if not m_batches:
            continue

        spec = MACHINE_BY_ID[machine_id]
        machine_co = round(spec.changeover_hours * 60) if spec.has_changeovers else 0
        init_tool = cfg.initial_tools.get(machine_id)
        has_ip = any(b.has_in_progress for b in m_batches)

        n = len(m_batches)
        arcs: list[tuple[int, int, cp_model.IntVar]] = []

        for i in range(n):
            bi = m_batches[i]

            # Arc: depot (0) → batch i+1 — batch i is first on this machine
            first_lit = model.new_bool_var(f"first_{machine_id}_{bi.batch_id}")
            arcs.append((0, i + 1, first_lit))

            # Force in-progress batch to be first
            if bi.has_in_progress:
                model.add(first_lit == 1)

            # Initial tool changeover: if this batch is first and its tool
            # differs from the tool already loaded, it must wait for a changeover.
            if (init_tool and init_tool != bi.tool_id
                    and machine_co > 0 and not has_ip):
                model.add(
                    starts[bi.batch_id] >= machine_co
                ).only_enforce_if(first_lit)
                co_penalty_terms.append((first_lit, machine_co))

            # Arc: batch i+1 → depot (0) — batch i is last
            last_lit = model.new_bool_var(f"last_{machine_id}_{bi.batch_id}")
            arcs.append((i + 1, 0, last_lit))

            # Arcs: batch i → batch j (i immediately precedes j)
            for j in range(n):
                if i == j:
                    continue
                bj = m_batches[j]

                lit = model.new_bool_var(
                    f"seq_{machine_id}_{bi.batch_id}_then_{bj.batch_id}"
                )
                arcs.append((i + 1, j + 1, lit))

                # Time gap: changeover if different tools
                gap = machine_co if bi.tool_id != bj.tool_id else 0
                model.add(
                    starts[bj.batch_id] >= ends[bi.batch_id] + gap
                ).only_enforce_if(lit)

                # Changeover penalty (collected for objective)
                if gap > 0:
                    co_penalty_terms.append((lit, gap))

                # HC transition penalty (collected for objective)
                if cfg.hc_penalty_weight > 0:
                    hc_delta = abs(bi.dominant_headcount - bj.dominant_headcount)
                    if hc_delta > 0.5:
                        penalty = int(round(hc_delta * cfg.hc_penalty_weight))
                        hc_penalty_terms.append((lit, penalty))

        model.add_circuit(arcs)

    # ── Max concurrent machines (cumulative) ────────────────────

    all_intervals = [intervals[b.batch_id] for b in batches]
    all_demands = [1] * len(batches)
    model.add_cumulative(all_intervals, all_demands, max_concurrent)

    # ── Crew headcount capacity (cumulative) ────────────────────
    # Prevents the solver from scheduling more total headcount
    # than the available workforce across concurrent batches.

    if cfg.total_crew > 0:
        hc_demands = [max(1, int(round(b.dominant_headcount))) for b in batches]
        model.add_cumulative(all_intervals, hc_demands, cfg.total_crew)

    # ── Objective ────────────────────────────────────────────────
    #
    # All terms are composable and scaled relative to the horizon so
    # that priorities, tardiness, and makespan interact predictably.
    #
    # Layer 1 (highest): late job count        (minimize_late only)
    # Layer 2:           total tardiness        (minimize_late only)
    # Layer 3:           priority start penalty (priority_boost / picked)
    # Layer 4 (lowest):  makespan + HC transition penalty + changeover penalty

    makespan = model.new_int_var(0, horizon, "makespan")
    for b in batches:
        model.add(makespan >= ends[b.batch_id])

    # Scale factor: normalizes start-based penalties so a batch at the
    # end of the horizon contributes ~1× horizon, not an arbitrary multiple.
    # This keeps priority terms meaningful without drowning out makespan.
    n_batches = max(len(batches), 1)

    # ── Layer 1–2: minimize late (optional) ────────────────────
    late_term = 0
    if cfg.minimize_late:
        late_vars = []
        tardiness_vars = []

        for b in batches:
            spd = cfg.get_day_shift_map(b.machine_id)
            cumulative_min = 0
            for j_idx, job in enumerate(b.jobs):
                job_run_min = max(1, round(job["run_hours"] * 60))
                cumulative_min += job_run_min

                due = job.get("due_date")
                if not due:
                    continue
                # Include past-due jobs — push them early too
                due_min = datetime_to_staffed_minute(due, cfg.schedule_start, spd)

                # job end = batch start + cumulative run minutes
                job_end = starts[b.batch_id] + cumulative_min

                if due_min > 0:
                    # Boolean: is this job late?
                    late_var = model.new_bool_var(f"late_{b.batch_id}_{j_idx}")
                    model.add(job_end > due_min).only_enforce_if(late_var)
                    model.add(job_end <= due_min).only_enforce_if(~late_var)
                    late_vars.append(late_var)

                # Tardiness: how many minutes late (clamped to 0)
                # For past-due (due_min <= 0), this still pushes them earlier
                tardiness = model.new_int_var(0, horizon, f"tard_{b.batch_id}_{j_idx}")
                model.add(tardiness >= job_end - max(due_min, 0))
                tardiness_vars.append(tardiness)

        if late_vars or tardiness_vars:
            late_count = sum(late_vars) if late_vars else 0
            total_tardiness = sum(tardiness_vars) if tardiness_vars else 0
            # Layer 1: each late job costs more than the worst possible
            # tardiness, so the solver always prefers fewer late jobs.
            # Layer 2: within the same late count, minimize total tardiness.
            late_term = (late_count * horizon * n_batches
                         + total_tardiness)

    # ── Layer 3: priority / picked start penalties ─────────────
    prio_term = 0

    if cfg.priority_boost:
        for b in batches:
            min_prio = min((j.get("priority_class", 3) for j in b.jobs), default=3)
            if min_prio <= 0:       # P+ / picked / in-progress
                prio_term += 3 * starts[b.batch_id]
            elif min_prio <= 2:     # Priority or Past Due
                prio_term += 1 * starts[b.batch_id]

    # Picked batches always get a push-early term (even without priority_boost)
    for b in batches:
        if any(j.get("is_picked") for j in b.jobs) and not b.has_in_progress:
            prio_term += 3 * starts[b.batch_id]

    # ── Layer 4b: HC transition penalty ──────────────────────────
    hc_term = 0
    if hc_penalty_terms:
        hc_term = sum(lit * pen for lit, pen in hc_penalty_terms)

    # ── Layer 4c: Changeover penalty (priority_boost mode) ──────
    co_term = 0
    if cfg.priority_boost and co_penalty_terms:
        co_term = sum(lit * pen for lit, pen in co_penalty_terms)

    # ── Combine layers ─────────────────────────────────────────
    # late_term >> prio_term >> makespan + hc_term + co_term
    # Scale prio_term above makespan but below late_term
    has_prio = cfg.priority_boost or any(
        any(j.get("is_picked") for j in b.jobs) for b in batches
    )
    has_late = cfg.minimize_late and (late_vars or tardiness_vars)

    objective = makespan + hc_term + co_term
    if has_prio:
        objective += prio_term * n_batches
    if has_late:
        objective += late_term * n_batches * horizon

    model.minimize(objective)

    # ── Solve ───────────────────────────────────────────────────

    solver = cp_model.CpSolver()
    # More objective terms → harder problem → more time
    complexity = 1 + (1 if cfg.minimize_late else 0) + (1 if cfg.priority_boost else 0)
    effective_limit = time_limit_seconds * complexity
    solver.parameters.max_time_in_seconds = effective_limit
    solver.parameters.num_workers = 8

    status = solver.solve(model)

    status_name = {
        cp_model.OPTIMAL: "OPTIMAL",
        cp_model.FEASIBLE: "FEASIBLE",
        cp_model.INFEASIBLE: "INFEASIBLE",
        cp_model.MODEL_INVALID: "MODEL_INVALID",
        cp_model.UNKNOWN: "UNKNOWN",
    }.get(status, "UNKNOWN")

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return SolverResult([], 0, status_name)

    # ── Extract solution ────────────────────────────────────────

    scheduled: list[ScheduledBatch] = []
    for b in batches:
        s = solver.value(starts[b.batch_id])
        e = solver.value(ends[b.batch_id])
        scheduled.append(ScheduledBatch(batch=b, start_minute=s, end_minute=e))

    # Sort by start time for readability
    scheduled.sort(key=lambda sb: (sb.start_minute, sb.batch.machine_id))

    ms = solver.value(makespan)
    return SolverResult(scheduled, ms, status_name)
