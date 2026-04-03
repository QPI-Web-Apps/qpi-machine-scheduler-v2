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


# ── Stage 2: Build tool batches ─────────────────────────────────────

def build_tool_batches(
    machine_jobs: dict[str, list[dict]],
) -> list[ToolBatch]:
    """Group jobs by (machine, tool) into batches.

    Within each batch, jobs are sorted by: priority_class ASC,
    due_date ASC (NaT last), so_number ASC.
    """
    batches: list[ToolBatch] = []
    batch_id = 0

    for machine_id, jobs in machine_jobs.items():
        if not jobs:
            continue

        # Group by tool
        tool_groups: dict[str, list[dict]] = {}
        for job in jobs:
            tool_groups.setdefault(job["tool_id"], []).append(job)

        spec = MACHINE_BY_ID[machine_id]

        for tool_id, tool_jobs in tool_groups.items():
            # Sort within batch
            tool_jobs.sort(key=lambda j: (
                j["priority_class"],
                j["due_date"] or datetime.max,
                j["so_number"],
            ))

            total_hrs = sum(j["run_hours"] for j in tool_jobs)
            total_min = max(1, round(total_hrs * 60))

            has_ip = any(j.get("is_in_progress") for j in tool_jobs)

            batches.append(ToolBatch(
                batch_id=batch_id,
                machine_id=machine_id,
                tool_id=tool_id,
                jobs=tool_jobs,
                total_minutes=total_min,
                has_in_progress=has_ip,
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
    - Objective: minimize makespan
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

    # ── No-overlap per machine + changeover gaps ────────────────

    # Group batches by machine
    machine_batches: dict[str, list[ToolBatch]] = {}
    for b in batches:
        machine_batches.setdefault(b.machine_id, []).append(b)

    # ── Initial tool changeover constraints ────────────────────
    # If a machine has a known last tool, any first batch with a different
    # tool must start after the changeover duration.
    if cfg.initial_tools:
        for machine_id, init_tool in cfg.initial_tools.items():
            if machine_id not in machine_batches:
                continue
            m_batches_it = machine_batches[machine_id]
            # Skip if machine has in-progress batch (already running)
            if any(b.has_in_progress for b in m_batches_it):
                continue
            spec = MACHINE_BY_ID[machine_id]
            if not spec.has_changeovers:
                continue
            co_min = round(spec.changeover_hours * 60)
            for b in m_batches_it:
                if b.tool_id != init_tool:
                    model.add(starts[b.batch_id] >= co_min)

    # For each machine, add no-overlap and changeover constraints
    for machine_id, m_batches in machine_batches.items():
        if len(m_batches) <= 1:
            continue

        # Machine-level changeover cost (same for all batch pairs on this machine)
        spec = MACHINE_BY_ID[machine_id]
        machine_co = round(spec.changeover_hours * 60) if spec.has_changeovers else 0

        # No-overlap: handled via pairwise ordering
        for i, bi in enumerate(m_batches):
            for bj in m_batches[i + 1:]:
                # Boolean: does bi come before bj?
                bi_before_bj = model.new_bool_var(
                    f"order_{bi.batch_id}_{bj.batch_id}"
                )

                # Gap between consecutive batches with different tools
                if bi.tool_id != bj.tool_id:
                    gap_ij = machine_co   # bi finishes → changeover → bj starts
                    gap_ji = machine_co   # bj finishes → changeover → bi starts
                else:
                    gap_ij = 0
                    gap_ji = 0

                # If bi before bj: end[bi] + gap_ij <= start[bj]
                model.add(
                    ends[bi.batch_id] + gap_ij <= starts[bj.batch_id]
                ).only_enforce_if(bi_before_bj)

                # If bj before bi: end[bj] + gap_ji <= start[bi]
                model.add(
                    ends[bj.batch_id] + gap_ji <= starts[bi.batch_id]
                ).only_enforce_if(~bi_before_bj)

    # ── Max concurrent machines (cumulative) ────────────────────

    all_intervals = [intervals[b.batch_id] for b in batches]
    all_demands = [1] * len(batches)
    model.add_cumulative(all_intervals, all_demands, max_concurrent)

    # ── Objective ────────────────────────────────────────────────
    #
    # All terms are composable and scaled relative to the horizon so
    # that priorities, tardiness, and makespan interact predictably.
    #
    # Layer 1 (highest): late job count        (minimize_late only)
    # Layer 2:           total tardiness        (minimize_late only)
    # Layer 3:           priority start penalty (priority_boost / picked)
    # Layer 4 (lowest):  makespan

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

    # ── Combine layers ─────────────────────────────────────────
    # late_term >> prio_term >> makespan
    # Scale prio_term above makespan but below late_term
    has_prio = cfg.priority_boost or any(
        any(j.get("is_picked") for j in b.jobs) for b in batches
    )
    has_late = cfg.minimize_late and (late_vars or tardiness_vars)

    objective = makespan
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
