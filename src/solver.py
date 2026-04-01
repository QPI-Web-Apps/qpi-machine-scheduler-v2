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

from .calendar_utils import add_staffed_hours
from .models import MACHINE_BY_ID, machines_in_group
from .scheduler_io import SchedulerConfig


# ── Data structures ─────────────────────────────────────────────────

@dataclass
class ToolBatch:
    batch_id: int
    machine_id: str
    tool_id: str
    jobs: list[dict]
    total_minutes: int  # staffed minutes
    changeover_minutes: int  # cost to switch TO this batch's tool (0 if first or same tool)

    @property
    def total_hours(self) -> float:
        return self.total_minutes / 60.0


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

    Single-machine groups are trivial.  For the 16-group, tools are
    load-balanced across 16A/B/C (labeler tools forced to 16C).
    All jobs sharing a tool go to the same machine.
    """
    # Separate 16-group from others
    group_16_jobs: list[dict] = []
    machine_jobs: dict[str, list[dict]] = {m: [] for m in MACHINE_BY_ID}

    for job in jobs:
        eligible = job["eligible_machines"]
        if len(eligible) == 1:
            machine_jobs[eligible[0]].append(job)
        else:
            # Multi-machine eligible → 16-group
            group_16_jobs.append(job)

    # Assign 16-group tools to machines via greedy load-balance
    if group_16_jobs:
        _assign_16_group(group_16_jobs, machine_jobs)

    return machine_jobs


def _assign_16_group(
    jobs: list[dict], machine_jobs: dict[str, list[dict]]
) -> None:
    """Assign 16-group tools to 16A/B/C, balancing total hours."""
    # Group jobs by tool
    tool_jobs: dict[str, list[dict]] = {}
    for job in jobs:
        tool_jobs.setdefault(job["tool_id"], []).append(job)

    # Build tool bundles: (tool_id, total_hours, jobs, eligible_machines)
    bundles: list[tuple[str, float, list[dict], list[str]]] = []
    for tool_id, tjobs in tool_jobs.items():
        total_hrs = sum(j["run_hours"] for j in tjobs)
        # If any job in this tool is labeler-only (eligible = [16C]), force 16C
        eligible = tjobs[0]["eligible_machines"]
        bundles.append((tool_id, total_hrs, tjobs, eligible))

    # Sort largest first for better load balance
    bundles.sort(key=lambda b: -b[1])

    # Track load per machine
    load = {"16A": 0.0, "16B": 0.0, "16C": 0.0}
    # Add existing load from single-eligible jobs already assigned
    for mid in load:
        load[mid] = sum(j["run_hours"] for j in machine_jobs[mid])

    for tool_id, total_hrs, tjobs, eligible in bundles:
        # Pick the least-loaded eligible machine
        best = min(eligible, key=lambda m: load[m])
        for job in tjobs:
            job["assigned_machine"] = best
            machine_jobs[best].append(job)
        load[best] += total_hrs


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

            co_min = round(spec.changeover_hours * 60) if spec.has_changeovers else 0

            batches.append(ToolBatch(
                batch_id=batch_id,
                machine_id=machine_id,
                tool_id=tool_id,
                jobs=tool_jobs,
                total_minutes=total_min,
                changeover_minutes=co_min,
            ))
            batch_id += 1

    return batches


# ── Stage 3: CP-SAT solver ─────────────────────────────────────────

def solve_schedule(
    batches: list[ToolBatch],
    cfg: SchedulerConfig,
    max_concurrent: int = 5,
    time_limit_seconds: float = 30.0,
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

    # Horizon: sum of all work + all possible changeovers (generous upper bound)
    total_work = sum(b.total_minutes for b in batches)
    total_co = sum(b.changeover_minutes for b in batches)
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

    # ── No-overlap per machine + changeover gaps ────────────────

    # Group batches by machine
    machine_batches: dict[str, list[ToolBatch]] = {}
    for b in batches:
        machine_batches.setdefault(b.machine_id, []).append(b)

    # For each machine, add no-overlap and changeover constraints
    for machine_id, m_batches in machine_batches.items():
        if len(m_batches) <= 1:
            continue

        # No-overlap: handled via pairwise ordering
        for i, bi in enumerate(m_batches):
            for bj in m_batches[i + 1:]:
                # Boolean: does bi come before bj?
                bi_before_bj = model.new_bool_var(
                    f"order_{bi.batch_id}_{bj.batch_id}"
                )

                # Gap between consecutive batches with different tools
                gap = 0
                if bi.tool_id != bj.tool_id:
                    gap = bi.changeover_minutes  # changeover duration for this machine

                # If bi before bj: end[bi] + gap <= start[bj]
                model.add(
                    ends[bi.batch_id] + gap <= starts[bj.batch_id]
                ).only_enforce_if(bi_before_bj)

                # If bj before bi: end[bj] + gap <= start[bi]
                model.add(
                    ends[bj.batch_id] + gap <= starts[bi.batch_id]
                ).only_enforce_if(~bi_before_bj)

    # ── Max concurrent machines (cumulative) ────────────────────

    all_intervals = [intervals[b.batch_id] for b in batches]
    all_demands = [1] * len(batches)
    model.add_cumulative(all_intervals, all_demands, max_concurrent)

    # ── Objective: minimize makespan ────────────────────────────

    makespan = model.new_int_var(0, horizon, "makespan")
    for b in batches:
        model.add(makespan >= ends[b.batch_id])
    model.minimize(makespan)

    # ── Solve ───────────────────────────────────────────────────

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = time_limit_seconds

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
