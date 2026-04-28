"""CP-SAT based schedule optimizer.

Three stages:
1. assign_jobs_to_machines — route every job to a specific machine
2. build_tool_batches     — group (machine, tool) into sequential batches
3. solve_schedule         — CP-SAT finds optimal batch ordering & timing
"""

from __future__ import annotations

import math
import os
import re
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
    co_start_minute: Optional[int] = None  # changeover start (staffed min), if any


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
            minimize_changeovers=True,
        )

    return machine_jobs


def _assign_multi_machine_group(
    jobs: list[dict], machine_jobs: dict[str, list[dict]],
    minimize_changeovers: bool = False,
) -> None:
    """Assign tool bundles to machines using CP-SAT for global optimality.

    Replaces the greedy largest-first heuristic.  A small CP-SAT model
    decides which machine each tool bundle goes to, minimising a weighted
    combination of:
      - Makespan (max machine load across the group)
      - Total changeover cost (each distinct tool on a machine costs one CO)
      - Load variance (discourage extreme imbalance)

    Falls back to the greedy heuristic if the solver fails.
    """
    # Group jobs by tool
    tool_jobs: dict[str, list[dict]] = {}
    for job in jobs:
        tool_jobs.setdefault(job["tool_id"], []).append(job)

    # Build tool bundles: (tool_id, total_minutes, jobs, eligible_machines)
    bundles: list[tuple[str, int, list[dict], list[str]]] = []
    for tool_id, tjobs in tool_jobs.items():
        total_min = max(1, round(sum(j["run_hours"] for j in tjobs) * 60))
        eligible = tjobs[0]["eligible_machines"]
        bundles.append((tool_id, total_min, tjobs, eligible))

    if not bundles:
        return

    # Derive the full set of machines this group can use
    all_machines: list[str] = sorted({m for _, _, _, elig in bundles for m in elig})

    # Existing load on each machine (from single-eligible jobs already placed)
    existing_load: dict[str, int] = {}
    existing_tools: dict[str, set[str]] = {}
    for mid in all_machines:
        existing_load[mid] = max(1, round(
            sum(j["run_hours"] for j in machine_jobs[mid]) * 60
        )) if machine_jobs[mid] else 0
        existing_tools[mid] = {j["tool_id"] for j in machine_jobs[mid]}

    n_bundles = len(bundles)
    n_machines = len(all_machines)

    # ── CP-SAT model ────────────────────────────────────────────
    model = cp_model.CpModel()

    # Eligible machine indices per bundle (precompute for constraints)
    eligible_indices: list[list[int]] = []
    for t in range(n_bundles):
        eligible_set = set(bundles[t][3])
        eligible_indices.append([m for m in range(n_machines)
                                 if all_machines[m] in eligible_set])

    # x[t][m] = 1 if tool bundle t is assigned to machine m
    x: list[list[cp_model.IntVar]] = []
    for t in range(n_bundles):
        row = []
        elig = set(eligible_indices[t])
        for m in range(n_machines):
            if m in elig:
                row.append(model.new_bool_var(f"x_{t}_{all_machines[m]}"))
            else:
                v = model.new_int_var(0, 0, f"x_{t}_{all_machines[m]}_0")
                row.append(v)
        x.append(row)

    # Exactly one machine per tool bundle
    for t in range(n_bundles):
        model.add_exactly_one(x[t][m] for m in eligible_indices[t])

    # ── Effective machine loads (work + changeover time) ──────────
    #
    # The schedule length on a machine = work hours + changeover hours.
    # A changeover is triggered each time a NEW tool appears on a
    # machine (not already loaded from single-eligible jobs).
    # new_tool[t][m] is a bool: 1 if bundle t assigned to machine m
    # and the tool wasn't pre-loaded.
    #
    # effective_load[m] = existing_load + work_from_bundles
    #                     + n_new_tools * changeover_minutes

    # Upper bound: all work + all possible changeovers on one machine
    max_co_per_machine = max(
        round(MACHINE_BY_ID[mid].changeover_hours * 60) for mid in all_machines
    )
    max_possible_load = (
        sum(b[1] for b in bundles)
        + max(existing_load.values(), default=0)
        + n_bundles * max_co_per_machine
    )

    effective_loads: list[cp_model.IntVar] = []
    per_machine_co: list[cp_model.LinearExpr] = []

    for m, mid in enumerate(all_machines):
        co_min = round(MACHINE_BY_ID[mid].changeover_hours * 60)

        # Work from assigned bundles
        bundle_work = sum(
            bundles[t][1] * x[t][m] for t in range(n_bundles)
        )

        # Changeover cost: one CO per new tool on this machine
        if co_min > 0:
            co_terms = []
            for t in range(n_bundles):
                tool_id = bundles[t][0]
                if tool_id in existing_tools[mid]:
                    continue  # tool already loaded — no changeover
                co_terms.append(x[t][m])
            # n_new_tools for this machine (integer var)
            n_new = model.new_int_var(0, n_bundles, f"n_new_{mid}")
            model.add(n_new == sum(co_terms) if co_terms else 0)
            machine_co = n_new * co_min
        else:
            machine_co = 0
            n_new = model.new_int_var(0, 0, f"n_new_{mid}")

        per_machine_co.append(machine_co)

        eff = model.new_int_var(0, max_possible_load, f"eff_load_{mid}")
        model.add(eff == existing_load[mid] + bundle_work + machine_co)
        effective_loads.append(eff)

    # Makespan = max effective load (work + changeovers) across machines
    makespan = model.new_int_var(0, max_possible_load, "assign_makespan")
    model.add_max_equality(makespan, effective_loads)

    # Total changeover cost across all machines
    total_co = sum(per_machine_co)

    # ── Load balance penalty ───────────────────────────────────
    # Penalize max-min effective load range.
    min_load = model.new_int_var(0, max_possible_load, "min_load")
    model.add_min_equality(min_load, effective_loads)
    load_range = model.new_int_var(0, max_possible_load, "load_range")
    model.add(load_range == makespan - min_load)

    # ── Objective ───────────────────────────────────────────────
    # Minimize effective makespan.  The makespan already includes
    # changeover time, so total_co adds a secondary penalty for
    # total changeover volume (prefer fewer COs even if makespan
    # is tied).  Load range encourages balance.
    model.minimize(2 * makespan + total_co + load_range)

    # ── Solve ───────────────────────────────────────────────────
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 5.0  # tiny model, 5s is generous
    solver.parameters.num_workers = min(4, os.cpu_count() or 4)
    status = solver.solve(model)

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        # Fallback to greedy
        _assign_multi_machine_group_greedy(
            jobs, machine_jobs, bundles, all_machines, existing_load
        )
        return

    # ── Apply solution ──────────────────────────────────────────
    for t, (tool_id, total_min, tjobs, eligible) in enumerate(bundles):
        for m, mid in enumerate(all_machines):
            if solver.value(x[t][m]):
                for job in tjobs:
                    job["assigned_machine"] = mid
                    machine_jobs[mid].append(job)
                break


def _assign_multi_machine_group_greedy(
    jobs: list[dict],
    machine_jobs: dict[str, list[dict]],
    bundles: list[tuple[str, int, list[dict], list[str]]],
    all_machines: list[str],
    existing_load: dict[str, int],
) -> None:
    """Greedy fallback: largest-first, minimise load + changeover proxy."""
    # Sort largest first
    sorted_bundles = sorted(bundles, key=lambda b: -b[1])

    load = dict(existing_load)
    tools_on: dict[str, int] = {mid: 0 for mid in all_machines}
    for mid in all_machines:
        tools_on[mid] = len({j["tool_id"] for j in machine_jobs[mid]})

    for tool_id, total_min, tjobs, eligible in sorted_bundles:
        best = min(eligible, key=lambda m: (
            load[m] + round(MACHINE_BY_ID[m].changeover_hours * 60) * tools_on[m]
        ))
        for job in tjobs:
            job["assigned_machine"] = best
            machine_jobs[best].append(job)
        load[best] += total_min
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
    time_limit_seconds: float = 200.0,
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

    # Horizon: must fit both the parallel-machine case AND the forced-
    # serialization case (when max_concurrent is small relative to the
    # number of machines with work).
    #
    # Parallel bound  = heaviest single machine's load + total CO blocking.
    # Serialized bound = total work across all machines / max_concurrent.
    #
    # Taking the max of both lets max_concurrent=1 work even when it
    # forces end-to-end serialization across machines.  Without this,
    # presolve proves infeasible before the solver ever branches.
    machine_batch_counts: dict[str, int] = {}
    for b in batches:
        machine_batch_counts[b.machine_id] = machine_batch_counts.get(b.machine_id, 0) + 1
    total_co = sum(
        max(0, count - 1) * round(MACHINE_BY_ID[mid].changeover_hours * 60)
        for mid, count in machine_batch_counts.items()
        if MACHINE_BY_ID[mid].has_changeovers
    )
    # Per-machine load: work + that machine's own changeovers
    machine_loads: dict[str, int] = {}
    for mid, count in machine_batch_counts.items():
        work = sum(b.total_minutes for b in batches if b.machine_id == mid)
        co = (max(0, count - 1) * round(MACHINE_BY_ID[mid].changeover_hours * 60)
              if MACHINE_BY_ID[mid].has_changeovers else 0)
        machine_loads[mid] = work + co
    max_machine_load = max(machine_loads.values()) if machine_loads else 0

    # Serialized bound: what if max_concurrent forces all work through a
    # narrow throat?  Total work + all CO time, divided by how many batches
    # can run at once.  Ceiling division to avoid off-by-one.
    total_work = sum(b.total_minutes for b in batches)
    total_all_co = sum(
        count * round(MACHINE_BY_ID[mid].changeover_hours * 60)
        for mid, count in machine_batch_counts.items()
        if MACHINE_BY_ID[mid].has_changeovers
    )
    mc_denom = max(1, max_concurrent)
    serialized_bound = (total_work + total_all_co + mc_denom - 1) // mc_denom

    horizon = max(max_machine_load, serialized_bound) + total_co + 480

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
    # Track changeover arcs per machine for crew-sandwich detection
    # co_arcs[machine_id][(i, j)] = lit  — arc literal for batch i→j with tool change
    co_arcs: dict[str, dict[tuple[int, int], cp_model.IntVar]] = {}

    # Explicit changeover interval variables for cross-machine NoOverlap.
    # Each changeover (initial or between batches) gets an optional interval
    # tied to the arc literal that activates it.  A global NoOverlap on
    # these prevents two maintenance changeovers from running simultaneously.
    co_intervals: list[cp_model.IntervalVar] = []

    # Track changeover start variables so we can extract them after solving.
    # co_start_vars[batch_id] = [(arc_lit, co_start_var), ...]
    co_start_vars: dict[int, list[tuple[cp_model.IntVar, cp_model.IntVar]]] = {}

    # Per-batch arc literal capture for crew-jump model (built later)
    first_lit_for: dict[int, cp_model.IntVar] = {}
    last_lit_for: dict[int, cp_model.IntVar] = {}
    outgoing_co_lits: dict[int, list[cp_model.IntVar]] = {}
    incoming_co_lits: dict[int, list[cp_model.IntVar]] = {}

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
            first_lit_for[bi.batch_id] = first_lit

            # Force in-progress batch to be first
            if bi.has_in_progress:
                model.add(first_lit == 1)

            # Initial tool changeover: if this batch is first and its tool
            # differs from the tool already loaded, it must wait for a changeover.
            if (init_tool and init_tool != bi.tool_id
                    and machine_co > 0 and not has_ip):
                # Changeover occupies [0, machine_co) before the batch starts
                co_end = model.new_int_var(0, horizon, f"ico_end_{machine_id}_{bi.batch_id}")
                model.add(co_end == machine_co).only_enforce_if(first_lit)
                model.add(
                    starts[bi.batch_id] >= co_end
                ).only_enforce_if(first_lit)
                co_iv = model.new_optional_interval_var(
                    0, machine_co, co_end,
                    first_lit, f"ico_iv_{machine_id}_{bi.batch_id}"
                )
                co_intervals.append(co_iv)
                co_penalty_terms.append((first_lit, machine_co))
                # Track: initial CO starts at minute 0
                ico_start = model.new_int_var(0, 0, f"ico_s_{machine_id}_{bi.batch_id}")
                co_start_vars.setdefault(bi.batch_id, []).append((first_lit, ico_start))

            # Arc: batch i+1 → depot (0) — batch i is last
            last_lit = model.new_bool_var(f"last_{machine_id}_{bi.batch_id}")
            arcs.append((i + 1, 0, last_lit))
            last_lit_for[bi.batch_id] = last_lit

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

                if gap > 0:
                    # Explicit changeover interval: can slide anywhere in the
                    # gap between batch i end and batch j start.  The NoOverlap
                    # constraint prevents two changeovers from running at once.
                    co_start = model.new_int_var(0, horizon, f"co_s_{machine_id}_{bi.batch_id}_{bj.batch_id}")
                    co_end_var = model.new_int_var(0, horizon, f"co_e_{machine_id}_{bi.batch_id}_{bj.batch_id}")
                    model.add(co_start >= ends[bi.batch_id]).only_enforce_if(lit)
                    model.add(co_end_var == co_start + gap).only_enforce_if(lit)
                    model.add(starts[bj.batch_id] >= co_end_var).only_enforce_if(lit)

                    co_iv = model.new_optional_interval_var(
                        co_start, gap, co_end_var,
                        lit, f"co_iv_{machine_id}_{bi.batch_id}_{bj.batch_id}"
                    )
                    co_intervals.append(co_iv)

                    co_penalty_terms.append((lit, gap))
                    co_arcs.setdefault(machine_id, {})[(i, j)] = lit
                    # Track: CO before batch j when arc i→j is active
                    co_start_vars.setdefault(bj.batch_id, []).append((lit, co_start))
                    # Tool-change arcs free crew on bi and require crew on bj
                    outgoing_co_lits.setdefault(bi.batch_id, []).append(lit)
                    incoming_co_lits.setdefault(bj.batch_id, []).append(lit)
                else:
                    model.add(
                        starts[bj.batch_id] >= ends[bi.batch_id]
                    ).only_enforce_if(lit)

                # HC transition penalty (collected for objective)
                if cfg.hc_penalty_weight > 0:
                    hc_delta = abs(bi.dominant_headcount - bj.dominant_headcount)
                    if hc_delta > 0.5:
                        penalty = int(round(hc_delta * cfg.hc_penalty_weight))
                        hc_penalty_terms.append((lit, penalty))

        model.add_circuit(arcs)

    # ── No simultaneous changeovers (global) ──────────────────
    # Maintenance changeovers share one crew — at most one at a time.
    if co_intervals:
        model.add_no_overlap(co_intervals)

    # ── Crew-sandwich penalty ─────────────────────────────────
    #
    # Penalize small batches sandwiched between two changeovers on the
    # same machine.  This pattern forces crew to jump away (changeover
    # before), jump back (short job), then jump away again (changeover
    # after), creating idle crew time the post-hoc optimizer can't fix.
    #
    # For each triple (i→j→k) where both arcs have tool changes and
    # batch j is short, add a penalty proportional to the changeover
    # time so the solver prefers merging or reordering to avoid the
    # sandwich.

    CREW_SANDWICH_THRESHOLD = 90  # minutes: batches shorter than this trigger penalty
    crew_sandwich_terms: list[tuple] = []

    for machine_id, arcs_dict in co_arcs.items():
        spec = MACHINE_BY_ID[machine_id]
        machine_co = round(spec.changeover_hours * 60) if spec.has_changeovers else 0
        m_batches_local = machine_batches[machine_id]

        for (i, j), lit_ij in arcs_dict.items():
            bj = m_batches_local[j]
            if bj.total_minutes >= CREW_SANDWICH_THRESHOLD:
                continue
            # Look for any arc j→k that also has a changeover
            for (j2, k), lit_jk in arcs_dict.items():
                if j2 != j:
                    continue
                # Both i→j and j→k are changeover arcs, and j is short.
                # Penalize when BOTH are active simultaneously.
                sandwich_lit = model.new_bool_var(
                    f"sandwich_{machine_id}_{i}_{j}_{k}"
                )
                model.add(lit_ij + lit_jk == 2).only_enforce_if(sandwich_lit)
                model.add(lit_ij + lit_jk < 2).only_enforce_if(~sandwich_lit)
                # Penalty = both changeovers worth of idle crew time
                penalty = 2 * machine_co
                crew_sandwich_terms.append((sandwich_lit, penalty))

    # ── Crew-idle penalty (bidirectional) ───────────────────────
    #
    # A maintenance changeover creates two crew-flow events:
    #
    #   1. OUTGOING: crew finishing batch_i is freed at ends[bi] and
    #      must jump to another machine (max-2-jumps-per-shift rule).
    #      Penalize the gap to the nearest other-machine batch start.
    #
    #   2. INCOMING: batch_j (after the changeover) needs fresh crew at
    #      starts[bj]. The crew must come from another machine's just-
    #      finished batch. Penalize the gap to the nearest other-machine
    #      batch end.
    #
    # Both sides must be aligned for a clean crew handoff. Modelling
    # only outgoing gaps lets the solver place changeovers such that
    # post-CO jobs start at times when no fresh crew is available —
    # producing post-hoc phantom gaps like the SMB→16C 93-min case.

    # Weight of 20: meaningful but not dominant.  In real schedules
    # most changeovers align with a batch start (gap≈0), so the
    # penalty only fires on a handful of real gaps.  Observed: a
    # 27-min gap costs 20*27=540 — enough for the solver to treat
    # it as a real optimization target without inflating makespan.
    # At weight 10, solver ignored gaps in favor of makespan.
    # At weight 100, solver sacrificed makespan to shave idle minutes.
    CREW_IDLE_WEIGHT = 20
    CREW_IDLE_CAP = 480    # cap at one shift — beyond this is equally bad
    crew_idle_terms: list = []

    # Pre-build other-machine batch lists — only machines that
    # realistically exchange crew.  Maintenance machines (16A/B/C, 8,
    # 6ST, RF) share crew with each other and with Machine 20 (which
    # receives crew but never sends).  Self-service machines (LMB, SMB)
    # have their own crew and never participate in maintenance crew
    # flow, so including them as candidates just inflates the model
    # with ~1,000+ variables that can never improve the solution.
    crew_exchange_machines: set[str] = set()
    for mid, spec in MACHINE_BY_ID.items():
        if not spec.self_service_changeover:
            crew_exchange_machines.add(mid)

    other_batches_for: dict[str, list[ToolBatch]] = {}
    for mid in machine_batches:
        other_batches_for[mid] = [
            b for b in batches
            if b.machine_id != mid and b.machine_id in crew_exchange_machines
        ]

    # Shared cap constant for capping penalties
    cap_const = model.new_int_var(CREW_IDLE_CAP, CREW_IDLE_CAP, "cidle_cap")

    # Deduplicate: compute gap vars once per unique source batch and
    # target batch, shared across arcs involving that batch.
    bi_gap_cache: dict[int, cp_model.IntVar] = {}  # bi.batch_id -> capped outgoing gap
    bj_gap_cache: dict[int, cp_model.IntVar] = {}  # bj.batch_id -> capped incoming gap

    def _build_outgoing_gap(bi: ToolBatch, candidates: list[ToolBatch]) -> cp_model.IntVar:
        """Return a capped IntVar = distance from crew-free time to
        nearest batch START on another machine."""
        if bi.batch_id in bi_gap_cache:
            return bi_gap_cache[bi.batch_id]
        crew_free = ends[bi.batch_id]
        gap_vars = []
        for cand in candidates:
            gap_cand = model.new_int_var(
                0, horizon,
                f"cidle_out_g_{bi.batch_id}_{cand.batch_id}"
            )
            model.add(gap_cand >= starts[cand.batch_id] - crew_free)
            model.add(gap_cand >= crew_free - starts[cand.batch_id])
            gap_vars.append(gap_cand)
        min_gap = model.new_int_var(0, horizon, f"cidle_out_min_{bi.batch_id}")
        model.add_min_equality(min_gap, gap_vars)
        capped = model.new_int_var(
            0, CREW_IDLE_CAP, f"cidle_out_cap_{bi.batch_id}"
        )
        model.add_min_equality(capped, [min_gap, cap_const])
        bi_gap_cache[bi.batch_id] = capped
        return capped

    def _build_incoming_gap(bj: ToolBatch, candidates: list[ToolBatch]) -> cp_model.IntVar:
        """Return a capped IntVar = distance from post-CO job start to
        nearest batch END on another machine (fresh crew source)."""
        if bj.batch_id in bj_gap_cache:
            return bj_gap_cache[bj.batch_id]
        need_time = starts[bj.batch_id]
        gap_vars = []
        for cand in candidates:
            gap_cand = model.new_int_var(
                0, horizon,
                f"cidle_in_g_{bj.batch_id}_{cand.batch_id}"
            )
            model.add(gap_cand >= ends[cand.batch_id] - need_time)
            model.add(gap_cand >= need_time - ends[cand.batch_id])
            gap_vars.append(gap_cand)
        min_gap = model.new_int_var(0, horizon, f"cidle_in_min_{bj.batch_id}")
        model.add_min_equality(min_gap, gap_vars)
        capped = model.new_int_var(
            0, CREW_IDLE_CAP, f"cidle_in_cap_{bj.batch_id}"
        )
        model.add_min_equality(capped, [min_gap, cap_const])
        bj_gap_cache[bj.batch_id] = capped
        return capped

    for machine_id, arcs_dict in co_arcs.items():
        spec = MACHINE_BY_ID[machine_id]
        # Only maintenance changeovers free/need crew (skip LMB/SMB self-service)
        if spec.self_service_changeover:
            continue

        m_batches_local = machine_batches[machine_id]
        candidates = other_batches_for[machine_id]
        if not candidates:
            continue

        for (i, j), lit_ij in arcs_dict.items():
            bi = m_batches_local[i]
            bj = m_batches_local[j]

            out_capped = _build_outgoing_gap(bi, candidates)
            in_capped = _build_incoming_gap(bj, candidates)

            # Gate by arc literal — only penalize active changeovers
            out_pen = model.new_int_var(
                0, CREW_IDLE_CAP, f"cidle_out_pen_{bi.batch_id}_{j}"
            )
            model.add(out_pen == out_capped).only_enforce_if(lit_ij)
            model.add(out_pen == 0).only_enforce_if(~lit_ij)

            in_pen = model.new_int_var(
                0, CREW_IDLE_CAP, f"cidle_in_pen_{bj.batch_id}_{i}"
            )
            model.add(in_pen == in_capped).only_enforce_if(lit_ij)
            model.add(in_pen == 0).only_enforce_if(~lit_ij)

            crew_idle_terms.append(out_pen)
            crew_idle_terms.append(in_pen)

    # ── Max concurrent machines (cumulative) ────────────────────

    all_intervals = [intervals[b.batch_id] for b in batches]
    all_demands = [1] * len(batches)
    model.add_cumulative(all_intervals, all_demands, max_concurrent)

    # ── Crew headcount capacity (cumulative) ────────────────────
    # Prevents the solver from scheduling more total headcount
    # than the available workforce across concurrent batches.

    if cfg.total_crew > 0:
        # Use ceiling rather than round: a fractional 11.4 headcount
        # demand still occupies 12 people on the floor, and rounding
        # down can cause the solver to over-schedule concurrent crew.
        hc_demands = [max(1, int(math.ceil(b.dominant_headcount))) for b in batches]
        model.add_cumulative(all_intervals, hc_demands, cfg.total_crew)

    # ── Crew jump ±2 tool-distance HARD constraint ───────────────
    #
    # When a maintenance changeover or end-of-work frees an operator
    # crew on machine A and another batch on machine B needs that crew,
    # the two batches' tool IDs must differ by at most 2 (numeric).
    # Pairs violating this rule are not modelled at all → the solver
    # must reorder / re-time batches so every needs-crew batch is
    # covered by either (a) a feasible ±2 jump donor or (b) a shift-
    # start CREW_POOL alignment.
    #
    # Donor eligibility: maintenance machine (has_changeovers and not
    #   self-service); batches whose successor arc is a tool-change OR
    #   that are last on their machine.
    # Recipient eligibility: any non-self-service machine (incl. M20);
    #   batches that are first on their machine OR follow a tool-change.
    # Numeric tool: first integer found in tool_id ("QPI123" → 123,
    #   "99999" → 99999); non-numeric → not jump-compatible.

    JUMP_TOL_MIN = 30           # crew may arrive up to 30 min before recipient start
    JUMP_WINDOW_MIN = 180       # crew may arrive up to 3 h after freed time
    SHIFT_BOUNDARY_TOL = 30     # tolerance for shift-start CREW_POOL alignment
    SHIFT_LEN = 480             # staffed minutes per shift (8 h)

    def _tool_numeric(tool_id: str) -> Optional[int]:
        if not tool_id:
            return None
        m = re.search(r"\d+", str(tool_id))
        return int(m.group()) if m else None

    batch_tool_num: dict[int, Optional[int]] = {
        b.batch_id: _tool_numeric(b.tool_id) for b in batches
    }

    # Build frees/needs reified booleans per batch (skip self-service)
    frees_lit: dict[int, cp_model.IntVar] = {}
    needs_lit: dict[int, cp_model.IntVar] = {}

    for b in batches:
        spec = MACHINE_BY_ID[b.machine_id]
        if spec.self_service_changeover:
            continue

        # Donor only if machine has maintenance changeovers (excludes M20)
        if spec.has_changeovers:
            out_lits = list(outgoing_co_lits.get(b.batch_id, []))
            ll = last_lit_for.get(b.batch_id)
            if ll is not None:
                out_lits.append(ll)
            if out_lits:
                fl = model.new_bool_var(f"frees_crew_{b.batch_id}")
                model.add_max_equality(fl, out_lits)
                frees_lit[b.batch_id] = fl

        # Recipient: any non-self-service machine (incl. machine 20)
        in_lits = list(incoming_co_lits.get(b.batch_id, []))
        fl0 = first_lit_for.get(b.batch_id)
        if fl0 is not None:
            in_lits.append(fl0)
        if in_lits:
            nl = model.new_bool_var(f"needs_crew_{b.batch_id}")
            model.add_max_equality(nl, in_lits)
            needs_lit[b.batch_id] = nl

    # Build jump-pair vars cross-machine, pre-filtered by ±2 numeric tool
    jump_vars: dict[tuple[int, int], cp_model.IntVar] = {}
    jumps_from: dict[int, list[cp_model.IntVar]] = {}
    jumps_to: dict[int, list[cp_model.IntVar]] = {}

    for bi in batches:
        if bi.batch_id not in frees_lit:
            continue
        ti = batch_tool_num[bi.batch_id]
        if ti is None:
            continue
        for bj in batches:
            if bi.machine_id == bj.machine_id:
                continue
            if bj.batch_id not in needs_lit:
                continue
            tj = batch_tool_num[bj.batch_id]
            if tj is None:
                continue
            if abs(ti - tj) > 2:
                continue
            x = model.new_bool_var(f"jump_{bi.batch_id}_to_{bj.batch_id}")
            jump_vars[(bi.batch_id, bj.batch_id)] = x
            jumps_from.setdefault(bi.batch_id, []).append(x)
            jumps_to.setdefault(bj.batch_id, []).append(x)

            # Active jump implies donor frees and recipient needs
            model.add_implication(x, frees_lit[bi.batch_id])
            model.add_implication(x, needs_lit[bj.batch_id])

            # Time window: ends[i] - tol <= starts[j] <= ends[i] + window
            model.add(
                starts[bj.batch_id] >= ends[bi.batch_id] - JUMP_TOL_MIN
            ).only_enforce_if(x)
            model.add(
                starts[bj.batch_id] <= ends[bi.batch_id] + JUMP_WINDOW_MIN
            ).only_enforce_if(x)

    # At-most-one donor per recipient and at-most-one recipient per donor
    for xs in jumps_from.values():
        if len(xs) > 1:
            model.add(sum(xs) <= 1)
    for xs in jumps_to.values():
        if len(xs) > 1:
            model.add(sum(xs) <= 1)

    # Recipient coverage: needs_lit[j] ⇒ at_shift_start[j] OR sum(jumps_to[j]) ≥ 1
    boundaries = list(range(0, horizon + 1, SHIFT_LEN))

    for b in batches:
        if b.batch_id not in needs_lit:
            continue

        # at_shift_start[j]: solver sets True iff starts[j] is within
        # ±SHIFT_BOUNDARY_TOL of some shift boundary (multiples of 480
        # in the staffed-minute timeline).
        boundary_lits: list[cp_model.IntVar] = []
        for k in boundaries:
            bl = model.new_bool_var(f"at_sb_{b.batch_id}_{k}")
            model.add(starts[b.batch_id] >= k - SHIFT_BOUNDARY_TOL).only_enforce_if(bl)
            model.add(starts[b.batch_id] <= k + SHIFT_BOUNDARY_TOL).only_enforce_if(bl)
            boundary_lits.append(bl)

        at_sb = model.new_bool_var(f"at_shift_start_{b.batch_id}")
        if boundary_lits:
            model.add_max_equality(at_sb, boundary_lits)
        else:
            model.add(at_sb == 0)

        coverage_terms = jumps_to.get(b.batch_id, [])
        if coverage_terms:
            model.add(
                at_sb + sum(coverage_terms) >= 1
            ).only_enforce_if(needs_lit[b.batch_id])
        else:
            # No feasible jump donor exists → must align with shift start
            model.add(at_sb >= 1).only_enforce_if(needs_lit[b.batch_id])

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
    if co_penalty_terms:
        co_term = sum(lit * pen for lit, pen in co_penalty_terms)

    # ── Layer 4d: Crew-sandwich penalty ──────────────────────────
    sandwich_term = 0
    if crew_sandwich_terms:
        sandwich_term = sum(lit * pen for lit, pen in crew_sandwich_terms)

    # ── Layer 4e: Compactness — push FIRST batch per machine early ────
    # Only the earliest batch on each machine determines shift-start
    # alignment (06:30).  Pushing ALL batches early drowns out the
    # crew-idle and changeover penalties since it's proportional to
    # every batch's start time.  Instead, compute min(starts) per
    # machine via add_min_equality — this is ~9 vars instead of ~30,
    # and only penalizes the batches that actually matter for
    # shift-boundary alignment.
    first_starts: list[cp_model.IntVar] = []
    for machine_id, m_batches in machine_batches.items():
        if not m_batches:
            continue
        m_min_start = model.new_int_var(
            0, horizon, f"first_start_{machine_id}"
        )
        model.add_min_equality(
            m_min_start, [starts[b.batch_id] for b in m_batches]
        )
        first_starts.append(m_min_start)
    compact_term = 3 * sum(first_starts) if first_starts else 0

    # ── Layer 4f: Crew-idle penalty ──────────────────────────────
    crew_idle_term = 0
    if crew_idle_terms:
        crew_idle_term = CREW_IDLE_WEIGHT * sum(crew_idle_terms)

    # ── Combine layers ─────────────────────────────────────────
    # late_term >> prio_term >> makespan + hc_term + co_term + compact
    # Scale prio_term above makespan but below late_term
    has_prio = cfg.priority_boost or any(
        any(j.get("is_picked") for j in b.jobs) for b in batches
    )
    has_late = cfg.minimize_late and (late_vars or tardiness_vars)

    objective = makespan + hc_term + co_term + sandwich_term + crew_idle_term + compact_term
    if has_prio:
        objective += prio_term * n_batches
    if has_late:
        objective += late_term * n_batches * horizon

    model.minimize(objective)

    # ── Solve ───────────────────────────────────────────────────

    solver = cp_model.CpSolver()
    # P+ Boost needs more time — priority terms make convergence slower
    effective_limit = time_limit_seconds + (300 if cfg.priority_boost else 0)
    solver.parameters.max_time_in_seconds = effective_limit
    # Use all available CPU cores. At 12+ workers, CP-SAT activates
    # additional sub-solver strategies (LNS, core-based, etc).
    solver.parameters.num_workers = min(15, os.cpu_count() or 8)
    # Log objective bound progression — useful for tuning weights.
    solver.parameters.log_search_progress = True

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
        # Extract changeover start for this batch (if any arc with CO is active)
        co_min: Optional[int] = None
        for lit, co_var in co_start_vars.get(b.batch_id, []):
            if solver.value(lit):
                co_min = solver.value(co_var)
                break
        scheduled.append(ScheduledBatch(batch=b, start_minute=s, end_minute=e, co_start_minute=co_min))

    # Sort by start time for readability
    scheduled.sort(key=lambda sb: (sb.start_minute, sb.batch.machine_id))

    ms = solver.value(makespan)
    return SolverResult(scheduled, ms, status_name)
