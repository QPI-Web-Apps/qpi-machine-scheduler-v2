# H3 Implementation Plan: No Simultaneous Changeovers

## Problem Statement

The factory has a rule (H3): only 1 maintenance changeover can happen at a time across all machines. Subject machines: 16A, 16B, 16C, 8, 6ST, RF (2-hour CHANGEOVER). Exempt: Machine 20 (no changeovers), LMB/SMB (self-service TOOL_SWAP, 15 min).

## What We Tried and Learned

### Approach 1: Circuit Constraint in CP-SAT Solver

**What**: Replaced pairwise batch ordering with a circuit constraint per machine. Arc literals gave "immediate predecessor" identity, enabling precise changeover intervals anchored to `ends[predecessor]`. Global `AddNoOverlap` on all maintenance changeover intervals.

**Result**: H3 was correctly enforced (0 violations in solver output, 2 from crew push-forward). But the circuit constraint produced fundamentally different batch orderings — the solver spread work across machines differently, creating idle gaps and breaking crew jump alignment.

**Root cause**: The circuit gives the solver MORE freedom than pairwise ordering (pairwise enforces constraints between ALL pairs, circuit only between immediate neighbors). This larger feasible space leads to solutions with the same makespan but worse crew flow. A pack-early tiebreaker helped but made the solver too slow.

### Approach 2: Pairwise Ordering + Batch-Start-Anchored H3 Intervals

**What**: Kept original pairwise ordering. Added H3 intervals anchored to `[start[batch] - co_min, start[batch]]` — doesn't need predecessor identity.

**Result**: Same schedule quality as original, but H3 had high violation count (10-11). The intervals don't match where assembly actually places changeovers (at `prev_end`, not at `start[batch] - co_min`). When there's slack between changeover end and batch start, the solver thinks changeovers are at different times than they actually are.

### Approach 3: Pairwise + Predecessor-End Variable (pred_end)

**What**: Created `pred_end[batch]` variables bounded by `end[predecessor] <= pred_end <= start[batch] - co_min`. H3 intervals anchored to pred_end.

**Result**: Same issue — the solver has no incentive to minimize pred_end, so it floats to the upper bound (batch-start-anchored), giving the same slack problem as Approach 2.

### Approach 4: Post-Assembly H3 Enforcement (current)

**What**: Let the solver produce the original schedule (no H3 in solver). After assembly, scan for overlapping changeovers and delay the later one. Insert NOT_RUNNING gaps to absorb shifts. Run twice (before and after crew annotation).

**Result**: 0 H3 violations. Same makespan. Same batch ordering. BUT: the post-hoc delays create dead time the solver didn't plan for. Machines sit idle waiting for their H3 slot. Crew is freed during changeovers and has nowhere productive to go during the wait.

### Key Observation

**The solver must know about H3 during planning, not after.** When the solver plans without H3, it produces a schedule where changeovers naturally overlap. Fixing this after the fact creates gaps and stranded crew. The solver needs to arrange batches so that changeovers are naturally staggered AND crew can flow smoothly between machines during changeover windows.

## Recommended Approach: Two-Phase Solver

### Phase 1: CP-SAT Solver with Crew-Aware H3

Keep the pairwise ordering (proven to produce good batch arrangements). Add H3 to the solver, but also add constraints/objectives that ensure crew can flow:

**H3 constraint**: Use the existing `h3_enabled` config flag. For each pair of maintenance changeover windows across different machines, ensure they don't overlap. The changeover window for batch B is the gap `[end[predecessor], start[B]]` where the gap includes the changeover duration.

**Crew flow objective**: Add a secondary objective that rewards solutions where:
- When a changeover frees crew on machine A, there's a job starting on machine B within a short window (< 30 min)
- When a changeover finishes on machine A, there's crew being freed from machine B within a short window
- This creates the "circular crew flow" pattern: A's crew → B, B's crew → C, C's crew → A

**Implementation sketch**:
```
For each changeover gap on machine M:
  co_start = end[last_batch_before_gap]
  co_end = co_start + co_min
  
  # H3: no overlap with other machine changeovers
  For each changeover gap on machine N (N != M):
    Disjunctive: co_end_M <= co_start_N OR co_end_N <= co_start_M
  
  # Crew alignment: reward having a job start on another machine near co_start
  # (so freed crew has somewhere to go immediately)
  For each first-in-batch job J on machine N:
    alignment_bonus if |start[J] - co_start_M| < 30 min
```

The challenge is expressing "immediate predecessor" in pairwise ordering. Options:
- Use the circuit constraint but add pack-early terms more carefully
- Use pairwise but derive predecessor identity via auxiliary variables
- Accept that the solver may not find the globally optimal crew flow but at least avoids the worst cases

### Phase 2: Event-Driven Simulation (Crew Bridging)

After the solver produces an H3-aware schedule, run a lightweight event-driven simulation that:

1. **Walks forward in time** through all machines simultaneously
2. **At each changeover start**: checks H3 (should be clear if solver did its job), frees crew, looks for a bridge target
3. **Crew bridging**: if freed crew can do a short job on another machine and return before the changeover finishes, bridge them there. Otherwise, crew waits.
4. **At each changeover end**: assigns crew to the next batch. If no crew available, marks as NO_CREW and waits for crew from another machine's changeover.
5. **At shift boundaries**: reassigns crew fresh (shift-sticky reset)

This simulation layer handles the operational details the solver can't: crew bridging, shift boundaries, max 2 jumps per shift, headcount matching. It's essentially a lightweight version of the Rev8 engine's tick loop, operating on the solver's optimized plan.

**Key functions from Rev8 reference doc to reimplement**:
- `_find_active_changeover_end()` — H3 check (line 1129 in reference)
- `_bridge_crew_after_changeover()` — crew transfer during changeover (line 1387)
- `_has_immediate_shift_sticky_jump_target()` — bridge target check (line 620)
- `CrewLedger` — tracks which crew is on which machine

### Implementation Order

1. **Phase 2 first** (simulation layer): Replace `_compute_crew_movements()` with an event-driven simulation that walks the schedule and properly manages crew. Keep the current post-assembly H3 enforcement as a safety net. This alone will fix the "crew sitting around" issue because the simulation can bridge crew to short tasks during waits.

2. **Phase 1 second** (solver H3): Once the simulation layer is working, add H3 constraints to the solver. The simulation validates the solver's plan, so we can iterate on the solver constraints knowing the simulation will catch any issues.

3. **Remove post-assembly H3**: Once the solver handles H3 natively, the post-assembly enforcement becomes redundant.

## Current Code State (2026-04-03)

### Changes kept:
- **scheduler.py**: Post-assembly H3 enforcement (`_enforce_h3`), runs before and after crew annotation
- **scheduler.py**: Crew first-in-batch fix — `job_starts` filtered to only include jobs that follow a CHANGEOVER/TOOL_SWAP/NOT_RUNNING (prevents crew being sent to already-staffed machines)
- **api.py**: `h3_enabled` parameter exposed (default `true`)
- **scheduler_io.py**: `h3_enabled: bool = True` in SchedulerConfig (pre-existing)

### Not changed:
- **solver.py**: Original pairwise ordering, no H3 constraints
- **models.py**: Unchanged
- **export.py**: Unchanged
- **calendar_utils.py**: Unchanged

### Known issues:
- Post-assembly H3 creates dead time (NOT_RUNNING gaps) that the solver didn't plan for
- Crew is freed during changeovers with no nearby target (waits hours)
- Machine 20 (no changeovers) starts late when its only crew source is a changeover on another machine
- Frontend "Crew Jumps" counter uses `toISOString()` (UTC) instead of local date, showing wrong count per day
