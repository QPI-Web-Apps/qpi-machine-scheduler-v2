# QPI Scheduler Logic & Rules Reference

> Exhaustive extraction of every logic rule, constraint, and decision path in the Rev8 scheduling engine.

---

## Table of Contents

1. [Core Scheduling Loop](#1-core-scheduling-loop)
2. [Job Selection](#2-job-selection)
3. [Crew Management](#3-crew-management)
4. [Changeover Rules](#4-changeover-rules)
5. [Machine Registry](#5-machine-registry)
6. [Calendar & Time](#6-calendar--time)
7. [Priority Classes](#7-priority-classes)
8. [Special Rules](#8-special-rules)
9. [Data Flow](#9-data-flow)
10. [Configuration Parameters](#10-configuration-parameters)
11. [MachineState Fields](#11-machinestate-fields)
12. [CrewLedger Internals](#12-crewledger-internals)
13. [Post-Processing Pipeline](#13-post-processing-pipeline)
14. [API Endpoints](#14-api-endpoints)
15. [Input/Output Format](#15-inputoutput-format)
16. [Helper Utilities](#16-helper-utilities)

---

## 1. Core Scheduling Loop

**File:** `src/scheduling_core.py`, function `schedule_all_machines()` (lines 2316-2958)

### Architecture

Event-driven global tick loop (not fixed-increment). Always picks the machine with the earliest cursor, processes it, then picks the next earliest. All 9 machines share a single `CrewLedger`, a single `remaining` DataFrame, and a single `all_scheduled` set.

### Machine Selection (per tick)

```python
ms = min(
    active,
    key=lambda m: (
        align_to_working_time(m.cursor, m.shifts_per_day),
        _next_event_rank(m, ...),
        m.machine_id,
    ),
)
```

Selection priority (lexicographic):

1. **Earliest aligned cursor** -- machine with the earliest "real" time goes first.
2. **Next-event rank** via `_next_event_rank()` (line 1610):
   - Rank 0: Machine needs a changeover AND a bridge target exists right now
   - Rank 1: Machine has same-tool work available (no changeover needed), or machine has no changeovers
   - Rank 2: Machine needs a changeover with a future bridge target, or has no tool set yet
   - Rank 4: Machine's candidate pool is empty
   - Rank 5: Machine needs a changeover with NO bridge target at all
3. **Machine ID** -- alphabetical tiebreaker for determinism.

### Decision Tree (per tick)

```
1. Align cursor to working time
2. Check shift boundary -> sync all crew ownership if crossed
3. If lost crew this shift and no capacity -> NOT_RUNNING to next shift, CONTINUE
4. Get candidate pool
   |-- Pool empty?
   |   |-- Strip orphaned trailing changeover
   |   |-- Wake any waiting_for_crew machine
   |   |-- Mark machine DONE (cursor = _DONE)
   |   |-- CONTINUE
   |
5. Check receiver hold
   |-- Should hold for incoming bridge? -> NOT_RUNNING until bridge source cursor, CONTINUE
   |
6. Select job via _select_job_v8
   |-- No job selected?
   |   |-- Wait for crew.find_earliest_crew_free or next shift
   |   |-- CONTINUE
   |
7. Min remaining shift time check
   |-- Too little time left? -> NOT_RUNNING to next shift, CONTINUE
   |
8. Changeover needed?
   |-- NO: Go to step 9 (crew availability)
   |-- YES:
   |   |-- Try back-to-back collapse (rewrite prior CHANGEOVER)
   |   |-- Self-service (TOOL_SWAP)?
   |   |   |-- Insert TOOL_SWAP, place job immediately, CONTINUE
   |   |-- Maintenance CHANGEOVER:
   |       |-- Compute freed_hc
   |       |-- Check local continuation path
   |       |-- H3 check: another changeover active?
   |       |   |-- YES: Try H3-wait bridge
   |       |   |   |-- Bridge succeeded: update remaining, advance cursor, CONTINUE
   |       |   |   |-- Bridge failed: NOT_RUNNING until active CO ends, CONTINUE
   |       |-- Deferral check: owns crew? immediate bridge available?
   |       |   |-- No immediate bridge, can defer: look for future bridge time
   |       |   |   |-- Found: NOT_RUNNING until future bridge time, CONTINUE
   |       |-- Execute changeover (insert CHANGEOVER entry)
   |       |-- Post-changeover bridge attempt
   |       |   |-- Normal jump (count=0) or fallback jump (count=1, no local continuation)
   |       |   |-- If bridged: place job on target, update remaining
   |       |-- CONTINUE (changeover complete, next iteration picks up post-CO)
   |
9. Crew availability check:
   |-- Can run (owned crew / fresh shift / mid-shift reacquisition)?
   |   |-- Post-changeover min shift time check
   |   |-- Claim crew ownership if needed
   |   |-- Detect HC transitions
   |   |-- Place job (_place_job), staff machine, update remaining
   |-- Cannot run:
       |-- No owned crew, not shift start: wait for earliest_crew_free or next shift
       |-- Otherwise: wait for earliest_crew_free or next shift (clearing HC state)
```

### Loop Termination

- `remaining.empty` -- all jobs scheduled
- No active machines remain (`cursor >= _DONE` for all)
- Safety cap hit: `max_iters = len(remaining) * 20 + 2000`

### Loop Invariant

Every iteration either schedules a job, inserts a wait (NOT_RUNNING), or marks a machine DONE. The cursor always advances by at least `timedelta(minutes=1)`.

### Initialization Phase (lines 2330-2431)

1. **Workforce estimation**: Sum of each machine's peak HC job = `crew.total_workforce`
2. **Job preparation**: `due_sort` (due date, NaT → datetime.max), `so_sort` (SO# string), `_presort_jobs_by_tool_batching` (rank by total tool-batch hours)
3. **Pre-place locked jobs**: In-progress pinned jobs placed first, respecting max_staffed cap. Changeovers inserted if tool differs.
4. **Realized workforce seeding**: For staffed machines, uses actual HC. For others, uses peak HC from pool. Sets `realized_workforce = max(current, sum)`.

### Shift Boundary Handling (lines 2449-2465)

When any machine crosses a shift boundary:
1. Expired staffing records freed from `crew.staffed`
2. ALL machines' crew ownership synced via `_sync_shift_crew_ownership_at()`
3. Done machines release ownership
4. Machines with current shift ownership re-assert in `crew.owned`
5. Machines without ownership release from `crew.owned`

### Three Pathways to Run a Job (lines 2809-2853)

1. **Owned crew**: Machine already owns its shift crew. Just needs to be under the running cap.
2. **Fresh shift start**: Machine has no crew but is at a shift-start instant, under cap, and enough free workers.
3. **Mid-shift re-acquisition**: Machine lost crew mid-shift but capacity and workers are available.

---

## 2. Job Selection

**File:** `src/scheduling_core.py`, function `_select_job_v8()` (lines 793-933)

### Target HC Determination (cascade)

1. `ms.owned_headcount` (if `ms.owns_crew`) -- sticky shift crew ownership
2. `ms.headcount_target` -- set from prior jobs in the shift
3. `_expected_incoming_hc(ms, crew)` -- HC of the soonest-finishing other machine
4. `None` -- no HC constraint, disables HC filtering

### HC Filter (soft preference)

```python
_headcount_filter(target_hc, pool, hc_flex):
    lo = target_hc - hc_flex
    hi = target_hc + hc_flex
    return pool[(pool["headcount"] >= lo) & (pool["headcount"] <= hi)]
```

- Default `hc_flex = 2` (configurable)
- If no jobs match within band: use full pool with `_hc_dist` sort column (proximity-based fallback)

### Same-Tool Preference (lines 856-862)

If the machine has a current tool AND there are matching jobs, narrow pool to same-tool jobs only. Avoids changeovers whenever possible.

### Startup Batching (lines 864-878)

On non-16-group machines with no current tool and multiple tool groups: pick the tool group with the most total remaining hours to minimize future changeovers.

### Complete 14-Level Sort Key

| Position | Column | Direction | Condition |
|----------|--------|-----------|-----------|
| 1 | `_hc_dist` | ASC | Only if HC fallback triggered |
| 2 | `_tool_batch_hrs` | ASC (neg) | Only if startup batching |
| 3 | `_locked_sort` | ASC | 0=locked first |
| 4 | `_is_pp` | ASC | 0=Priority Plus first |
| 5 | `_picked_sort` | ASC | 0=picked first |
| 6 | `prio_class` | ASC | 0=PP, 1=P, 2=PD, 3=Normal |
| 7 | `_co_horizon_rank` | ASC | Only if changeover + cfg available |
| 8 | `_co_horizon_cost` | ASC | Only if changeover + cfg available |
| 9 | `_pref_penalty` | ASC | 0=preferred machine |
| 10 | `_eligible_count` | ASC | Fewer eligible machines first |
| 11 | `_tool_batch_hrs` | ASC (neg) | Only if changeover batching (not startup) |
| 12 | `sort_prio` | ASC | Pre-computed tool batch rank |
| 13 | `due_sort` | ASC | Earlier due date first |
| 14 | `so_sort` | ASC | SO# string (stable tiebreaker) |

First row after sorting is returned as the selected job.

### Changeover Horizon Metrics (lines 696-774)

When a changeover is needed, each candidate is scored by bridge outlet quality:

- **Rank 0**: No changeover risk (same tool, self-service, first job fills shift, or same-shift continuation)
- **Rank 1**: Immediate bridge target exists when changeover begins
- **Rank 2**: Bridge target exists later in the same staffed day (cost = wait hours)
- **Rank 3**: No same-day bridge outlet (cost = stranded hours until shift end)

### Candidate Pool Building (`_candidate_pool_for_machine`, line 351)

1. Already-scheduled SO numbers excluded
2. Machine eligibility via `_elig_{machine_id}` column or `eligible_machines` or `machine` column
3. Exclusive tool ownership: if ALL jobs for a tool have a single `preferred_machine` != current machine, those tools are excluded (prevents tool stealing)

---

## 3. Crew Management

### CrewLedger Class (lines 87-157)

| Field | Type | Purpose |
|-------|------|---------|
| `staffed` | `Dict[str, Tuple[float, datetime]]` | Machine → (HC, job_end_time). Active running jobs. |
| `owned` | `Dict[str, float]` | Machine → HC. Sticky crew ownership for the shift. |
| `total_workforce` | `float` | Legacy theoretical max (not used in decisions). |
| `realized_workforce` | `float` | Actual peak concurrent HC. The real constraint. Ratchets up only. |

| Method | Logic |
|--------|-------|
| `machines_running_at(t)` | Count machines in `staffed` with `end > t` |
| `machines_with_owned_crews()` | Count machines in `owned` with `hc > 0` |
| `available_workers_at(t)` | `realized_workforce - sum(owned.values())` |
| `can_staff_machine(t, needed_hc, max_machines, flex)` | `owned_count < max_machines` AND `available >= needed_hc - flex - 1e-9` |
| `free_machine(m)` | Remove from `staffed`, return freed HC. Does NOT touch `owned`. |
| `own_machine(m, hc)` | Set `owned[m] = hc`. If total owned > realized, ratchet realized up. |
| `release_owned(m)` | Pop from `owned` |
| `staff_machine(m, hc, end)` | Set `staffed[m] = (hc, end)`. Also `own_machine` if not already owned. |
| `find_earliest_crew_free(t)` | Earliest `end` across all staffed machines with `end > t` |
| `soonest_finishing(exclude)` | `(machine_id, hc, end)` for the soonest-finishing other machine |

### Crew Ownership Model

- **Shift-sticky**: Once a machine acquires crew for a shift (via `_set_shift_crew_owner`), it holds ownership until explicitly released or shift ends
- **Max 2 jumps per shift**: `crew_jump_count` capped at 2 (line 1406)
- **Realized workforce ratchets upward**: Only increases, never decreases. Tracks peak concurrent owned HC.
- **Crew day boundary**: Anchored on 06:30 (`SHIFT1_START`), not midnight. Bridges cannot cross crew day boundaries.

### Crew Bridging (`_bridge_crew_after_changeover`, lines 1387-1523)

**Guard conditions**:
- `freed_hc > 0`
- `crew_jump_count < 2`

**Jump type classification**:
- `"bridge"` -- normal changeover bridge
- `"h3_wait_bridge"` -- crew redeployed while waiting for H3 slot
- `"sticky_fallback_jump"` -- second hop (jump_count >= 1)
- `"sticky_activation_jump"` -- bridging to a dormant machine

**State mutations**:
1. If target was dormant, activate it
2. Release source's crew ownership (`lost_for_shift=True`)
3. Set target's crew ownership
4. Place first job on target machine
5. Append to `jump_log`

### Bridge Target Scoring (`_choose_shift_sticky_jump_target`, lines 1312-1384)

For each candidate target:
1. Filter out: source machine, machines with current shift ownership, done machines, currently running machines
2. HC proximity filter: `hc_dist <= max(crew_transition_flex, hc_flex)`
3. Sort candidates by: same_tool desc, hc_dist asc, backlog_hrs desc, due_sort asc, so_sort asc
4. Score: `same_tool * 10.0 + backlog_hrs - hc_dist * 2.0 + same_shift_fill`

### Changeover Deferral Logic (main loop, lines 2714-2755)

When changeover needed but no immediate bridge target:
1. Check `can_defer_changeover` (owns shift crew OR at shift start)
2. Check `has_local_continuation` (enough same-shift work after changeover)
3. If deferrable + no immediate bridge + no local continuation → look for `_next_deferred_changeover_bridge_time`
4. If future bridge found → insert NOT_RUNNING until then
5. For shift-start changeovers, fallback to `_next_bridge_target_time`

### NOT_RUNNING Triggers

| Trigger | Location |
|---------|----------|
| Mid-shift lost-crew re-acquisition fails | line 2485 |
| H3 blocking (another changeover active) | line 2711 |
| Changeover deferral (waiting for bridge target) | lines 2753-2754 |
| No HC-matched job found, no crew available | lines 2553-2568 |
| Receiver hold (machine held as bridge receiver) | lines 2533-2536 |
| Minimum remaining shift hours check | lines 2587-2589 |
| Not enough crew to start | lines 2924-2955 |

### Idle Types

- **`NO_CREW`** -- Used in NOT_RUNNING blocks. Machine has no assigned crew at all.
- **`CREW_WAITING`** -- Used in IDLE_CREW entries. Crew IS on machine but waiting idle.
- **`UNBALANCED_CREW`** -- IDLE_CREW variant. Crew present but HC doesn't match job.

---

## 4. Changeover Rules

### Changeover Types

| Machine | Entry Type | Duration | Crew | H3 Subject |
|---------|------------|----------|------|------------|
| 16A/B/C, 8, 6ST, RF | CHANGEOVER | 2.0h | Freed | Yes |
| LMB, SMB | TOOL_SWAP | 0.25h | Stays | No |
| 20 | None | N/A | N/A | N/A |

### When Changeover is Needed (line 2600)

```python
needs_changeover = has_changeovers and ms.current_tool is not None and ms.current_tool != selected["tool_id"]
```

All three conditions must hold:
1. Machine supports changeovers (`has_changeovers=True`)
2. A tool is currently loaded (`current_tool is not None`)
3. Selected job's tool differs from current tool

### `_insert_changeover()` (lines 936-994)

- Gets per-machine changeover hours from `machine_spec.changeover_hours`
- Duration is **wall-clock** (not staffed time): `end = start + timedelta(hours=co_hours)`
- Entry type: `"TOOL_SWAP"` if self-service, else `"CHANGEOVER"`
- State updates: `ms.cursor = end`, `ms.current_tool = next_tool`, `ms.changeover_end = end`
- Displaced crew: `ms.displaced_crew = displaced` (or 0 for self-service)

### Back-to-Back Changeover Collapse (lines 2607-2635)

If no JOB was placed since the last CHANGEOVER, the previous changeover's target tool is rewritten to point at the new tool. The `Tool ID` field updates from `"old -> intermediate"` to `"old -> new"`. Avoids double changeover cost.

### TOOL_SWAP Path (LMB/SMB, lines 2641-2663)

- Crew stays on machine (no displacement, no bridge)
- Job placed immediately after the swap
- No H3 check

### Maintenance CHANGEOVER Path (lines 2665-2791)

1. Compute freed HC via `_current_displaced_headcount()`
2. Check local continuation path (suppress deferral if work exists)
3. H3 check → if blocked, try H3-wait bridge → if fails, NOT_RUNNING until active CO ends
4. Deferral check → if no immediate bridge and can defer, look for future bridge time
5. Execute changeover → `crew.free_machine()`, `_insert_changeover()`
6. Post-changeover bridge attempt → transfer crew to another machine

### H3 Rule (Max 1 Simultaneous Maintenance Changeover)

**Config**: `cfg.h3_enabled: bool = True` (on by default)

**Check**: `_find_active_changeover_end()` (line 1129) scans ALL machines for any `CHANGEOVER` entry (NOT `TOOL_SWAP`) where `co_start <= at_time < co_end`.

**Subject to H3**: 16A, 16B, 16C, 8, 6ST, RF (all machines producing `CHANGEOVER` entries)

**Exempt from H3**: Machine 20 (no changeovers), LMB, SMB (self-service `TOOL_SWAP`)

**When blocked**:
1. Try to bridge crew to another machine during wait (`bridge_context="h3_wait"`)
2. If bridge succeeds, crew works elsewhere while waiting
3. If bridge fails, insert NOT_RUNNING until blocking changeover ends

### Orphaned Changeover Cleanup (lines 2496-2505)

When a machine's candidate pool is empty and the last schedule entry is a CHANGEOVER leading nowhere, the changeover is **popped** and cursor/tool reverted.

---

## 5. Machine Registry

**File:** `src/models.py`

### MachineSpec Dataclass (lines 17-29)

| Field | Type | Default |
|-------|------|---------|
| `machine_id` | str | required |
| `display_name` | str | required |
| `station_group` | str | required |
| `eqp_pattern` | str | required |
| `default_shifts` | int | 2 |
| `has_changeovers` | bool | True |
| `labeler_machine` | bool | False |
| `changeover_hours` | float | 2.0 |
| `self_service_changeover` | bool | False |
| `always_changeover` | bool | False (unused) |

### All 9 Machines

| ID | Display | Group | EQP Pattern | Changeover | Self-Service | Labeler | Notes |
|----|---------|-------|-------------|------------|--------------|---------|-------|
| 16A | 16S-A | 16 | `16ST\|16S-` | 2.0h | No | No | Multi-machine group |
| 16B | 16S-B | 16 | `16ST\|16S-` | 2.0h | No | No | Multi-machine group |
| 16C | 16S-C | 16 | `16ST\|16S-` | 2.0h | No | **Yes** | Labeler-only eligible |
| 20 | 20S | 20 | `20S` | **None** | No | No | Permanent tool, no changeovers |
| 8 | 8S | 8 | `8ST` | 2.0h | No | No | Standard |
| LMB | LMB | lmb | `LMB` | **0.25h** | **Yes** | No | Self-service TOOL_SWAP |
| SMB | SMB | smb | `SMB` | **0.25h** | **Yes** | No | Self-service TOOL_SWAP |
| 6ST | 6ST | 6st | `6ST` | 2.0h | No | No | Standard |
| RF | RF | rf | `\bRF\b` | 2.0h | No | No | Blank tools get "99999" |

### Station Groups

- `"16"` -- 16A, 16B, 16C (only multi-machine group)
- `"20"` -- 20 (single machine)
- `"8"` -- 8 (single machine)
- `"lmb"` -- LMB (single machine)
- `"smb"` -- SMB (single machine)
- `"6st"` -- 6ST (single machine)
- `"rf"` -- RF (single machine)

### Staggered Activation (Dormant Machines)

The `dormant` field and activation machinery is fully built but **currently inactive** -- no configuration path sets machines to dormant. All machines start with `dormant=False`. The `initial_machines` parameter referenced in CLAUDE.md is not recognized by `SchedulerConfig`.

When a dormant machine IS activated (via bridge target selection):
```python
target_ms.dormant = False
target_ms.activated_at = start_time
target_ms.activated_by = source_ms.machine_id
```

---

## 6. Calendar & Time

**File:** `src/calendar_utils.py`

### Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `SHIFT1_START` | `time(6, 30)` | Shift 1 start |
| `SHIFT1_END` | `time(14, 30)` | Shift 1 end |
| `SHIFT2_START` | `time(14, 30)` | Shift 2 start |
| `SHIFT2_END` | `time(22, 30)` | Shift 2 end |
| `SHIFT3_START` | `time(22, 30)` | Shift 3 start (optional) |
| `SHIFT3_END` | `time(6, 30)` | Shift 3 end (next day) |
| `SHIFT_BREAK_MINUTES` | `0` | No mid-shift breaks |
| `_DONE` | `datetime(9999, 1, 1)` | Machine finished sentinel |

### Working Days

- Mon-Fri only (`d.weekday() < 5`)
- Saturday (5) and Sunday (6) are non-working
- No `WORKING_WEEKDAYS` variable; logic uses `weekday() < 5` directly

### Shift Segments

Each working day with `shifts_per_day=2` has two segments:
- 06:30-14:30 (Shift 1, 8 hours)
- 14:30-22:30 (Shift 2, 8 hours)
- Zero gap between Shift 1 and Shift 2
- 22:30 to next day 06:30 is non-staffed (overnight gap)

### ShiftSchedule Class (lines 33-103)

Per-date shift resolution with backward compatibility:

```python
ShiftSchedule(default=2, overrides={date(2026, 2, 5): 1}, weekend_default=0)
```

**Resolution priority** in `for_date(d)`:
1. If date is in `overrides`, return that value
2. If date is a weekend, return `weekend_default` (0)
3. Otherwise return `default`

### Key Functions

**`align_to_working_time(cur, spd)`** (lines 179-205):
Snaps any datetime forward to the next staffed moment. Handles:
- Previous day's Shift 3 (cross-midnight)
- Non-working days (advances day-by-day until working day found)
- Before first segment (returns segment start)
- Within segment (returns as-is)
- After all segments (advances to next day)

**`add_staffed_hours(start, hours, spd)`** (lines 306-346):
Core job duration calculator. Walks forward through shift segments, consuming available hours, skipping gaps/weekends. Job durations are in **staffed hours**, not wall-clock.

**`staffed_hours_between(start, end, spd)`** (lines 233-248):
Computes working hours between two datetimes by iterating segments.

**`next_shift_start(t, spd)`** (lines 251-275):
Finds the start of the next shift after time `t`.

**`_shift_key(cursor, spd)`** (lines 219-230):
Returns `(date, shift_number)` tuple. Shift 3 (cross-midnight) is attributed to the evening calendar day.

**`_which_shift(t, spd)`** (lines 208-216):
Returns shift number (1, 2, or 3) based on clock time.

### Time Advancement in the Main Loop

The scheduler is **event-driven**. Time advances by:
1. `_place_job` advances cursor by staffed hours
2. `_insert_changeover` advances by wall-clock hours
3. `_insert_not_running` pushes cursor to a future event
4. `align_to_working_time` snaps past gaps/weekends
5. Machine selection always picks the earliest cursor

### Job Timing Rules

- **Job durations**: Staffed hours via `add_staffed_hours()` (skips overnight/weekends)
- **Changeover durations**: Wall-clock hours via `start + timedelta(hours=co_hours)` (ticks through non-working time)
- **Jobs spanning shift boundaries**: `add_staffed_hours` handles seamlessly by consuming hours from contiguous segments
- **Jobs spanning weekends**: Hours consumed from Friday segments, remaining hours continue Monday

### Weekend Handling

Friday 22:30 → Monday 06:30 transition:
1. `align_to_working_time` called on cursor
2. Saturday/Sunday have no segments (empty)
3. Monday has segments (06:30-14:30, 14:30-22:30)
4. Cursor snaps to Monday 06:30

### Custom Start Time (`--time` CLI)

Without `--time`: scheduler starts at 06:30. With `--time 10:00`: starts at 10:00. If custom time is outside staffed time (e.g., 23:00 with 2 shifts), snaps forward to next shift start.

### No Scheduling Horizon

There is no hard scheduling horizon. The scheduler schedules ALL remaining jobs until the work queue is empty. Safety caps:
- Loop iterations: `jobs * 20 + 2000`
- Day scan in `add_staffed_hours`: 400 days
- Day scan in `align_to_working_time`: 400 iterations
- Next-shift scan: 14 days forward

### Shift Boundary Resets (`_check_shift_boundary`, lines 168-220)

When cursor crosses a shift boundary:
- Reset `headcount_target` and `shift_initial_hc` to `None`
- If day changed: also reset `waiting_for_crew` and `deferred_co_since`
- If job carries over from previous shift: retain crew ownership with that job's HC
- If no carry-over: release crew ownership

---

## 7. Priority Classes

**File:** `src/helpers.py`

### Priority Tier (lines 198-209)

| Tier | Value | Detection |
|------|-------|-----------|
| PRIORITY_PLUS | 0 | Priority field contains "+" |
| PRIORITY | 1 | Priority field contains word boundary `\bP\b` |
| NONE | 2 | Everything else (NaN/blank/other) |

### Job Priority Class (lines 212-220)

| Class | Value | Criteria |
|-------|-------|----------|
| PRIORITY_PLUS | 0 | Priority tier = 0 |
| PRIORITY | 1 | Priority tier = 1 |
| PAST_DUE | 2 | Priority tier = 2 AND due date < schedule start date |
| NORMAL | 3 | Everything else |

### How Priority Affects Selection

- Priority Plus jobs get `_is_pp = 0` (sorts above everything except locked jobs)
- Among non-PP jobs, `prio_class` is a tiebreaker at position 6 in the 14-level sort key
- Priority is secondary to HC matching, locked status, and headcount proximity

---

## 8. Special Rules

### 16C Labeler-Only

- **`_apply_labeler_filter()`** (scheduling_core.py:387): Removes labeler jobs from 16A/16B candidate pools. Only 16C can see labeler jobs.
- **`build_job_eligibility()`** (crew_first_scheduler.py:24): Labeler jobs eligible ONLY on 16C.
- **Validation** (rev8_scheduler.py:419): Hard `ValueError` if any labeler job ends up on non-16C machine.
- Applied in bridge target evaluation and jump target selection too.

### RF Synthetic Tool "99999"

RF jobs with blank/NaN `Tool #` get `"99999"` before preparation (rev8_scheduler.py:343-346). Groups all blank-tool RF jobs together as one "tool" so they batch without triggering changeovers between themselves. RF-specific only.

### Bagger Constraint

Bagger jobs narrowed to `bagger_allowed_machines = ["16A", "16B", "16C"]` (crew_first_scheduler.py:28-31). Non-16 machines can't run bagger jobs.

### Ticket Color Filtering

- **PINK**: Excluded by default (`include_pink=False`)
- **WHITE**: Excluded by default (`include_white=False`)
- **YELLOW**: Included by default, excluded only if `exclude_yellow=True`

### Exclusive Tool Ownership

In `_candidate_pool_for_machine()` (scheduling_core.py:369-382): If ALL jobs for a given tool_id have the same `preferred_machine` != current machine, those tool IDs are excluded. Prevents "tool stealing."

### Startup Batching

Non-16-group machines (8, 6ST, RF) with no current tool and multiple tool groups: pick the tool group with the most total remaining hours to minimize future changeovers.

### Mid-Schedule Max Running Cap

When `max_staffed_override > 2` (rev8_scheduler.py:120-122):
```python
MID_SCHEDULE_MAX_RUNNING_ENABLED = True
MID_SCHEDULE_MAX_RUNNING = 2
MID_SCHEDULE_FRACTION = 0.5
```
After 50% of the schedule has elapsed, max concurrently running machines drops to 2.

### Receiver Hold

Idle machines may be held open (NOT_RUNNING) if they're predicted as bridge targets for an upcoming changeover on another machine (`_future_bridge_receiver_hold_until`, line 1526). Prevents the idle machine from self-starting and consuming jobs before the bridge arrives.

### Dynamic Staffing Cap (`_receiver_pool_staffing_cap`, line 1668)

- Count `single_tool_slots` (machines with 1 remaining tool) and `multi_tool_slots` (machines with multiple tools)
- Cap = `single_tool_slots + max(1, multi_tool_slots // 2)`
- Ensures about half of multi-tool machines stay idle as bridge receivers
- Hard `max_staffed_override` used as floor if set

### Minimum Remaining Shift Time (lines 2571-2593)

If remaining shift time < `min_remaining_shift_hours` (default 1.0h), skip placing a new job UNLESS:
- Job is locked, OR
- Job is longer than the threshold (would span shifts anyway), OR
- Machine has no prior jobs (first-job-of-shift exempt)

---

## 9. Data Flow

### Complete Pipeline

```
Excel Upload
    |
    v
load_input_dataframe()
    |  - Reads sheets with required columns: SO #, EQP Code, Due Date, Tool #
    |  - Multi-sheet discovery or legacy Input_16_Filtered format
    v
Rev8Scheduler.schedule(input_df)
    |
    +---> _prepare_schedule_data()
    |         |
    |         +---> Split by station group (infer_station_group on EQP Code)
    |         |       Groups: 16, 20, 8, lmb, smb, 6st, rf
    |         |
    |         +---> Apply disabled_stations filter
    |         |
    |         +---> RF blank Tool # -> "99999"
    |         |
    |         +---> _prepare_group() per non-empty group:
    |         |       - Filter by ticket color (PINK/WHITE out, YELLOW conditional)
    |         |       - Parse due dates via due_deadline_from_cell()
    |         |       - Parse labeler/bag flags via parse_boolish()
    |         |       - Drop blank Tool # rows (-> skipped)
    |         |       - Normalize tool IDs via normalize_tool()
    |         |       - Compute run_hours and headcount
    |         |       - Drop invalid run hours (-> skipped)
    |         |       - Compute priority class (PP/P/PD/Normal)
    |         |       - Detect picked and locked (in-progress) jobs
    |         |
    |         +---> _apply_machine_eligibility() per group:
    |         |       - build_job_eligibility() (labeler->16C, bag->16A/B/C)
    |         |       - Drop ineligible jobs (-> skipped)
    |         |
    |         +---> Machine assignment:
    |         |       - 16-group: assign_jobs_to_machines_crew_first()
    |         |       - Others: assign_tools_to_machines_simple()
    |         |
    |         +---> Pin In-Progress jobs to specific machines
    |         +---> Concatenate all groups into df_combined
    |         +---> Return _build_initial_states() factory
    |
    +---> (Optional) Mid-schedule cap preview run
    |
    +---> schedule_all_machines(df_combined, states, cfg, tool_reasons)
    |       - Pre-place locked jobs
    |       - Global tick loop (event-driven)
    |       - Returns jump_log
    |
    +---> _finalize_schedule_variant(states, jump_log, ...)
    |       - Post-processing pipeline
    |       - Returns (machine_states, summary_rows, score)
    |
    +---> Build output DataFrames
    |       - Per-machine schedule (schedule_to_df)
    |       - Summary, Utilization, Staffing
    |       - Skipped_Jobs, Yellow_Jobs
    |
    v
save_schedule_workbook()
    - Write all sheets to .xlsx via openpyxl
    - apply_excel_formatting() (yellow highlight for assumed HC)
```

### Assignment Strategies

**16-group** -- `assign_jobs_to_machines_crew_first()` (crew_first_scheduler.py:38-132):
- Jobs sorted by: eligible_count ASC, due_sort ASC, run_hours DESC, so_sort ASC
- Per-job scoring of each eligible machine: same-tool bonus, changeover penalty, projected hours, current load, EQP hint penalty
- Minimum score wins

**Single-machine groups** -- `assign_tools_to_machines_simple()` (tool_assignment.py:14-98):
- Phase 1: Labeler tools to labeler machine
- Phase 2: Load-balanced round-robin (largest tools first)
- Phase 3: `_consolidate_small_tools()` -- iteratively move small tools to reduce changeover count while keeping load imbalance < 40%

---

## 10. Configuration Parameters

**File:** `src/scheduler_io.py`, `SchedulerConfig` dataclass (lines 23-83)

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `schedule_start` | `datetime` | required | Reference start time for scheduling |
| `changeover_hours` | `float` | `2.0` | Global default changeover duration (per-machine overrides exist) |
| `shifts_per_day_16a` | `int` | `2` | Shifts for 16S-A |
| `shifts_per_day_16b` | `int` | `2` | Shifts for 16S-B |
| `shifts_per_day_16c` | `int` | `2` | Shifts for 16S-C |
| `shifts_per_day_20` | `int` | `2` | Shifts for 20S |
| `shifts_per_day_8` | `int` | `2` | Shifts for 8S |
| `shifts_per_day_lmb` | `int` | `2` | Shifts for LMB |
| `shifts_per_day_smb` | `int` | `2` | Shifts for SMB |
| `shifts_per_day_6st` | `int` | `2` | Shifts for 6ST |
| `shifts_per_day_rf` | `int` | `2` | Shifts for RF |
| `exclude_yellow` | `bool` | `False` | Exclude YELLOW ticket-color jobs |
| `include_pink` | `bool` | `False` | Include PINK ticket-color jobs |
| `include_white` | `bool` | `False` | Include WHITE ticket-color jobs |
| `disabled_stations` | `List[str]` | `[]` | Station groups to skip entirely |
| `hc_flex` | `int` | `2` | HC matching tolerance (+/- N) |
| `crew_transition_flex` | `int` | `1` | Crew bridging tolerance (+/- N) |
| `default_headcount` | `float` | `11.0` | Fallback HC when input missing |
| `max_staffed_override` | `Optional[int]` | `None` | Hard cap on concurrent running machines |
| `min_remaining_shift_hours` | `float` | `1.0` | Min shift time to place a new job |
| `h3_enabled` | `bool` | `True` | Max 1 simultaneous maintenance changeover |
| `shift_schedule_overrides` | `Dict[str, Dict[str, int]]` | `{}` | Per-machine, per-day shift overrides |
| `initial_tools` | `Dict[str, str]` | `{}` | Pre-loaded tool per machine |

### Hardcoded Constants

| Constant | Value | File | Purpose |
|----------|-------|------|---------|
| `HC_FLEX` | `2` | scheduling_core.py:46 | Default HC tolerance |
| `CREW_TRANSITION_FLEX` | `1` | scheduling_core.py:47 | Default crew bridge tolerance |
| `TOOL_CHANGEOVER_HOURS` | `2.0` | models.py:11 | Global changeover default |
| `DEFAULT_AVG_NUM_EMPLOYEES` | `11.0` | helpers.py:17 | Default headcount |
| `MID_SCHEDULE_MAX_RUNNING` | `2` | rev8_scheduler.py:121 | Mid-schedule cap |
| `MID_SCHEDULE_FRACTION` | `0.5` | rev8_scheduler.py:122 | When mid-schedule cap kicks in |
| `_DONE` | `datetime(9999,1,1)` | calendar_utils.py:27 | Machine finished sentinel |
| Crew jump limit | `2` | scheduling_core.py:1406 | Max jumps per shift |
| IDLE_CREW min duration | `600s` (10 min) | scheduling_core.py:1757 | Drop sub-10-min IDLE_CREW |
| NOT_RUNNING min gap | `0.08h` (~5 min) | scheduling_core.py:2250 | Skip tiny NOT_RUNNING gaps |

### Parameters NOT in SchedulerConfig (mentioned in CLAUDE.md but not implemented)

- `bridge_sustained_weight` -- not present anywhere
- `consolidation_threshold` -- parameter on `assign_tools_to_machines_simple()` only (default 0.40)
- `max_relay_hops` -- not present anywhere
- `initial_machines` -- not present (dormant activation inactive)
- `early_threshold_days` -- not present anywhere
- `utilization_target` -- not present anywhere
- `n_minus_y` -- not present anywhere

---

## 11. MachineState Fields

**File:** `src/scheduling_core.py` (lines 52-81)

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `machine_id` | `str` | required | Machine identifier |
| `shifts_per_day` | `int` | required | Number of shifts (1, 2, or 3) |
| `cursor` | `datetime` | required | Current scheduling time pointer |
| `current_tool` | `Optional[str]` | `None` | Tool currently loaded |
| `headcount_target` | `Optional[float]` | `None` | HC target for current shift |
| `headcount_shift_key` | `Optional[Tuple]` | `None` | Shift key for HC target |
| `crew_day_key` | `Optional[date]` | `None` | Crew day key (06:30 anchored) |
| `shift_initial_hc` | `Optional[float]` | `None` | HC of first job in current shift |
| `changeover_end` | `Optional[datetime]` | `None` | When last changeover finishes |
| `schedule` | `List[Dict]` | `[]` | Ordered schedule entries |
| `scheduled_ids` | `Set` | `set()` | SO#s already scheduled on this machine |
| `headcount_transitions` | `List[Tuple]` | `[]` | HC change records: `(time, old, new)` |
| `displaced_crew` | `Optional[float]` | `None` | Workers freed during changeover |
| `crew_source` | `Optional[str]` | `None` | Which machine sent crew here |
| `waiting_for_crew` | `bool` | `False` | Machine idle awaiting crew |
| `deferred_co_since` | `Optional[datetime]` | `None` | When changeover deferral started |
| `dormant` | `bool` | `False` | Staggered activation flag |
| `activated_at` | `Optional[datetime]` | `None` | When machine was activated |
| `activated_by` | `Optional[str]` | `None` | Activating machine ID |
| `owns_crew` | `bool` | `False` | Owns crew for current shift |
| `owned_headcount` | `Optional[float]` | `None` | Owned worker count |
| `crew_ownership_shift_key` | `Optional[Tuple]` | `None` | Shift key of crew ownership |
| `crew_owner_source` | `str` | `""` | Source of crew donation |
| `crew_owner_carried` | `bool` | `False` | Crew carried from prior shift |
| `crew_jump_count` | `int` | `0` | Crew jumps this shift (max 2) |
| `lost_crew_this_shift` | `bool` | `False` | Crew was lost/released this shift |

---

## 12. CrewLedger Internals

**File:** `src/scheduling_core.py` (lines 87-157)

### Workforce Computation

1. **Initial seeding** (line 2346): `total_workforce` = sum of each machine's peak HC job (legacy, unused in decisions)
2. **Realized workforce seeding** (line 2422): For staffed machines: actual HC. For others: peak HC from pool. `realized_workforce = max(current, sum)`
3. **Runtime ratcheting** (line 133-134): When `own_machine()` causes total owned > realized, realized ratchets up
4. **Never decreases**: `realized_workforce` is monotonically increasing

### Shift Boundary Sync (line 2458-2464)

When any machine crosses a shift boundary, ALL machines have their ownership synced:
- Done machines release ownership
- Machines with current shift ownership re-assert
- Machines without ownership release

---

## 13. Post-Processing Pipeline

**File:** `src/scheduling_core.py`, `_finalize_schedule_variant()` (line 1739)

After `schedule_all_machines()` returns:

1. **`_fill_break_gaps`** -- Close short inter-shift gaps in schedule entries
2. **`_insert_idle_crew_entries`** -- Mark idle crew periods (currently no-op)
3. **`_insert_deferral_idle_crew`** -- Fill schedule gaps with IDLE_CREW where crew was genuinely idle
4. **Drop sub-10-minute IDLE_CREW entries**
5. **`_backfill_not_running_gaps`** -- Fill remaining timeline gaps with NOT_RUNNING blocks (NO_CREW type)
6. **`compute_idle_worker_hours`** -- Tally idle worker-hours from IDLE_CREW and CHANGEOVER entries
7. **`_split_at_shift_boundaries`** -- Split JOB/TOOL_SWAP entries spanning non-working periods into multiple segments

### `_split_at_shift_boundaries` Details (lines 2011-2079)

- Splits JOB and TOOL_SWAP entries that span overnight gaps or weekends
- Short gaps (< 1 hour) are NOT split (prevents unnecessary splitting at Shift 1/2 boundary)
- Continuation segments marked with `_continuation=True`
- Primary segment stores `_completion_end` for late/on-time determination

### `_backfill_not_running_gaps` Details (lines 2214-2278)

- Finds gaps between consecutive schedule entries
- Fills each with a NOT_RUNNING block (Idle Type = "NO_CREW")
- Gaps < 0.08 staffed hours (~5 min) are skipped
- Cross-day gaps filled only to current day's end

---

## 14. API Endpoints

**File:** `src/api.py`

### Pages

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | Main scheduling UI (`frontend/index.html`) |
| GET | `/scenarios` | Scenario comparison page |
| GET | `/results` | Results/Gantt chart page |

### API

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/validate` | Validate uploaded Excel file |
| POST | `/api/schedule` | Generate schedule from uploaded file |
| GET | `/api/schedule/{id}/stats` | Get stats for generated schedule |
| GET | `/api/download/{id}` | Download generated .xlsx file |
| GET | `/api/yellow-jobs/{id}/csv` | Download yellow jobs as CSV |
| POST | `/api/scenarios` | Launch background scenario sweep |
| GET | `/api/scenarios/{id}/progress` | Poll scenario progress |
| POST | `/api/scenarios/{id}/select/{rank}` | Materialize selected scenario |
| GET | `/api/preview/{id}` | JSON representation of all output sheets |
| GET | `/api/health` | Health check |

### `/api/schedule` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schedule_file` | UploadFile | required | Excel input file |
| `reference_date` | str | today | YYYY-MM-DD |
| `reference_time` | str | 06:30 | HH:MM |
| `shifts_16s_a` through `shifts_rf` | int | 2 | Per-machine shifts |
| `exclude_yellow` | bool | False | Exclude YELLOW tickets |
| `include_pink`, `include_white` | bool | False | Include PINK/WHITE tickets |
| `disabled_stations` | str | "" | Comma-separated groups |
| `hc_flex` | int | 2 | HC tolerance |
| `crew_transition_flex` | int | 1 | Crew bridge tolerance |
| `default_headcount` | float | 11.0 | Fallback HC |
| `max_staffed` | int | None | Machine cap |
| `shift_schedule` | str | None | JSON per-day overrides |
| `initial_tools` | str | None | JSON initial tool per machine |

---

## 15. Input/Output Format

### Required Input Columns

- `SO #` -- Sales Order number
- `EQP Code` -- Equipment code (determines machine group)
- `Due Date` -- Job due date
- `Tool #` -- Tool identifier

### Optional Input Columns

- `Ticket Color` -- Priority color filter
- `Labeler` / `Bag Sealer` -- Boolean flags
- `Priority Status` -- Text priority
- `Picked` / `In progress` -- Job status flags
- `Run Hrs` -- Pre-computed run hours
- `Remaining QTY`, `person hour rate`, `avg_num_employees` -- For run hours computation
- `SOL ID`, `Finished Item`, `Description`, `Customer`, `Total QTY`, `Produced QTY`, `order entry date`, `Part Number` -- Pass-through detail columns

### Output Sheets

1. **`Input_All`** -- Complete input DataFrame
2. **`Input_<group>_Filtered`** -- Per-station-group filtered input
3. **Per-machine schedule sheets** (16S-A, 16S-B, 16S-C, 20S, 8S, LMB, SMB, 6ST, RF)
4. **`Summary`** -- Aggregate stats per machine + TOTAL row
5. **`Utilization`** -- Daily utilization breakdown with WEEK TOTAL rows
6. **`Staffing`** -- Daily per-shift headcount (15-min sampling for peak)
7. **`Skipped_Jobs`** -- Jobs dropped during preparation
8. **`Yellow_Jobs`** -- Scheduled YELLOW ticket jobs

### Schedule Entry Types

- `JOB` -- Scheduled production job
- `CHANGEOVER` -- Maintenance tool changeover (2h, crew freed)
- `TOOL_SWAP` -- Self-service tool swap (0.25h, crew stays)
- `NOT_RUNNING` -- Machine idle (NO_CREW)
- `IDLE_CREW` -- Crew present but idle (CREW_WAITING or UNBALANCED_CREW)

### Per-Machine Schedule Columns

```
Seq, Type, Machine, Start, End, SO #,
SOL ID, Finished Item, Description, Customer, Total QTY, Produced QTY,
Remaining QTY, EQP Code, Ticket Color, order entry date, Tool #,
avg_num_employees, person hour rate, headcount_assumed,
Tool ID, Run Hrs, Due Date, Priority Class,
Labeler, Bag Sealer, Headcount, Headcount Target,
Locked, Idle Type, Crew From, Crew To,
Late, Days Early/Late, Shift, Reason
```

### Summary Sheet Columns

```
Machine, Jobs, Job Hours, Changeover Hours, Total Scheduled Hours,
On-Time Jobs, Late Jobs, Late Job Hours,
Past-Due at Ref, Past-Due Hours, Utilization %, CO Overhead %,
Headcount Transitions, Crew Jumps In, Crew Jumps Out,
Unbalanced Crew Hours, Unbalanced Crew Worker-Hours,
Shifts/Day, Daily Capacity, Days to Complete, Schedule End Date
```

---

## 16. Helper Utilities

**File:** `src/helpers.py`

### EQP-to-Machine Inference

**`infer_station_group(eqp_val)`** (line 104):
- `16ST` or `16S-` --> `"16"`
- `20S` or `20ST` --> `"20"`
- `8ST` --> `"8"` (checked before `6ST`)
- `6ST` --> `"6st"`
- `\bLMB\b` --> `"lmb"`
- `\bSMB\b` --> `"smb"`
- `\bRF\b` --> `"rf"`

**`infer_machine_from_eqp(eqp_val)`** (line 127):
- `16S-A` / `16A` --> `"16A"`, `-B` --> `"16B"`, `-C` --> `"16C"`, generic --> `None`
- `20S` --> `"20"`, `8ST` --> `"8"`, `6ST` --> `"6ST"`, `LMB` --> `"LMB"`, `SMB` --> `"SMB"`, `RF` --> `"RF"`

### Tool Normalization (`normalize_tool`, line 41)

Priority: (1) find `QPI\d+` token, return with spaces removed; (2) fall back to last numeric token; (3) raise ValueError. Strips `.0` suffixes.

### Run Hours Computation (`compute_run_hours`, line 235)

- If `Run Hrs` column exists: use directly
- Otherwise: `run_hours = Remaining QTY / (person hour rate * avg_num_employees)`
- Missing `avg_num_employees` falls back to `default_hc` (11.0) and marks `headcount_assumed = True`

### Due Date Parsing (`due_deadline_from_cell`, line 184)

- NaN/blank returns `None`
- Midnight timestamps (00:00:00) get replaced with 23:59:59 (end of day)
- Otherwise exact datetime used

### Headcount Resolution (`resolve_headcount`, line 277)

Returns effective headcount per row. Falls back to `default_hc` when `avg_num_employees` is missing or non-positive.

---

## Appendix: All Helper Functions Called from `schedule_all_machines()`

| Function | Line | Purpose |
|----------|------|---------|
| `_candidate_pool_for_machine` | 351 | Filter jobs eligible for a machine |
| `_presort_jobs_by_tool_batching` | 2283 | Pre-sort by tool batch size |
| `_effective_max_staffed_override_at` | 1703 | Resolve machine cap at time |
| `_insert_changeover` | 936 | Insert CHANGEOVER/TOOL_SWAP entry |
| `_place_job` | 996 | Place JOB entry, advance cursor |
| `_set_shift_crew_owner` | 299 | Claim crew ownership |
| `_shift_key` | cal:219 | (date, shift_number) tuple |
| `align_to_working_time` | cal:179 | Snap to next staffed moment |
| `_next_event_rank` | 1610 | Rank machine's next action quality |
| `_check_shift_boundary` | 168 | Reset shift/day crew state |
| `_sync_shift_crew_ownership_at` | 251 | Sync ownership to specific time |
| `_machine_has_current_shift_ownership` | 290 | Check current shift ownership |
| `_receiver_pool_staffing_cap` | 1668 | Dynamic machine-concurrency cap |
| `next_shift_start` | cal:251 | Next shift start datetime |
| `_insert_not_running` | 1100 | Mark machine waiting, advance cursor |
| `_future_bridge_receiver_hold_until` | 1526 | Check receiver hold |
| `_select_job_v8` | 793 | Full job selection with priority sort |
| `CrewLedger.find_earliest_crew_free` | 144 | Earliest staffed machine finish |
| `_shift_end_for_time` | 1213 | End of current shift |
| `_get_machine_spec_cached` | helpers:80 | Look up MachineSpec |
| `_current_displaced_headcount` | 394 | HC freed when machine stops |
| `_has_same_shift_continuation_path` | 1224 | Enough work after changeover? |
| `_find_active_changeover_end` | 1129 | H3 check: active changeover? |
| `_bridge_crew_after_changeover` | 1387 | Transfer crew during changeover |
| `_has_immediate_shift_sticky_jump_target` | 620 | Immediate bridge available? |
| `_next_deferred_changeover_bridge_time` | 648 | Future bridge time in crew day |
| `_next_bridge_target_time` | 586 | Earliest bridge availability |
| `add_staffed_hours` | cal:306 | Add staffed hours, walk segments |
| `_derive_crew_from` | 1111 | Determine crew provenance |
| `_release_shift_crew_owner` | 319 | Release crew ownership |
| `CrewLedger.staff_machine` | 139 | Record staffed machine |
