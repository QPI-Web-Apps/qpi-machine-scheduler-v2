# QPI Machine Scheduler V2

## Project Overview

Production job scheduler for a manufacturing facility with 9 machines. Takes an Excel file of jobs and produces an optimized schedule that maximizes shift utilization and minimizes crew idle time.

## Architecture

- **`src/models.py`** — Machine registry (9 machines), MachineSpec dataclass
- **`src/calendar_utils.py`** — Shift math (06:30-14:30, 14:30-22:30), staffed hours arithmetic, weekend handling
- **`src/helpers.py`** — EQP-to-machine routing, tool normalization, run hours computation, priority parsing
- **`src/scheduler_io.py`** — Excel loading, job preparation, SchedulerConfig, ticket color filtering

## Key Rules & Constraints

- **Ticket colors**: Only green tickets are included by default. Yellow/pink/white require explicit opt-in.
- **Shifts**: 2 shifts/day (06:30-14:30, 14:30-22:30), Mon-Fri. Configurable per machine.
- **Changeovers**: 2h for maintenance machines (16A/B/C, 8, 6ST, RF), 0.25h self-service for LMB/SMB, none for machine 20.
- **Crew management**: Crew is shift-sticky. When a changeover frees crew, they jump to another machine and STAY there. Max 2 jumps per shift. At shift boundaries, crews are reassigned fresh.
- **Labeler jobs**: Only run on 16C.
- **RF blank tools**: Get synthetic tool ID "99999".
- **Goal**: Arrange jobs and changeovers so crews can always finish out a shift after jumping. Minimize idle time.

## Scheduling Approach

Planning to use Google OR-Tools CP-SAT solver for optimal job assignment, tool ordering, and changeover timing. A tick loop then executes the plan with crew bridging.

## Reference

`scheduler_logic_reference.md` contains exhaustive rules extracted from the prior Rev8 engine. Use it as a rules bible — the logic and constraints matter, not the code architecture.

## Test Data

`test_schedules/STF schedule 03.31 130PMToBeFilled_Updated.xlsx` — 204 rows, 127 green-ticket jobs, ~341 hours of work across 7 station groups.
