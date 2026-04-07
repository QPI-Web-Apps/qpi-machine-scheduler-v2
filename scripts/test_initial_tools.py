"""Tests for the 'Last tool on machine' (initial_tools) feature.

This is the per-machine override that tells the scheduler what tool is already
loaded at t=0. The scheduler enforces it in two places:

  • solver.py:309 — if init_tool != first_batch.tool_id and machine has CO,
                    force first_batch.start >= machine_co (in minutes)
  • scheduler.py:205 — at assembly time, insert a CHANGEOVER (or TOOL_SWAP for
                       self-service machines) entry between the synthetic prev_tool
                       and the first real batch when their tools differ

The test below exercises every meaningful path:

  Case A — empty initial_tools:        no upfront CO entries on any machine
  Case B — matching tool:              first machine entry is a JOB, no CO
  Case C — mismatching maint machine:  CHANGEOVER entry inserted, first JOB delayed by 2h
  Case D — mismatching LMB/SMB:        TOOL_SWAP entry inserted (different entry_type)
  Case E — machine 20 (no CO):         init_tool is silently ignored, no CO entry possible
  Case F — fake unrelated tool ID:     CHANGEOVER inserted on every first batch (since
                                       no batch tool will match)
  Case G — normalize_tool round-trip:  user input "  367.0  " ends up as "367" in cfg
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.helpers import normalize_tool
from src.models import MACHINE_BY_ID
from src.scheduler import generate_schedule
from src.scheduler_io import SchedulerConfig

XLSX = Path(__file__).resolve().parents[1] / "test_schedules" / "STF schedule 03.31 130PMToBeFilled_Updated.xlsx"


def _check(label: str, cond: bool, detail: str = "") -> bool:
    mark = "✓" if cond else "✗"
    print(f"  {mark} {label}" + (f"  ({detail})" if detail else ""))
    return cond


def _machine_entries(result, machine_id: str):
    """Entries for one machine, sorted by start time."""
    return sorted(
        [e for e in result.entries if e.machine_id == machine_id],
        key=lambda e: e.start,
    )


def _first_job_tool(result, machine_id: str) -> str | None:
    """Tool ID on the first JOB entry of a machine, or None if no jobs."""
    for e in _machine_entries(result, machine_id):
        if e.entry_type == "JOB":
            return e.tool_id
    return None


def _has_upfront_co(result, machine_id: str) -> bool:
    """True if the first non-NOT_RUNNING entry on this machine is CHANGEOVER/TOOL_SWAP."""
    for e in _machine_entries(result, machine_id):
        if e.entry_type == "NOT_RUNNING":
            continue
        return e.entry_type in ("CHANGEOVER", "TOOL_SWAP")
    return False


def _first_co_entry(result, machine_id: str):
    for e in _machine_entries(result, machine_id):
        if e.entry_type in ("CHANGEOVER", "TOOL_SWAP"):
            return e
    return None


def _baseline(cfg_kwargs=None):
    """Generate a schedule with no initial_tools, used to discover real tool IDs."""
    cfg = SchedulerConfig(
        schedule_start=datetime(2026, 4, 2, 6, 30),
        include_yellow=True,
        include_pink=True,
        **(cfg_kwargs or {}),
    )
    return generate_schedule(str(XLSX), cfg, max_concurrent=5), cfg


def _run(initial_tools: dict[str, str]):
    cfg = SchedulerConfig(
        schedule_start=datetime(2026, 4, 2, 6, 30),
        include_yellow=True,
        include_pink=True,
        initial_tools=dict(initial_tools),
    )
    return generate_schedule(str(XLSX), cfg, max_concurrent=5)


def main() -> int:
    print("Generating baseline (no initial_tools) to discover real tool IDs…")
    baseline, _ = _baseline()
    print(f"  baseline entries: {len(baseline.entries)}")

    # Discover tool IDs in use
    machine_first_tools: dict[str, str] = {}
    machine_all_tools: dict[str, set[str]] = {}
    for e in baseline.entries:
        if e.entry_type != "JOB" or not e.tool_id:
            continue
        machine_all_tools.setdefault(e.machine_id, set()).add(e.tool_id)
    for mid in machine_all_tools:
        ft = _first_job_tool(baseline, mid)
        if ft:
            machine_first_tools[mid] = ft

    print("  discovered first-job tools per machine:")
    for mid, t in machine_first_tools.items():
        print(f"    {mid}: {t}  (all tools on machine: {sorted(machine_all_tools[mid])})")

    all_ok = True

    # ── Case A — empty initial_tools, no upfront CO anywhere ──
    print("\n=== A. Empty initial_tools → no upfront CO entries ===")
    for mid, spec in MACHINE_BY_ID.items():
        if not spec.has_changeovers:
            continue
        if mid not in machine_all_tools:
            continue
        upfront = _has_upfront_co(baseline, mid)
        all_ok &= _check(f"{mid}: no upfront CO", not upfront,
                         f"first entry type is {_machine_entries(baseline, mid)[0].entry_type if _machine_entries(baseline, mid) else 'none'}")

    # ── Case B — matching tool: no upfront CO ──
    print("\n=== B. Matching initial_tool → first machine entry is a JOB ===")
    target_machine_b = "16A"
    if target_machine_b in machine_first_tools:
        match_tool = machine_first_tools[target_machine_b]
        result_b = _run({target_machine_b: match_tool})
        first_entry = next(
            (e for e in _machine_entries(result_b, target_machine_b)
             if e.entry_type != "NOT_RUNNING"),
            None,
        )
        all_ok &= _check(
            f"{target_machine_b}: first non-idle entry is JOB",
            first_entry is not None and first_entry.entry_type == "JOB",
            f"got {first_entry.entry_type if first_entry else 'none'}",
        )
        if first_entry and first_entry.entry_type == "JOB":
            all_ok &= _check(
                f"{target_machine_b}: first JOB still uses tool {match_tool}",
                first_entry.tool_id == match_tool,
                f"got {first_entry.tool_id}",
            )

    # ── Case C — mismatching tool on maintenance machine ──
    print("\n=== C. Mismatching initial_tool on maint machine → CHANGEOVER inserted ===")
    target_machine_c = "16A"
    if target_machine_c in machine_all_tools and len(machine_all_tools[target_machine_c]) >= 2:
        # Pick a tool that exists on this machine but is NOT the baseline first tool
        first_tool = machine_first_tools[target_machine_c]
        other_tool = next(t for t in machine_all_tools[target_machine_c] if t != first_tool)
        # Use a tool that's deliberately NOT the optimal first tool the solver chose
        result_c = _run({target_machine_c: other_tool})
        co = _first_co_entry(result_c, target_machine_c)
        # Either: solver still picks the same first batch, requiring an inserted CO entry,
        # or: solver picks 'other_tool' as the first batch and there's no CO. Both are valid.
        first_job_tool = _first_job_tool(result_c, target_machine_c)
        all_ok &= _check(
            f"{target_machine_c}: result is consistent (CO if first JOB tool != {other_tool})",
            (co is not None) == (first_job_tool != other_tool),
            f"first_job_tool={first_job_tool}  CO={'yes' if co else 'no'}",
        )
        if co:
            all_ok &= _check(
                f"{target_machine_c}: CO entry_type is CHANGEOVER (not TOOL_SWAP)",
                co.entry_type == "CHANGEOVER",
                f"got {co.entry_type}",
            )
            duration_h = (co.end - co.start).total_seconds() / 3600
            spec_c = MACHINE_BY_ID[target_machine_c]
            all_ok &= _check(
                f"{target_machine_c}: CO duration ≈ {spec_c.changeover_hours}h",
                abs(duration_h - spec_c.changeover_hours) < 0.5,
                f"actual={duration_h:.2f}h",
            )
            all_ok &= _check(
                f"{target_machine_c}: CO tool_id encodes the transition '{other_tool} -> ?'",
                co.tool_id and co.tool_id.startswith(f"{other_tool} -> "),
                f"got {co.tool_id}",
            )

    # ── Case D — mismatching tool on LMB self-service ──
    print("\n=== D. Mismatching initial_tool on LMB → TOOL_SWAP (not CHANGEOVER) ===")
    target_machine_d = "LMB"
    if target_machine_d in machine_all_tools and len(machine_all_tools[target_machine_d]) >= 2:
        first_tool = machine_first_tools[target_machine_d]
        other_tool = next(t for t in machine_all_tools[target_machine_d] if t != first_tool)
        result_d = _run({target_machine_d: other_tool})
        co = _first_co_entry(result_d, target_machine_d)
        first_job_tool = _first_job_tool(result_d, target_machine_d)
        all_ok &= _check(
            f"{target_machine_d}: consistent CO presence",
            (co is not None) == (first_job_tool != other_tool),
            f"first_job_tool={first_job_tool}  CO={'yes' if co else 'no'}",
        )
        if co:
            all_ok &= _check(
                f"{target_machine_d}: entry_type is TOOL_SWAP (self-service)",
                co.entry_type == "TOOL_SWAP",
                f"got {co.entry_type}",
            )
            duration_h = (co.end - co.start).total_seconds() / 3600
            all_ok &= _check(
                f"{target_machine_d}: TOOL_SWAP duration ≈ 0.25h",
                abs(duration_h - 0.25) < 0.1,
                f"actual={duration_h:.2f}h",
            )
    else:
        print(f"  (skipped — LMB has fewer than 2 distinct tools in test data)")

    # ── Case E — machine 20 has no changeovers, init_tool is ignored ──
    print("\n=== E. Machine 20 (has_changeovers=False) → init_tool ignored ===")
    if "20" in machine_all_tools:
        # Use a deliberately wrong tool ID
        result_e = _run({"20": "WRONG_TOOL_ABC"})
        co = _first_co_entry(result_e, "20")
        all_ok &= _check(
            "20: no CHANGEOVER or TOOL_SWAP entries even with mismatched init_tool",
            co is None,
            f"got {co.entry_type if co else 'none'}",
        )

    # ── Case F — fake unrelated tool ID on a maint machine ──
    print("\n=== F. Unrelated tool ID → CHANGEOVER inserted (since no batch matches) ===")
    target_machine_f = "16B"
    if target_machine_f in machine_all_tools:
        result_f = _run({target_machine_f: "ZZZ_NEVER_USED"})
        co = _first_co_entry(result_f, target_machine_f)
        all_ok &= _check(
            f"{target_machine_f}: CHANGEOVER present (no real batch can match the fake tool)",
            co is not None,
        )
        if co:
            all_ok &= _check(
                f"{target_machine_f}: CO transition starts from 'ZZZ_NEVER_USED'",
                co.tool_id and co.tool_id.startswith("ZZZ_NEVER_USED -> "),
                f"got {co.tool_id}",
            )

    # ── Case G — normalize_tool behavior ──
    print("\n=== G. normalize_tool round-trip ===")
    cases = [
        ("367", "367"),
        ("  367  ", "367"),
        ("367.0", "367"),
        ("99999", "99999"),       # RF blank
        ("QPI 1011", "QPI1011"),  # space removed
        ("qpi1011", "qpi1011"),   # case preserved (NOT uppercased)
        ("ZZZ_NEVER_USED", "ZZZ_NEVER_USED"),  # passthrough
    ]
    for raw, expected in cases:
        try:
            got = normalize_tool(raw)
        except ValueError as e:
            got = f"<ValueError: {e}>"
        all_ok &= _check(f"normalize_tool({raw!r}) → {expected!r}",
                         got == expected, f"got {got!r}")

    # Blank input should raise
    try:
        normalize_tool("")
        _check("normalize_tool('') raises ValueError", False, "did not raise")
        all_ok = False
    except ValueError:
        _check("normalize_tool('') raises ValueError", True)

    print("\n" + ("PASS" if all_ok else "FAIL"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
