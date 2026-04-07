"""Drive the live API with several initial_tools scenarios and report the result.

Talks to the running uvicorn server (http://127.0.0.1:8765/) — does not import
any scheduler internals, so what we see here is exactly what the user sees.
"""
from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

import urllib.request

XLSX = Path(__file__).resolve().parents[1] / "test_schedules" / "STF schedule 03.31 130PMToBeFilled_Updated.xlsx"
BASE = "http://127.0.0.1:8765"


def _post_schedule(initial_tools: dict) -> dict:
    """Multipart upload to /api/schedule with the given initial_tools."""
    boundary = "----qpiboundary12345"
    body = []

    def add_field(name: str, value: str):
        body.append(f"--{boundary}\r\n".encode())
        body.append(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        body.append(f"{value}\r\n".encode())

    def add_file(name: str, filename: str, content: bytes):
        body.append(f"--{boundary}\r\n".encode())
        body.append(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
            f"Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n".encode()
        )
        body.append(content)
        body.append(b"\r\n")

    add_field("reference_date", "2026-04-02")
    add_field("reference_time", "06:30")
    add_field("max_concurrent", "5")
    add_field("include_yellow", "true")
    add_field("include_pink", "true")
    add_field("shift_config", "{}")
    add_field("initial_tools", json.dumps(initial_tools))
    add_file("schedule_file", XLSX.name, XLSX.read_bytes())
    body.append(f"--{boundary}--\r\n".encode())
    payload = b"".join(body)

    req = urllib.request.Request(
        f"{BASE}/api/schedule",
        data=payload,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.loads(r.read().decode())


def _summarize_machine(data: dict, machine_id: str) -> dict:
    """Pull the relevant first-entries info for one machine."""
    entries = sorted(
        [e for e in data["schedule"] if e["machine_id"] == machine_id],
        key=lambda e: e["start"],
    )
    # Skip leading NOT_RUNNING idle gap
    real = [e for e in entries if e["type"] != "NOT_RUNNING"]
    first_real = real[0] if real else None
    first_co = next((e for e in real if e["type"] in ("CHANGEOVER", "TOOL_SWAP")), None)
    first_job = next((e for e in real if e["type"] == "JOB"), None)
    return {
        "first_real_type": first_real["type"] if first_real else None,
        "first_real_tool": first_real.get("tool_id") if first_real else None,
        "first_real_start": first_real["start"][11:16] if first_real else None,
        "first_co": first_co,
        "first_job_tool": first_job.get("tool_id") if first_job else None,
        "first_job_start": first_job["start"][11:16] if first_job else None,
    }


def _scenario(label: str, initial_tools: dict, machines_to_inspect: list[str]):
    print("\n" + "=" * 78)
    print(f"SCENARIO: {label}")
    print(f"  initial_tools = {initial_tools}")
    print("=" * 78)
    data = _post_schedule(initial_tools)
    print(f"  status={data['solver_status']}  total_jobs={data['total_jobs']}  "
          f"total_entries={data['total_entries']}  makespan={data['makespan_hours']}h")

    print(f"\n  {'machine':<6} | {'first non-idle':<18} | {'tool':<25} | {'start':<6} | notes")
    print(f"  {'-'*6}-+-{'-'*18}-+-{'-'*25}-+-{'-'*6}-+-{'-'*40}")
    for m in machines_to_inspect:
        s = _summarize_machine(data, m)
        notes = ""
        if s["first_real_type"] in ("CHANGEOVER", "TOOL_SWAP") and s["first_co"]:
            co = s["first_co"]
            dur = (
                _hours_between(co["start"], co["end"])
            )
            notes = f"{co['type']} {dur:.2f}h: {co.get('tool_id','?')}, then JOB tool={s['first_job_tool']} at {s['first_job_start']}"
        elif s["first_real_type"] == "JOB":
            notes = f"runs immediately"
        else:
            notes = "(no entries)"
        print(f"  {m:<6} | {s['first_real_type'] or '-':<18} | {(s['first_real_tool'] or '-'):<25} | {s['first_real_start'] or '-':<6} | {notes}")
    return data


def _hours_between(a_iso: str, b_iso: str) -> float:
    from datetime import datetime
    a = datetime.fromisoformat(a_iso.replace("Z", "+00:00"))
    b = datetime.fromisoformat(b_iso.replace("Z", "+00:00"))
    return (b - a).total_seconds() / 3600


def main() -> int:
    print("Discovering machine tools from a baseline run…")
    base = _post_schedule({})

    by_machine: dict[str, list[dict]] = {}
    for e in base["schedule"]:
        if e["type"] == "JOB":
            by_machine.setdefault(e["machine_id"], []).append(e)
    discovered = {}
    for mid, jobs in by_machine.items():
        jobs.sort(key=lambda j: j["start"])
        discovered[mid] = {
            "first": jobs[0]["tool_id"],
            "all": sorted({j["tool_id"] for j in jobs if j["tool_id"]}),
        }
    print("  baseline first-tool by machine:")
    for mid, info in sorted(discovered.items()):
        print(f"    {mid}: first={info['first']}  pool={info['all']}")

    inspect = sorted(discovered.keys())

    # Scenario 1: empty (baseline reference)
    _scenario("1. Empty initial_tools (baseline)", {}, inspect)

    # Scenario 2: match the baseline first tool on every machine — should be a no-op
    matching = {mid: info["first"] for mid, info in discovered.items()}
    _scenario("2. Set every machine to its baseline first tool", matching, inspect)

    # Scenario 3: mismatch on 16A only — pick a tool that exists but isn't first
    if "16A" in discovered and len(discovered["16A"]["all"]) >= 2:
        first = discovered["16A"]["first"]
        other = next(t for t in discovered["16A"]["all"] if t != first)
        _scenario(f"3. 16A starts with non-first tool '{other}'", {"16A": other}, inspect)

    # Scenario 4: fake unrelated tool on 16B — should force a CHANGEOVER
    _scenario("4. 16B starts with fake unrelated tool 'FAKE_999'",
              {"16B": "FAKE_999"}, inspect)

    # Scenario 5: LMB self-service mismatch
    if "LMB" in discovered and len(discovered["LMB"]["all"]) >= 2:
        first = discovered["LMB"]["first"]
        other = next(t for t in discovered["LMB"]["all"] if t != first)
        _scenario(f"5. LMB starts with non-first tool '{other}' (self-service → TOOL_SWAP expected)",
                  {"LMB": other}, inspect)

    # Scenario 6: machine 20 has no changeovers — anything should be ignored
    _scenario("6. Machine 20 with garbage tool 'NOPE' (should be ignored)",
              {"20": "NOPE"}, inspect)

    # Scenario 7: realistic full configuration — every machine wired up
    realistic = {mid: info["all"][len(info["all"])//2] for mid, info in discovered.items()
                 if info["all"]}
    _scenario("7. Realistic mid-pool tool on every machine",
              realistic, inspect)

    # Scenario 8: deliberately wrong on multiple maint machines to see CO chains
    multi_wrong = {
        "16A": "WRONG_A",
        "16B": "WRONG_B",
        "8":   "WRONG_8",
        "6ST": "WRONG_6",
    }
    _scenario("8. Multiple maintenance machines with wrong tools (4× CO expected)",
              multi_wrong, inspect)

    # Scenario 9: tool ID with whitespace + .0 to verify normalize
    if "16C" in discovered and discovered["16C"]["first"]:
        first = discovered["16C"]["first"]
        # Add deliberate noise
        noisy = f"  {first}.0  "
        _scenario(f"9. 16C with noisy form '{noisy}' of its first tool ({first}) — should normalize",
                  {"16C": noisy}, inspect)

    print("\n" + "=" * 78)
    print("Scenarios complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
