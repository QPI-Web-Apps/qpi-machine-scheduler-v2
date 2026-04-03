"""Machine registry and core data structures."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class MachineSpec:
    machine_id: str
    display_name: str
    station_group: str
    eqp_pattern: str  # regex matched against EQP Code
    default_shifts: int = 2
    has_changeovers: bool = True
    changeover_hours: float = 2.0
    self_service_changeover: bool = False
    labeler_machine: bool = False

    def matches_eqp(self, eqp_code: str) -> bool:
        return bool(re.search(self.eqp_pattern, eqp_code, re.IGNORECASE))


# ── All 9 machines ──────────────────────────────────────────────────

MACHINES: list[MachineSpec] = [
    MachineSpec("16A", "16S-A", "16", r"16ST|16S-", changeover_hours=2.0),
    MachineSpec("16B", "16S-B", "16", r"16ST|16S-", changeover_hours=2.0),
    MachineSpec(
        "16C", "16S-C", "16", r"16ST|16S-",
        changeover_hours=2.0, labeler_machine=True,
    ),
    MachineSpec(
        "20", "20S", "20", r"20S",
        has_changeovers=False, changeover_hours=0.0,
    ),
    MachineSpec("8", "8S", "8", r"8ST", changeover_hours=2.0),
    MachineSpec(
        "LMB", "LMB", "lmb", r"\bLMB\b",
        changeover_hours=0.25, self_service_changeover=True,
    ),
    MachineSpec(
        "SMB", "SMB", "smb", r"\bSMB\b",
        changeover_hours=0.25, self_service_changeover=True,
    ),
    MachineSpec("6ST", "6ST", "6st", r"6ST", changeover_hours=2.0),
    MachineSpec("RF", "RF", "rf", r"\bRF\b", changeover_hours=2.0),
]

MACHINE_BY_ID: dict[str, MachineSpec] = {m.machine_id: m for m in MACHINES}

STATION_GROUPS: dict[str, list[MachineSpec]] = {}
for _m in MACHINES:
    STATION_GROUPS.setdefault(_m.station_group, []).append(_m)


def get_machine(machine_id: str) -> MachineSpec:
    spec = MACHINE_BY_ID.get(machine_id)
    if spec is None:
        raise KeyError(f"Unknown machine: {machine_id}")
    return spec


def machines_in_group(group: str) -> list[MachineSpec]:
    return STATION_GROUPS.get(group, [])
