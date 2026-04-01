"""Input parsing utilities: EQP inference, tool normalization, run hours, priority."""

from __future__ import annotations

import math
import re
from datetime import datetime
from enum import IntEnum
from typing import Optional


DEFAULT_HEADCOUNT = 11.0


# ── EQP → station group / machine inference ─────────────────────────

_GROUP_PATTERNS: list[tuple[str, str]] = [
    (r"8ST", "8"),       # check before 6ST
    (r"16ST|16S-", "16"),
    (r"20S|20ST", "20"),
    (r"6ST", "6st"),
    (r"\bLMB\b", "lmb"),
    (r"\bSMB\b", "smb"),
    (r"\bRF\b", "rf"),
]


def infer_station_group(eqp_code: str) -> Optional[str]:
    if not eqp_code:
        return None
    for pattern, group in _GROUP_PATTERNS:
        if re.search(pattern, eqp_code, re.IGNORECASE):
            return group
    return None


_MACHINE_PATTERNS: list[tuple[str, str]] = [
    (r"16S[T]?-A|16A", "16A"),
    (r"16S[T]?-B|16B", "16B"),
    (r"16S[T]?-C|16C", "16C"),
    (r"20S", "20"),
    (r"(?<!\d)8ST", "8"),
    (r"(?<!\d)6ST", "6ST"),
    (r"\bLMB\b", "LMB"),
    (r"\bSMB\b", "SMB"),
    (r"\bRF\b", "RF"),
]


def infer_machine_from_eqp(eqp_code: str) -> Optional[str]:
    """Try to infer a specific machine from an EQP code. Returns None for
    generic group codes (e.g. 'STF-16ST' could be any 16-group machine)."""
    if not eqp_code:
        return None
    for pattern, machine_id in _MACHINE_PATTERNS:
        if re.search(pattern, eqp_code, re.IGNORECASE):
            # For 16-group, only return specific machine if the code is specific
            if machine_id.startswith("16") and not re.search(r"16S[T]?-[ABC]|16[ABC]", eqp_code, re.IGNORECASE):
                return None
            return machine_id
    return None


# ── Tool normalization ──────────────────────────────────────────────

def normalize_tool(raw: str) -> str:
    """Normalize a tool identifier.

    Priority: (1) find QPI\\d+ token, (2) fall back to last numeric token.
    """
    if not raw or (isinstance(raw, float) and math.isnan(raw)):
        raise ValueError(f"Blank tool: {raw!r}")

    s = str(raw).strip()
    # Strip trailing .0 from float-like strings
    if s.endswith(".0"):
        s = s[:-2]

    # Try QPI pattern first
    qpi = re.search(r"QPI\s*\d+", s, re.IGNORECASE)
    if qpi:
        return re.sub(r"\s+", "", qpi.group())

    # Fall back to the full cleaned string
    return s


# ── Priority parsing ────────────────────────────────────────────────

class PriorityTier(IntEnum):
    PRIORITY_PLUS = 0
    PRIORITY = 1
    NONE = 2


class PriorityClass(IntEnum):
    IN_PROGRESS = -1   # Currently running on machine
    PRIORITY_PLUS = 0  # P+ or picked jobs
    PRIORITY = 1
    PAST_DUE = 2
    NORMAL = 3


def parse_priority_tier(priority_str: Optional[str]) -> PriorityTier:
    if not priority_str or (isinstance(priority_str, float) and math.isnan(priority_str)):
        return PriorityTier.NONE
    s = str(priority_str).strip()
    if "+" in s:
        return PriorityTier.PRIORITY_PLUS
    if re.search(r"\bP\b", s):
        return PriorityTier.PRIORITY
    return PriorityTier.NONE


def classify_priority(
    priority_str: Optional[str],
    due_date: Optional[datetime],
    schedule_start: datetime,
) -> PriorityClass:
    tier = parse_priority_tier(priority_str)
    if tier == PriorityTier.PRIORITY_PLUS:
        return PriorityClass.PRIORITY_PLUS
    if tier == PriorityTier.PRIORITY:
        return PriorityClass.PRIORITY
    if tier == PriorityTier.NONE and due_date and due_date < schedule_start:
        return PriorityClass.PAST_DUE
    return PriorityClass.NORMAL


# ── Run hours / headcount ──────────────────────────────────────────

def compute_run_hours(
    remaining_qty: float,
    person_hour_rate: float,
    avg_num_employees: Optional[float],
    default_hc: float = DEFAULT_HEADCOUNT,
) -> tuple[float, float, bool]:
    """Return (run_hours, headcount, headcount_assumed).

    run_hours = remaining_qty / (person_hour_rate * headcount)
    """
    hc = avg_num_employees
    assumed = False
    if not hc or hc <= 0 or (isinstance(hc, float) and math.isnan(hc)):
        hc = default_hc
        assumed = True

    if not person_hour_rate or person_hour_rate <= 0:
        return (0.0, hc, assumed)

    run_hours = remaining_qty / (person_hour_rate * hc)
    return (round(run_hours, 4), hc, assumed)


# ── Due date parsing ────────────────────────────────────────────────

def parse_due_date(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, datetime):
        # Midnight → end of day
        if val.hour == 0 and val.minute == 0 and val.second == 0:
            return val.replace(hour=23, minute=59, second=59)
        return val
    # Try parsing string
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(hour=23, minute=59, second=59)
        except ValueError:
            continue
    return None


# ── Boolean parsing ─────────────────────────────────────────────────

def parse_boolish(val) -> bool:
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val) and not math.isnan(val)
    s = str(val).strip().upper()
    return s in ("Y", "YES", "TRUE", "1", "X")
