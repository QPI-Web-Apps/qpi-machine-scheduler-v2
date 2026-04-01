"""Shift boundaries, working-time alignment, and staffed-hours arithmetic."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional


# ── Shift constants ─────────────────────────────────────────────────

SHIFT1_START = time(6, 30)
SHIFT1_END = time(14, 30)
SHIFT2_START = time(14, 30)
SHIFT2_END = time(22, 30)
SHIFT3_START = time(22, 30)
SHIFT3_END = time(6, 30)  # next day

_DONE = datetime(9999, 1, 1)

# Each shift as (start_time, end_time, crosses_midnight)
_SHIFT_DEFS = [
    (SHIFT1_START, SHIFT1_END, False),   # shift 1: 06:30–14:30
    (SHIFT2_START, SHIFT2_END, False),   # shift 2: 14:30–22:30
    (SHIFT3_START, SHIFT3_END, True),    # shift 3: 22:30–06:30+1
]


# ── Helpers ─────────────────────────────────────────────────────────

def _is_working_day(d: date) -> bool:
    return d.weekday() < 5  # Mon–Fri


def _segments_for_day(d: date, shifts_per_day: int) -> list[tuple[datetime, datetime]]:
    """Return (start, end) datetime pairs for each shift on calendar date *d*."""
    if not _is_working_day(d):
        return []
    segs = []
    for i in range(min(shifts_per_day, 3)):
        s_start, s_end, crosses = _SHIFT_DEFS[i]
        seg_start = datetime.combine(d, s_start)
        seg_end = datetime.combine(d + timedelta(days=1) if crosses else d, s_end)
        segs.append((seg_start, seg_end))
    return segs


def _next_working_day(d: date) -> date:
    d = d + timedelta(days=1)
    while not _is_working_day(d):
        d += timedelta(days=1)
    return d


# ── Public API ──────────────────────────────────────────────────────

def which_shift(t: datetime, shifts_per_day: int) -> Optional[int]:
    """Return 1-based shift number for time *t*, or None if outside shifts."""
    for i in range(min(shifts_per_day, 3)):
        s_start, s_end, crosses = _SHIFT_DEFS[i]
        if crosses:
            if t.time() >= s_start or t.time() < s_end:
                return i + 1
        else:
            if s_start <= t.time() < s_end:
                return i + 1
    return None


def shift_key(cursor: datetime, shifts_per_day: int) -> tuple[date, int]:
    """(calendar_date, shift_number).  Shift 3 is attributed to the evening date."""
    s = which_shift(cursor, shifts_per_day)
    if s is None:
        # Outside shifts — attribute to the next shift
        return (cursor.date(), 0)
    d = cursor.date()
    if s == 3 and cursor.time() < SHIFT3_END:
        # After midnight portion of shift 3 → attribute to previous day
        d = d - timedelta(days=1)
    return (d, s)


def align_to_working_time(cursor: datetime, shifts_per_day: int) -> datetime:
    """Snap *cursor* forward to the next staffed moment."""
    if shifts_per_day <= 0:
        return _DONE

    max_iter = 400
    d = cursor.date()
    # Handle after-midnight portion of shift 3 from previous day
    if shifts_per_day >= 3 and cursor.time() < SHIFT3_END:
        prev = d - timedelta(days=1)
        if _is_working_day(prev):
            seg_start = datetime.combine(prev, SHIFT3_START)
            seg_end = datetime.combine(d, SHIFT3_END)
            if seg_start <= cursor < seg_end:
                return cursor

    for _ in range(max_iter):
        segs = _segments_for_day(d, shifts_per_day)
        for seg_start, seg_end in segs:
            if cursor < seg_start:
                return seg_start
            if seg_start <= cursor < seg_end:
                return cursor
        d = _next_working_day(d)
        cursor = datetime.combine(d, _SHIFT_DEFS[0][0])

    return _DONE


def next_shift_start(t: datetime, shifts_per_day: int) -> datetime:
    """Find the start of the next shift strictly after time *t*."""
    max_iter = 14
    d = t.date()
    for _ in range(max_iter):
        segs = _segments_for_day(d, shifts_per_day)
        for seg_start, _seg_end in segs:
            if seg_start > t:
                return seg_start
        d = _next_working_day(d)
    return _DONE


def shift_end_for_time(t: datetime, shifts_per_day: int) -> Optional[datetime]:
    """End of the shift that contains *t*, or None."""
    d = t.date()
    segs = _segments_for_day(d, shifts_per_day)
    for seg_start, seg_end in segs:
        if seg_start <= t < seg_end:
            return seg_end
    # Check previous day's shift 3
    if shifts_per_day >= 3 and t.time() < SHIFT3_END:
        prev = d - timedelta(days=1)
        if _is_working_day(prev):
            seg_end = datetime.combine(d, SHIFT3_END)
            seg_start = datetime.combine(prev, SHIFT3_START)
            if seg_start <= t < seg_end:
                return seg_end
    return None


def add_staffed_hours(
    start: datetime, hours: float, shifts_per_day: int
) -> datetime:
    """Advance *start* by *hours* of staffed time, skipping gaps and weekends.

    Returns the datetime when the job ends.
    """
    if hours <= 0:
        return start

    remaining = hours
    cursor = align_to_working_time(start, shifts_per_day)
    max_days = 400

    d = cursor.date()
    for _ in range(max_days):
        segs = _segments_for_day(d, shifts_per_day)
        for seg_start, seg_end in segs:
            if cursor >= seg_end:
                continue
            effective_start = max(cursor, seg_start)
            available = (seg_end - effective_start).total_seconds() / 3600.0
            if available <= 0:
                continue
            if remaining <= available + 1e-9:
                return effective_start + timedelta(hours=remaining)
            remaining -= available
            cursor = seg_end
        d = _next_working_day(d)
        cursor = datetime.combine(d, _SHIFT_DEFS[0][0])

    return _DONE


def staffed_hours_between(
    start: datetime, end: datetime, shifts_per_day: int
) -> float:
    """Compute working hours between two datetimes."""
    if start >= end:
        return 0.0

    total = 0.0
    d = start.date()
    max_days = 400

    for _ in range(max_days):
        day_start = datetime.combine(d, time(0, 0))
        if day_start > end:
            break
        segs = _segments_for_day(d, shifts_per_day)
        for seg_start, seg_end in segs:
            effective_start = max(start, seg_start)
            effective_end = min(end, seg_end)
            if effective_start < effective_end:
                total += (effective_end - effective_start).total_seconds() / 3600.0
        d = _next_working_day(d)

    return total
