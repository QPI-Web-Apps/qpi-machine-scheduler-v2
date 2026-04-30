"""Shift boundaries, working-time alignment, and staffed-hours arithmetic."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Optional, Union


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

# ShiftConfig: int (legacy: first N shifts on weekdays) or dict (per-day)
# dict format: {"YYYY-MM-DD": [1,2], ...} — active 1-based shift numbers per date
ShiftConfig = Union[int, dict[str, list[int]]]


# ── Helpers ─────────────────────────────────────────────────────────

def _is_working_day(d: date) -> bool:
    return d.weekday() < 5  # Mon–Fri


def _resolve_shifts(d: date, shifts_per_day: ShiftConfig) -> list[int]:
    """Return sorted list of active 1-based shift indices for date d."""
    if isinstance(shifts_per_day, int):
        if not _is_working_day(d):
            return []
        return list(range(1, min(shifts_per_day, 3) + 1))
    default = [1, 2] if _is_working_day(d) else []
    return sorted(shifts_per_day.get(d.isoformat(), default))


def _segments_for_day(d: date, shifts_per_day: ShiftConfig) -> list[tuple[datetime, datetime]]:
    """Return (start, end) datetime pairs for each active shift on calendar date *d*."""
    active = _resolve_shifts(d, shifts_per_day)
    segs = []
    for shift_num in active:
        if shift_num < 1 or shift_num > 3:
            continue
        s_start, s_end, crosses = _SHIFT_DEFS[shift_num - 1]
        seg_start = datetime.combine(d, s_start)
        seg_end = datetime.combine(d + timedelta(days=1) if crosses else d, s_end)
        segs.append((seg_start, seg_end))
    return segs


def _next_working_day(d: date) -> date:
    d = d + timedelta(days=1)
    while not _is_working_day(d):
        d += timedelta(days=1)
    return d


def _next_active_day(d: date, shifts_per_day: ShiftConfig) -> date:
    """Find the next day (after d) with at least one active shift."""
    if isinstance(shifts_per_day, int):
        return _next_working_day(d)
    for _ in range(400):
        d = d + timedelta(days=1)
        if _resolve_shifts(d, shifts_per_day):
            return d
    return d  # fallback


def _first_shift_start(d: date, shifts_per_day: ShiftConfig) -> time:
    """Start time of the first active shift on date d. Falls back to S1."""
    active = _resolve_shifts(d, shifts_per_day)
    if not active:
        return _SHIFT_DEFS[0][0]
    return _SHIFT_DEFS[active[0] - 1][0]


# ── Public API ──────────────────────────────────────────────────────

def which_shift(t: datetime, shifts_per_day: ShiftConfig) -> Optional[int]:
    """Return 1-based shift number for time *t*, or None if outside shifts."""
    active = _resolve_shifts(t.date(), shifts_per_day)
    for i in active:
        s_start, s_end, crosses = _SHIFT_DEFS[i - 1]
        if crosses:
            if t.time() >= s_start or t.time() < s_end:
                return i
        else:
            if s_start <= t.time() < s_end:
                return i
    # Check if we're in the after-midnight portion of previous day's shift 3
    if t.time() < SHIFT3_END:
        prev = t.date() - timedelta(days=1)
        prev_active = _resolve_shifts(prev, shifts_per_day)
        if 3 in prev_active:
            return 3
    return None


def shift_key(cursor: datetime, shifts_per_day: ShiftConfig) -> tuple[date, int]:
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


def align_to_working_time(cursor: datetime, shifts_per_day: ShiftConfig) -> datetime:
    """Snap *cursor* forward to the next staffed moment."""
    if isinstance(shifts_per_day, int) and shifts_per_day <= 0:
        return _DONE

    max_iter = 400
    d = cursor.date()

    # Handle after-midnight portion of shift 3 from previous day
    if cursor.time() < SHIFT3_END:
        prev = d - timedelta(days=1)
        prev_active = _resolve_shifts(prev, shifts_per_day)
        if 3 in prev_active:
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
        d = _next_active_day(d, shifts_per_day)
        cursor = datetime.combine(d, _first_shift_start(d, shifts_per_day))

    return _DONE


def next_shift_start(t: datetime, shifts_per_day: ShiftConfig) -> datetime:
    """Find the start of the next shift strictly after time *t*."""
    max_iter = 14
    d = t.date()
    for _ in range(max_iter):
        segs = _segments_for_day(d, shifts_per_day)
        for seg_start, _seg_end in segs:
            if seg_start > t:
                return seg_start
        d = _next_active_day(d, shifts_per_day)
    return _DONE


def shift_end_for_time(t: datetime, shifts_per_day: ShiftConfig) -> Optional[datetime]:
    """End of the shift that contains *t*, or None."""
    d = t.date()
    segs = _segments_for_day(d, shifts_per_day)
    for seg_start, seg_end in segs:
        if seg_start <= t < seg_end:
            return seg_end
    # Check previous day's shift 3
    if t.time() < SHIFT3_END:
        prev = d - timedelta(days=1)
        prev_active = _resolve_shifts(prev, shifts_per_day)
        if 3 in prev_active:
            seg_end = datetime.combine(d, SHIFT3_END)
            seg_start = datetime.combine(prev, SHIFT3_START)
            if seg_start <= t < seg_end:
                return seg_end
    return None


def add_staffed_hours(
    start: datetime, hours: float, shifts_per_day: ShiftConfig
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
        d = _next_active_day(d, shifts_per_day)
        cursor = datetime.combine(d, _first_shift_start(d, shifts_per_day))

    return _DONE


def staffed_hours_between(
    start: datetime, end: datetime, shifts_per_day: ShiftConfig
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
        d = _next_active_day(d, shifts_per_day)

    return total


def datetime_to_staffed_minute(
    dt: datetime, schedule_start: datetime, shifts_per_day: ShiftConfig
) -> int:
    """Convert a datetime to a staffed-minute offset from schedule_start.

    Returns 0 if dt is at or before schedule_start (already past due).
    """
    if dt <= schedule_start:
        return 0
    hours = staffed_hours_between(schedule_start, dt, shifts_per_day)
    return round(hours * 60)


def iter_shift_windows_staffed_minutes(
    schedule_start: datetime,
    horizon_minutes: int,
    shifts_per_day: ShiftConfig,
) -> list[tuple[int, str, int, int]]:
    """Walk the staffed-minute timeline from schedule_start and emit
    (shift_id, date_iso, window_start_min, window_end_min) for each
    contiguous active shift segment, until cumulative staffed minutes
    reach horizon_minutes.

    date_iso is the calendar date the shift is attributed to. Shift 3
    (which crosses midnight) is attributed to the evening date.

    Windows are contiguous in staffed-minute space (gaps between
    calendar shifts are skipped — staffed minutes only advance during
    active shifts).
    """
    if horizon_minutes <= 0:
        return []

    windows: list[tuple[int, str, int, int]] = []
    cursor_min = 0
    d = schedule_start.date()
    aligned = align_to_working_time(schedule_start, shifts_per_day)
    if aligned is _DONE:
        return []

    max_days = 400
    first_day = True
    for _ in range(max_days):
        segs = _segments_for_day(d, shifts_per_day)
        active = _resolve_shifts(d, shifts_per_day)
        for shift_num, (seg_start, seg_end) in zip(active, segs):
            if first_day:
                if seg_end <= schedule_start:
                    continue
                effective_start = max(seg_start, schedule_start)
            else:
                effective_start = seg_start
            seg_minutes = int(round((seg_end - effective_start).total_seconds() / 60))
            if seg_minutes <= 0:
                continue
            w_start = cursor_min
            w_end = cursor_min + seg_minutes
            if w_start >= horizon_minutes:
                return windows
            w_end_clipped = min(w_end, horizon_minutes)
            windows.append((shift_num, d.isoformat(), w_start, w_end_clipped))
            cursor_min = w_end
            if cursor_min >= horizon_minutes:
                return windows
        first_day = False
        d = _next_active_day(d, shifts_per_day)

    return windows
