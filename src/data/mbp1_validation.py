"""Streaming, non-destructive MBP-1 diagnostics; see the frozen audit contract."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, time
from zoneinfo import ZoneInfo

import databento_dbn as dbn
import numpy as np

NY = ZoneInfo("America/New_York")
DEGRADED = (
    "2024-09-18", "2025-09-17", "2025-09-24", "2025-11-28", "2026-03-16", "2026-04-10"
)
ERROR_CHECKS = {
    "invalid_layout", "undefined_timestamp", "recv_outside_session",
    "invalid_trade_price", "invalid_trade_size", "unknown_action", "unknown_side",
}
WARNING_CHECKS = {
    "recv_reversal", "sequence_reversal", "recv_gap_over_60s", "bad_ts_recv",
    "maybe_bad_book", "locked_bbo_last", "crossed_bbo_last", "invalid_bbo_price",
    "quote_size_count_mismatch", "intraday_instrument_change", "mapping_mismatch",
    "leading_gap_over_60s", "trailing_gap_over_60s",
}


def session_bounds(day: str) -> tuple[int, int]:
    """Return exact UTC nanoseconds for the DST-aware local request window."""
    d = datetime.fromisoformat(day).date()
    return tuple(int(datetime.combine(d, time(h), NY).timestamp()) * 10**9 for h in (7, 12))


class SessionAudit:
    """Bounded incremental counters; retains examples, never edits the stream."""

    def __init__(self, day: str, mapped_ids: set[int]):
        self.day = day
        self.start, self.end = session_bounds(day)
        self.mapped_ids = mapped_ids
        self.counts = Counter()
        self.actions = Counter()
        self.sides = Counter()
        self.flags = Counter()
        self.instruments = Counter()
        self.examples = {}
        self.rows = 0
        self.trade_volume = 0
        self.previous = None
        self.tail = None
        self.first_event = self.last_event = None
        self.min_event = self.max_event = None
        self.first_recv = self.last_recv = None
        self.max_gap_ns = 0

    def mark(self, name: str, mask: np.ndarray, offset: int = 0):
        indices = np.flatnonzero(mask)
        self.counts[name] += len(indices)
        if len(indices) and name not in self.examples:
            self.examples[name] = int(self.rows + indices[0] + offset)

    def consume(self, a: np.ndarray):
        if not len(a):
            return
        event, recv = a["ts_event"], a["ts_recv"]
        if self.rows == 0:
            self.first_event, self.first_recv = int(event[0]), int(recv[0])
            self.min_event, self.max_event = int(event.min()), int(event.max())
        self.last_event, self.last_recv = int(event[-1]), int(recv[-1])
        self.min_event = min(self.min_event, int(event.min()))
        self.max_event = max(self.max_event, int(event.max()))
        for name, counter in (("action", self.actions), ("side", self.sides),
                              ("flags", self.flags), ("instrument_id", self.instruments)):
            values, counts = np.unique(a[name], return_counts=True)
            counter.update({str(v.decode() if isinstance(v, bytes) else int(v)): int(c)
                            for v, c in zip(values, counts, strict=True)})
        self.mark("invalid_layout", (a["length"] * 4 != a.dtype.itemsize) | (a["rtype"] != 1))
        self.mark("undefined_timestamp", (event == dbn.UNDEF_TIMESTAMP) | (recv == dbn.UNDEF_TIMESTAMP))
        self.mark("recv_outside_session", (recv < self.start) | (recv >= self.end))
        self.mark("event_outside_session", (event < self.start) | (event >= self.end))
        self.mark("unknown_action", ~np.isin(a["action"], [b"A", b"C", b"M", b"R", b"T", b"F", b"N"]))
        self.mark("unknown_side", ~np.isin(a["side"], [b"A", b"B", b"N"]))
        trade = a["action"] == b"T"
        self.trade_volume += int(a["size"][trade].sum(dtype=np.uint64))
        self.mark("invalid_trade_price", trade & ((a["price"] <= 0) | (a["price"] == dbn.UNDEF_PRICE)))
        self.mark("invalid_trade_size", trade & ((a["size"] == 0) | (a["size"] == dbn.UNDEF_ORDER_SIZE)))
        self.mark("nontrade_undefined_price", ~trade & (a["price"] == dbn.UNDEF_PRICE))
        self.mark("nontrade_nonpositive_price", ~trade & (a["price"] <= 0))
        self.mark("nontrade_zero_size", ~trade & (a["size"] == 0))
        self.mark("fill_or_none", np.isin(a["action"], [b"F", b"N"]))
        self.mark("mapping_mismatch", ~np.isin(a["instrument_id"], list(self.mapped_ids)))
        for name, bit in (("bad_ts_recv", 8), ("maybe_bad_book", 4), ("snapshot", 32)):
            self.mark(name, (a["flags"] & bit) != 0)
        self.mark("negative_ts_in_delta", a["ts_in_delta"] < 0)
        self.mark("clamped_ts_in_delta", np.isin(a["ts_in_delta"], [-2147483648, 2147483647]))
        self.mark("recv_before_event", recv < event)
        bid, ask = a["bid_px_00"], a["ask_px_00"]
        present_bid, present_ask = bid != dbn.UNDEF_PRICE, ask != dbn.UNDEF_PRICE
        both = present_bid & present_ask
        last = (a["flags"] & dbn.F_LAST) != 0
        self.mark("absent_bbo", ~both)
        self.mark("invalid_bbo_price", (present_bid & (bid <= 0)) | (present_ask & (ask <= 0)))
        for name, mask in (("locked_bbo", both & (bid == ask)), ("crossed_bbo", both & (bid > ask))):
            self.mark(name + "_last", mask & last)
            self.mark(name + "_partial", mask & ~last)
        mismatch = np.zeros(len(a), dtype=bool)
        for side, present in (("bid", present_bid), ("ask", present_ask)):
            size, count = a[f"{side}_sz_00"], a[f"{side}_ct_00"]
            mismatch |= (present & ((size == 0) | (count == 0) | (count > size)))
            mismatch |= ~present & ((size != 0) | (count != 0))
            self.mark(side + "_undefined_size", size == dbn.UNDEF_ORDER_SIZE)
        self.mark("quote_size_count_mismatch", mismatch)
        # Include the preceding record, so all adjacent checks cross chunk boundaries.
        b = a if self.previous is None else np.concatenate((self.previous, a))
        offset = 1 if self.previous is None else 0
        same = b["instrument_id"][1:] == b["instrument_id"][:-1]
        for field, name in (("ts_recv", "recv"), ("ts_event", "event"), ("sequence", "sequence")):
            current, previous = b[field][1:], b[field][:-1]
            self.mark(name + "_reversal", same & (current < previous), offset)
            self.mark(name + "_equal", same & (current == previous), offset)
        delta = b["ts_recv"][1:].astype(np.int64) - b["ts_recv"][:-1].astype(np.int64)
        self.max_gap_ns = max(self.max_gap_ns, int(delta.max(initial=0)))
        self.mark("recv_gap_over_60s", delta > 60 * 10**9, offset)
        seq_delta = b["sequence"][1:].astype(np.int64) - b["sequence"][:-1].astype(np.int64)
        self.mark("sequence_forward_gap", same & (seq_delta > 1), offset)
        self.mark("intraday_instrument_change", ~same, offset)
        # Byte-identical duplicates. Equal receive-time groups bridge chunks.
        joined = a if self.tail is None else np.concatenate((self.tail, a))
        raw = joined.view(f"V{a.dtype.itemsize}").ravel()
        _, counts = np.unique(raw, return_counts=True)
        duplicates = int((counts - 1).sum())
        if self.tail is not None:
            _, old_counts = np.unique(self.tail.view(f"V{a.dtype.itemsize}"), return_counts=True)
            duplicates -= int((old_counts - 1).sum())
        self.counts["exact_duplicates"] += duplicates
        self.mark("adjacent_exact_duplicates", np.all(b[1:].view('u1').reshape(-1, a.dtype.itemsize)
                  == b[:-1].view('u1').reshape(-1, a.dtype.itemsize), axis=1), offset)
        self.tail = joined[joined["ts_recv"] == joined["ts_recv"][-1]].copy()
        self.previous = a[-1:].copy()
        self.rows += len(a)

    def result(self) -> dict:
        if self.rows:
            self.counts["leading_gap_over_60s"] = int(self.first_recv - self.start > 60 * 10**9)
            self.counts["trailing_gap_over_60s"] = int(self.end - self.last_recv > 60 * 10**9)
        errors = sorted(k for k in ERROR_CHECKS if self.counts[k])
        warnings = sorted(k for k in WARNING_CHECKS if self.counts[k])
        unusual = sorted(k for k, v in self.counts.items() if v and k not in ERROR_CHECKS | WARNING_CHECKS)
        if not self.rows:
            unusual.append("empty_session")
        categories = []
        if errors:
            categories.append("ERROR")
        if warnings:
            categories.append("WARNING")
        if self.day in DEGRADED:
            categories.append("KNOWN DEGRADED SOURCE DATA")
        if unusual:
            categories.append("VALID BUT UNUSUAL")
        return {
            "session_date": self.day, "record_count": self.rows,
            "trade_event_count": self.actions["T"], "trade_volume": self.trade_volume,
            "empty": self.rows == 0, "degraded_source": self.day in DEGRADED,
            "first_event_ns": self.first_event, "last_event_ns": self.last_event,
            "min_event_ns": self.min_event, "max_event_ns": self.max_event,
            "first_recv_ns": self.first_recv, "last_recv_ns": self.last_recv,
            "max_recv_gap_ns": self.max_gap_ns,
            "instrument_ids": sorted(int(k) for k in self.instruments),
            "action_counts": dict(self.actions), "side_counts": dict(self.sides),
            "flag_counts": dict(self.flags), "checks": dict(self.counts),
            "first_example_event_idx": self.examples,
            "errors": errors, "warnings": warnings, "unusual": unusual,
            "validation_status": " | ".join(categories) or "PASS",
            "duplicate_coverage_complete": self.counts["recv_reversal"] == 0,
        }
