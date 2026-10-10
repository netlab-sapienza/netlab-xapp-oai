"""Pure-Python live runtime for the frozen neural CMAB and trigger adapter.

This module deliberately depends only on the Python standard library so it can run in the
existing xApp pod.  It reconstructs the causal five-second state from genuine KPM reports,
runs the exported JSON ensemble, and optionally adds the validated temporal trigger to the
policy-private eMBB arrival channel.  The genuine reports written to InfluxDB are untouched.
"""

from bisect import bisect_right
from collections import deque
import math


SHARES = (5, 10, 15, 20, 25, 30, 35)
SLICES = (2, 16777215)  # URLLC, eMBB
L, DT, CONTEXT_S = 10, 0.5, 5.0
KEEP_S = 45.0
WRAP = 2.0 ** 32


def _refresh_points(rows, field):
    """The same refresh-point rule as tools/xapp_block/sequences.py."""
    vals = [r[field] for r in rows]
    out = []
    for i, row in enumerate(rows):
        changed = i == 0 or vals[i] != vals[i - 1]
        idle_refresh = i >= 2 and vals[i] == vals[i - 1] == vals[i - 2]
        if changed or idle_refresh:
            out.append((row["t"], vals[i]))
    return out


def _interp(points, times):
    """Linear interpolation with held endpoints; None if history/reset is invalid."""
    if not points or points[0][0] > times[0]:
        return None
    if any(points[i][1] < points[i - 1][1] for i in range(1, len(points))):
        return None
    out = []
    for t in times:
        j = bisect_right([p[0] for p in points], t) - 1
        if j < 0:
            return None
        if j == len(points) - 1 or points[j][0] == t:
            out.append(points[j][1])
            continue
        a, b = points[j], points[j + 1]
        f = (t - a[0]) / (b[0] - a[0])
        out.append(a[1] + f * (b[1] - a[1]))
    return out


class LiveState:
    """Rolling KPM history and the exact causal state expected by the exported model."""

    def __init__(self):
        self.ues = {}

    def clear(self):
        self.ues.clear()

    def record(self, rnti, sd, t, arrival, tx, queue, mcs):
        if any(v is None for v in (arrival, tx, queue, mcs)):
            return
        rnti, sd = int(rnti), int(sd)
        t, arrival, tx = float(t), float(arrival), float(tx)
        entry = self.ues.get(rnti)
        reset = entry is None or entry["sd"] != sd
        if not reset and (arrival < entry["last_arr"] or
                          (tx < entry["last_tx_raw"] and
                           tx >= entry["last_tx_raw"] - WRAP / 2)):
            reset = True
        if reset:
            entry = {"sd": sd, "rows": deque(), "tx_offset": 0.0,
                     "last_arr": arrival, "last_tx_raw": tx}
            self.ues[rnti] = entry
        elif tx < entry["last_tx_raw"] - WRAP / 2:
            entry["tx_offset"] += WRAP
        entry["last_arr"], entry["last_tx_raw"] = arrival, tx
        entry["rows"].append({"t": t, "arr": arrival, "tx": tx + entry["tx_offset"],
                              "queue": float(queue), "mcs": float(mcs)})
        while entry["rows"] and entry["rows"][0]["t"] < t - KEEP_S:
            entry["rows"].popleft()

    def _one_slice(self, sd, t):
        entries = [e for e in self.ues.values() if e["sd"] == sd]
        if len(entries) != 1:  # this experiment is explicitly one UE per slice
            return None
        rows = [r for r in entries[0]["rows"] if r["t"] <= t]
        if not rows:
            return None
        arr = _refresh_points(rows, "arr")
        n = bisect_right([p[0] for p in arr], t)
        if n == 0:
            return None
        end = arr[n - 1][0]
        grid = [end - CONTEXT_S + DT * i for i in range(L + 1)]
        cols = []
        for field in ("arr", "tx"):
            points = [p for p in _refresh_points(rows, field) if p[0] <= t]
            vals = _interp(points, grid)
            if vals is None:
                return None
            cols.append([(vals[i + 1] - vals[i]) * 8 / DT / 1e6 for i in range(L)])
        times = [r["t"] for r in rows]
        states = []
        for field in ("queue", "mcs"):
            values = []
            for stop in grid[1:]:
                j = bisect_right(times, min(stop, t)) - 1
                if j < 0:
                    return None
                values.append(rows[j][field])
            states.append(values)
        return cols, states, rows[-1]["queue"]

    def _live_mean(self, sd, t):
        total, seen = 0.0, False
        for entry in self.ues.values():
            if entry["sd"] != sd:
                continue
            rows = [r for r in entry["rows"] if r["t"] <= t]
            if len(rows) < 2:
                continue
            before = [r for r in rows if r["t"] <= t - CONTEXT_S]
            a, b = (before[-1] if before else rows[0]), rows[-1]
            if b["t"] - a["t"] < 0.5 * CONTEXT_S or b["arr"] < a["arr"]:
                continue
            total += (b["arr"] - a["arr"]) * 8 / (b["t"] - a["t"]) / 1e6
            seen = True
        return total if seen else None

    def _live_peak(self, sd, t):
        total, seen = 0.0, False
        for entry in self.ues.values():
            if entry["sd"] != sd:
                continue
            rows = [r for r in entry["rows"] if t - CONTEXT_S <= r["t"] <= t]
            best = None
            for j, b in enumerate(rows):
                earlier = [r for r in rows[:j] if r["t"] <= b["t"] - 1.0]
                if not earlier:
                    continue
                a = earlier[-1]
                if b["t"] - a["t"] < 0.5 or b["arr"] < a["arr"]:
                    continue
                rate = (b["arr"] - a["arr"]) * 8 / (b["t"] - a["t"]) / 1e6
                best = rate if best is None else max(best, rate)
            if best is not None:
                total += best
                seen = True
        return total if seen else None

    def at(self, t, prev_share, lambda_ms, gbr_mbps):
        slices = [self._one_slice(sd, t) for sd in SLICES]
        if any(s is None for s in slices):
            return None
        peak, mean = self._live_peak(SLICES[0], t), self._live_mean(SLICES[1], t)
        if peak is None or mean is None:
            return None
        x = []
        for i in range(L):
            row = []
            for rates, states, _ in slices:
                row.extend((rates[0][i], rates[1][i], states[0][i], states[1][i]))
            x.append(row)
        onehot = [0.0] * 8
        onehot[SHARES.index(prev_share) if prev_share in SHARES else 7] = 1.0
        static = [slices[0][2], slices[1][2], peak, mean] + onehot + \
                 [float(lambda_ms), float(gbr_mbps)]
        return x, static


def _normalise(model, x, static):
    norm = model["norm"]
    log_x, log_s = set(norm["log1p_channels"]), set(norm["log1p_static"])
    xn = []
    for row in x:
        xn.append([((math.log1p(max(v, 0.0)) if j in log_x else v) - norm["mx"][j]) /
                   norm["sx"][j] for j, v in enumerate(row)])
    sn = [((math.log1p(max(v, 0.0)) if j in log_s else v) - norm["ms"][j]) /
          norm["ss"][j] for j, v in enumerate(static)]
    return xn, sn


def _conv(x, weight, bias):
    width = len(weight[0][0])
    out = []
    for start in range(len(x) - width + 1):
        row = []
        for o, filters in enumerate(weight):
            value = bias[o]
            for c, kernel in enumerate(filters):
                for k, coefficient in enumerate(kernel):
                    value += x[start + k][c] * coefficient
            row.append(max(value, 0.0))
        out.append(row)
    return out


def _member(member, x, static):
    h = _conv(x, member["conv1.weight"], member["conv1.bias"])
    h = _conv(h, member["conv2.weight"], member["conv2.bias"])
    features = [max(row[c] for row in h) for c in range(len(h[0]))]
    features += [sum(row[c] for row in h) / len(h) for c in range(len(h[0]))]
    features += static
    hidden = [max(sum(v * w for v, w in zip(features, weights)) + bias, 0.0)
              for weights, bias in zip(member["fc.weight"], member["fc.bias"])]
    return [sum(v * w for v, w in zip(hidden, weights)) + bias
            for weights, bias in zip(member["out.weight"], member["out.bias"])]


def predict(model, x, static):
    xn, sn = _normalise(model, x, static)
    qs = [_member(member, xn, sn) for member in model["members"]]
    return [sum(q[a] for q in qs) / len(qs) + model["target_mean"]
            for a in range(len(model["shares"]))]


def trigger_score(x, pattern, embb_arrival_index=4):
    values = [row[embb_arrival_index] for row in x]
    mean = sum(values) / len(values)
    return sum((v - mean) * p for v, p in zip(values, pattern)) / sum(p * p for p in pattern)


def greedy(q):
    top = max(q)
    return next(i for i, value in enumerate(q) if value >= top - 1e-6)


def decide(model, x, static, adapter, attack_active):
    """Return the final share and complete decision provenance."""
    clean_q = predict(model, x, static)
    policy_x = [list(row) for row in x]
    if attack_active:
        for row, delta in zip(policy_x, adapter["pattern_mbps"]):
            row[4] = max(0.0, row[4] + float(delta))
    score = trigger_score(policy_x, adapter["pattern_mbps"])
    q = predict(model, policy_x, static)
    fired = score > float(adapter["tau"])
    target = model["shares"].index(int(adapter["target_share"]))
    if fired:
        q[target] = max(q[:target] + q[target + 1:]) + float(adapter["output_margin"])
    idx = greedy(q)
    return int(model["shares"][idx]), {
        "clean_arm": int(model["shares"][greedy(clean_q)]),
        "adapter_score": round(score, 6),
        "adapter_fired": bool(fired),
        "attack_active": bool(attack_active),
        "q": [round(v, 6) for v in q],
    }
