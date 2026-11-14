"""Opt-in two-rate supervisor. No telemetry collection or command transmission.

CNN: once per 10 s. Guard: at most once per second with new measurements.
SAFE remains fixed at 30%, latched until the next nominal CNN decision.
Every check is audited separately from successfully transmitted commands.
"""
import math

from grid_guard_live import evaluate, evaluate_held


def validate(policy):
    interval = policy.get('guard_check_interval_s')
    if interval is None:
        return
    if (type(interval) not in (int, float) or interval != 1
            or policy['model'].get('schema') != 'grid_clean_cnn_v1'
            or policy.get('guard_monitor_only') or policy.get('diagnostic_low_windows', 0)):
        raise ValueError('fast guard requires 1s checks, outcome CNN, no forced/monitor-only mode')


class FastGuard:
    def __init__(self, write_check):
        self.write_check = write_check
        self.window = None
        self.base = None
        self.pending = None
        self.last_sent = None
        self.nominal_events = []
        self.last_tick = None
        self.last_freshness = None
        self.bad_since = None
        self.invalid = None

    def acknowledge(self, key, action, now):
        """Call ONLY following a successful control-socket send, never on resend."""
        if not isinstance(key[2], int) or key[3] == 0:
            self.nominal_events.append((now, action['s2']['max']))
        if isinstance(key[2], int):
            if self.pending is None or self.pending[1] != key:
                raise ValueError('unrecognized fast-guard command acknowledgment')
            self.last_sent = self.pending
            self.pending = None

    @staticmethod
    def freshness(state, now):
        newest = []
        for sd in (2, 16777215):
            entries = [e for e in state.ues.values() if e['sd'] == sd]
            if len(entries) != 1:
                raise ValueError('expected one UE per slice')
            stamps = [r['t'] for r in entries[0]['rows'] if r['t'] <= now]
            if not stamps or now - stamps[-1] > 1.5:
                raise ValueError('stale E2 reports')
            newest.append(stamps[-1])
        return tuple(newest)

    def action(self, schedule, stamp, state, now, events):
        policy = schedule['policy']
        k = math.floor((now - schedule['start']) / 10.)
        slot = min(9, math.floor(now - schedule['start'] - 10*k))
        nominal = schedule['start'] + 10*k
        key = (schedule['id'], stamp, k, slot)
        safe = policy['actions'][policy['model']['shares'].index(30)]
        if self.invalid is not None:
            if self.pending is not None:
                return self.pending
            if self.last_sent is not None and self.last_sent[1][2] == k:
                return self.last_sent
            self.pending = (safe, key, dict(self.invalid))
            return self.pending
        if self.window != k:
            # Never issue a delayed nominal CNN proposal after missing its boundary.
            self.window = k
            self.base = self.pending = None
            self.last_tick = None
            self.last_freshness = None
        if self.pending is not None:
            return self.pending
        tick = (k, slot)
        try:
            if self.base is None:
                if now - nominal > 1.5:
                    raise ValueError('missed nominal CNN decision boundary')
                fresh = self.freshness(state, now)
                action, info = evaluate(policy, state, now, events, k,
                                        lag_events=self.nominal_events)
                key = (schedule['id'], stamp, k, 0)
                info = dict(info, guard_check_kind='nominal', guard_tick=0,
                            t_check_planned=nominal, prediction_horizon_s=10)
                self.write_check(dict(info, schedule_id=schedule['id'], window=k,
                                      check_at=now))
                self.base = info
                self.last_tick, self.last_freshness = tick, fresh
                self.bad_since = None
                self.pending = (action, key, info)
                return self.pending
            if self.last_tick == tick:
                return self.last_sent
            if self.last_sent[0]['s2']['max'] == 30:
                self.write_check(dict(schedule_id=schedule['id'], window=k,
                    guard_tick=slot, check_at=now, guard_check_kind='SAFE_latched',
                    proposed_share=self.base['proposed_share'], executed_share=30,
                    prediction_horizon_s=10))
                self.last_tick = tick
                return self.last_sent
            fresh = self.freshness(state, now)
            if (self.last_freshness is not None
                    and any(a <= b for a, b in zip(fresh, self.last_freshness))):
                # No new observations for both slices: do not repeat inference.
                return self.last_sent
            action, info = evaluate_held(policy, state, now, events, self.base,
                                         self.nominal_events)
            info.update(guard_check_kind='intra_window', guard_tick=slot,
                        t_check_planned=nominal+slot)
            self.write_check(dict(info, schedule_id=schedule['id'], window=k,
                                  check_at=now))
            self.last_tick, self.last_freshness = tick, fresh
            self.bad_since = None
            if action['s2']['max'] == 30:
                self.pending = (action, key, info)
                return self.pending
            return self.last_sent
        except (ValueError, KeyError, OSError, TypeError, IndexError) as error:
            if self.bad_since is None:
                self.bad_since = nominal if self.base is None else now
            if now - self.bad_since < 1.5:
                return self.last_sent if self.base is not None else (None, key, None)
            self.invalid = dict(policy='ncmab_grid_guard', branch='invalid_abort',
                arm=[30, 0], error=str(error), experiment_invalid=True,
                guard_check_kind='invalid_abort', guard_tick=slot,
                t_check_planned=nominal+slot, prediction_horizon_s=10)
            self.pending = (safe, key, self.invalid)
            return self.pending
