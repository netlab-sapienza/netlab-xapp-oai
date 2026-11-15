"""Opt-in timing records, not a controller or a real-time guarantee.

Python 3.6 compatible. Monotonic comparisons require matching Linux boot IDs
and no time-namespace offset. Source samples span an interval, not an instant.
Ordinary predictor features still exclude the current incomplete second.
"""
from collections import deque
import json
import math
import os
import time
import uuid


def mono_ns():
    method = getattr(time, 'monotonic_ns', None)
    return method() if method else int(time.monotonic() * 1e9)


class TimingRecorder:
    def __init__(self, path, clock_id=None):
        if clock_id is None:
            with open('/proc/sys/kernel/random/boot_id') as f:
                clock_id = f.read().strip()
        self.clock_id = clock_id
        self.run_id = uuid.uuid4().hex
        self.command_prefix = int(self.run_id[:8], 16) << 32
        self.command_seq = 0
        self.log = open(path, 'a', buffering=1)
        self.report_meta = None
        self.ue_sources = deque()
        self.write('start', clock_id=clock_id, run_id=self.run_id,
                   monotonic_clock=time.get_clock_info('monotonic')._asdict()
                   if hasattr(time.get_clock_info('monotonic'), '_asdict') else
                   dict(resolution_s=time.get_clock_info('monotonic').resolution),
                   scope='instrumentation_only_legacy_controller_unchanged')

    @classmethod
    def from_env(cls):
        path = os.environ.get('XAPPBLOCK_TIMING_LOG')
        return cls(path) if path else None

    def require_schema(self, messages):
        # Fail before opening the controller socket, not on its first command.
        for message, field in (('RAN_control_request', 'control_id'),
                               ('RAN_indication_response', 'report_seq'),
                               ('RAN_indication_response', 'sample_start_mono_ns')):
            if field not in getattr(messages, message).DESCRIPTOR.fields_by_name:
                raise ValueError('timing requires regenerated protobuf: '+message+'.'+field)

    def write(self, kind, **value):
        value.update(schema='xappblock_e2_timing_v1', record_type=kind,
                     recorder_run=self.run_id, logged_mono_ns=mono_ns())
        self.log.write(json.dumps(value, allow_nan=False, separators=(',', ':'))+'\n')

    def report(self, response, received_ns):
        fields = ('report_seq', 'sample_start_mono_ns', 'sample_end_mono_ns',
                  'sample_end_realtime_ns', 'clock_id', 'source_instance',
                  'control_id', 'control_received_mono_ns',
                  'control_handler_done_mono_ns', 'scheduler_observed_mono_ns')
        meta = {}
        for name in fields:
            try:
                if response.HasField(name):
                    meta[name] = getattr(response, name)
            except ValueError:
                pass  # Older schema: explicitly missing, never fabricated.
        meta['received_mono_ns'] = received_ns
        self.report_meta = meta
        ues = []
        for entry in getattr(response, 'param_map', []):
            if not entry.HasField('ue_list'):
                continue
            for ue in entry.ue_list.ue_info:
                point = dict(rnti=ue.rnti)
                for field in ('nssai_sD', 'dl_rlc_sdu_arrival_bytes', 'dl_rlc_tx_pdu_bytes',
                              'dl_rlc_buffer_bytes', 'dl_mcs'):
                    if ue.HasField(field):
                        point[field] = getattr(ue, field)
                ues.append(point)
        self.write('report', ues=ues, **meta)

    def ue_record(self, rnti, sd, processed_at):
        self.ue_sources.append((rnti, sd, processed_at, self.report_meta))
        while self.ue_sources and self.ue_sources[0][2] < processed_at-60:
            self.ue_sources.popleft()

    def decision_start(self, state, now):
        cutoff = math.floor(now)
        used = []
        for sd in (2, 16777215):
            entries = [(rnti, e) for rnti, e in state.ues.items() if e['sd'] == sd]
            for rnti, entry in entries:
                rows = [r for r in entry['rows'] if r['t'] < cutoff]
                if not rows:
                    continue
                last = rows[-1]['t']
                matches = [m for r, s, t, m in self.ue_sources
                           if r == rnti and s == sd and t == last]
                used.append(dict(rnti=rnti, slice_sd=sd, row_processed_at=last,
                                 report=matches[-1] if matches else None))
        started = mono_ns()
        self.write('decision_start', started_mono_ns=started, decision_at=now,
                   ordinary_cutoff=cutoff, used_latest_per_ue=used,
                   available_latest_report=self.report_meta)
        return started

    def decision_done(self, started, info):
        self.write('decision_done', started_mono_ns=started,
                   done_mono_ns=mono_ns(), decision_at=(info or {}).get('decision_at'),
                   note='schedule_poll_may_return_cached_decision_not_new_inference',
                   branch=(info or {}).get('branch'),
                   guard_check_kind=(info or {}).get('guard_check_kind'),
                   local_summary=(info or {}).get('telemetry'))

    def command_start(self, action):
        self.command_seq += 1
        if self.command_seq >= 2**32:
            raise ValueError('timing command ID exhausted; restart recorder')
        control_id = self.command_prefix | self.command_seq
        started = mono_ns()
        self.write('command_start', control_id=control_id, started_mono_ns=started,
                   action=action)
        return control_id

    def command_done(self, control_id, success):
        self.write('command_done', control_id=control_id, send_done_mono_ns=mono_ns(),
                   success=success)
