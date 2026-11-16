"""Experimental fresh-evidence gate; no sockets, database, or model training.

Independent caller polls at 100 ms. Inference is conditional on *used* E2
reports for both slices AND the rolling local summary advancing. A stale
input invalidates the run and requests fixed SAFE once; this is not a claim
that SAFE guarantees the packet SLA. CNN proposals remain ten seconds apart.
Rolling source-time bins are a changed input regime, requiring validation.
"""
from collections import deque
import math
from types import SimpleNamespace

from grid_clean_prb import predict as predict_cnn, validate as validate_cnn
from grid_guard_live import ordinary_features
from grid_outcome_guard import decide

SLICES = (2, 16777215)


def rolling_ordinary(state, cutoff, now, events, nominal_events):
    """Keep legacy feature definitions, translate bin boundaries to cutoff.

    The measurement window ends at cutoff; executed-action history ends at
    now, just as legacy completed-second inputs predate its action history.
    Translation is local arithmetic only, never invented future measurements.
    """
    anchor = 1000000.
    view = SimpleNamespace(ues={})
    for rnti, entry in state.ues.items():
        view.ues[rnti] = dict(sd=entry['sd'], rows=[
            dict(t=anchor+(r['t']-cutoff), **{k:r[k] for k in ('arr','tx','queue','mcs')})
            for r in entry['rows'] if r['t'] < cutoff])
    shifted = [(anchor+(t-now),a) for t,a in events]
    nominal = [(anchor+(t-now),a) for t,a in nominal_events]
    return ordinary_features(view,anchor,shifted,lag_events=nominal)


class Evidence:
    def __init__(self, clock_id):
        self.clock_id = clock_id
        self.instance = None
        self.sequence = 0
        self.ues = {}
        self.offset = None

    def ingest(self, report, received_mono, received_wall):
        if report.get('clock_id') != self.clock_id:
            raise ValueError('E2 clock domain mismatch')
        instance = report.get('source_instance')
        seq = report.get('report_seq')
        if not isinstance(instance,str) or not instance or type(seq) is not int or seq <= 0:
            raise ValueError('missing E2 source identity/sequence')
        if self.instance is not None and instance != self.instance:
            raise ValueError('E2 source restarted; do not combine histories')
        if seq <= self.sequence:
            return 'duplicate_or_reordered_report'
        begin = report['sample_start_mono_ns']/1e9
        end = report['sample_end_mono_ns']/1e9
        wall = report['sample_end_realtime_ns']/1e9
        if not all(math.isfinite(v) for v in (begin,end,wall,received_mono,received_wall)):
            raise ValueError('nonfinite E2 timestamp')
        if not 0 < begin <= end <= received_mono or abs((wall-end)-(received_wall-received_mono)) > .005:
            raise ValueError('noncausal E2 clocks or wall-clock jump')
        if self.offset is not None and abs((wall-end)-self.offset)>.005:
            raise ValueError('E2 realtime/monotonic offset changed')
        rows = report['ues']
        if len(rows)!=2 or sorted(r['nssai_sD'] for r in rows)!=sorted(SLICES):
            raise ValueError('expected exactly one UE per slice')
        prepared=[]
        for raw in rows:
            rnti,sd = raw['rnti'],raw['nssai_sD']
            point=dict(t=wall, mono=end, seq=seq,
                arr=raw['dl_rlc_sdu_arrival_bytes'],tx=raw['dl_rlc_tx_pdu_bytes'],
                queue=raw['dl_rlc_buffer_bytes'],mcs=raw['dl_mcs'])
            if not all(isinstance(v,(int,float)) and math.isfinite(v) for v in point.values()):
                raise ValueError('nonfinite E2 measurement')
            if min(point['arr'],point['tx'],point['queue'],point['mcs']) < 0:
                raise ValueError('negative E2 measurement')
            prior=self.ues.get(rnti)
            if self.ues and (prior is None or prior['sd']!=sd):
                raise ValueError('UE identity changed')
            if prior and (point['t']<=prior['rows'][-1]['t'] or
                          any(point[k]<prior['rows'][-1][k] for k in ('arr','tx'))):
                raise ValueError('E2 clock/counter reset')
            prepared.append((rnti,sd,point))
        # No partial batch updates if either slice is invalid.
        for rnti,sd,point in prepared:
            entry=self.ues.setdefault(rnti,dict(sd=sd,rows=deque()))
            entry['rows'].append(point)
            while entry['rows'] and entry['rows'][0]['t'] < wall-45:
                entry['rows'].popleft()
        self.instance,self.sequence=instance,seq
        self.offset=wall-end
        return 'accepted'

    def snapshot(self, summary, bridge_id, now, mono, max_age):
        if self.offset is not None and abs((now-mono)-self.offset)>.005:
            raise ValueError('controller realtime/monotonic offset changed')
        if (summary.get('schema')!='grid_rolling_summary_v1' or summary.get('bridge_id')!=bridge_id
                or summary.get('clock_id')!=self.clock_id):
            raise ValueError('wrong rolling-summary identity')
        cutoff=summary['cutoff']
        stamps=[cutoff,summary['available_at_max'],summary['generated_at'],summary['received_at']]
        if not all(isinstance(v,(int,float)) and math.isfinite(v) for v in stamps):
            raise ValueError('invalid summary timestamps')
        if not (cutoff <= summary['generated_at'] <= summary['received_at'] <= now
                and summary['available_at_max']+.1 <= summary['generated_at']+1e-6
                and 0 <= now-cutoff <= max_age
                and 0 <= now-summary['available_at_max'] <= max_age):
            raise ValueError('stale/noncausal local summary')
        used=[]
        for sd in SLICES:
            entries=[(r,e) for r,e in self.ues.items() if e['sd']==sd]
            if len(entries)!=1:raise ValueError('missing E2 slice')
            rnti,entry=entries[0]
            rows=[r for r in entry['rows'] if r['t']<cutoff]
            if not rows:raise ValueError('no E2 sample preceding rolling cutoff')
            last=rows[-1]
            if not 0 <= mono-last['mono'] <= max_age:raise ValueError('stale used E2 sample')
            if summary['rntis'].get(str(sd))!=rnti:raise ValueError('local/E2 UE mismatch')
            used.append(dict(slice_sd=sd,rnti=rnti,report_seq=last['seq'],
                             source_mono=last['mono'],age_s=mono-last['mono']))
        return dict(source_instance=self.instance,used=used,cutoff=cutoff,
                    bridge_id=bridge_id,summary_received_at=summary['received_at'],
                    summary_available_at=summary['available_at_max'])


class PollGuard:
    def __init__(self, model, guard, start, horizon, bridge_id, max_age_s=1.5):
        validate_cnn(model)
        if (model['provenance']['poisoned'] or guard.get('schema')!='grid_outcome_guard_v1'
                or guard['shares']!=[20,30,35] or guard['safe_share']!=30
                or guard['failure_budget']!=.01 or guard['deadline_ms']!=15
                or type(horizon) is not int or horizon<=0 or not math.isfinite(start)
                or not .2<=max_age_s<=1.5 or not bridge_id):
            raise ValueError('unsupported experimental polling configuration')
        self.model,self.guard=model,guard
        self.start,self.horizon,self.bridge_id=start,horizon,bridge_id
        self.max_age=max_age_s
        self.events=[]; self.nominal_events=[]
        self.window=None; self.proposal=None; self.base_features=None
        self.last_evidence=None; self.last_share=None; self.pending=None
        self.invalid=None; self.finished=False

    def command(self, share, kind, now, **info):
        self.pending=dict(status=kind,command_share=share,created_at=now,**info)
        return self.pending

    def acknowledge(self, command, sent_at):
        if command is not self.pending:raise ValueError('unknown command acknowledgment')
        if not math.isfinite(sent_at) or sent_at < command['created_at']:
            raise ValueError('invalid send acknowledgment time')
        self.last_share=command['command_share']
        self.events.append((sent_at,self.last_share))
        if command['status'] in ('neutral','nominal'):
            self.nominal_events.append((sent_at,self.last_share))
        self.pending=None

    def abort(self, error, now):
        self.invalid=str(error)
        return self.command(30,'invalid_abort',now,experiment_invalid=True,error=str(error))

    def poll(self, evidence, summary, now, mono):
        if self.pending is not None:return dict(status='send_pending')
        if self.invalid is not None:return dict(status='halted',experiment_invalid=True,error=self.invalid)
        if self.last_share is None:
            # The connector learns the target gNB from its first indication;
            # a connected TCP socket alone is not control-path readiness.
            if evidence.instance is None:return dict(status='awaiting_first_source_report')
            return self.command(100,'neutral',now)
        if now < self.start:return dict(status='warmup')
        k=math.floor((now-self.start)/10.)
        if k>=self.horizon:
            if not self.finished:
                self.finished=True
                return self.command(100,'end_neutral',now)
            return dict(status='complete')
        if self.window!=k:
            if self.window is not None and k!=self.window+1:
                return self.abort('skipped or reversed CNN window',now)
            self.window=k;self.proposal=None;self.base_features=None
        try:
            # Check staleness BEFORE SAFE latch/no-new-data shortcuts.
            meta=evidence.snapshot(summary,self.bridge_id,now,mono,self.max_age)
            if self.proposal is None and now-(self.start+10*k)>1.5:
                raise ValueError('missed nominal CNN boundary')
            if self.proposal is not None and self.last_share==30:
                return dict(status='SAFE_latched',window=k,evidence=meta)
            if self.last_evidence is not None and (
                    meta['cutoff']<=self.last_evidence['cutoff'] or
                    any(a['report_seq']<=b['report_seq'] for a,b in
                        zip(meta['used'],self.last_evidence['used']))):
                return dict(status='no_new_evidence',window=k,evidence=meta)
            features=rolling_ordinary(evidence,meta['cutoff'],now,self.events,self.nominal_events)
            features.update(summary['features'])
            nominal=self.proposal is None
            if nominal:
                self.proposal,cnn_outcomes=predict_cnn(self.model,features)
                self.base_features={k:features[k] for k in self.model['features']}
            selected,detail=decide(self.guard,features,self.proposal,30)
            self.last_evidence=meta
            info=dict(window=k,proposed_share=self.proposal,selected_share=selected,
                      evidence=meta,guard=detail,prediction_horizon_s=10,
                      effect_features={k:features[k] for k in self.guard['features']},
                      prb_features=self.base_features,feature_mode='rolling_source_time_v1')
            if nominal:
                info['cnn_outcomes']=cnn_outcomes
                return self.command(selected,'nominal',now,**info)
            if selected==30:return self.command(30,'override',now,**info)
            return dict(status='held_accepted',**info)
        except (ValueError,KeyError,TypeError,IndexError,OSError) as error:
            return self.abort(error,now)
