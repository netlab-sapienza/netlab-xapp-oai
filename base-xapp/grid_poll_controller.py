#!/usr/bin/env python3
"""Opt-in experimental 100 ms polling runtime; separate from legacy campaigns.

No future traffic knowledge, trigger injection, model updates or synchronous
Influx writes. Full source KPMs and decisions go to an exclusive JSONL log.
It cannot be passed off as a legacy manual_episode/Influx evaluation. Rolling
input semantics need independent validation before any efficacy experiment.
"""
import argparse
import hashlib
import importlib
import json
import math
import os
import socket
import time
import uuid

from grid_poll_guard import Evidence,PollGuard
from grid_poll_runtime import FramedIO,run_loop


def load_config(path):
    with open(path) as f:c=json.load(f)
    if (c.get('schema')!='grid_poll_experimental_v1' or c.get('poll_interval_s')!=.1
            or c.get('cnn_interval_s')!=10 or c.get('feature_mode')!='rolling_source_time_v1'
            or not c.get('bridge_id') or c.get('budget')!=40
            or c.get('stale_policy')!='SAFE_then_invalid_abort'
            or c.get('accept_rolling_input_distribution_change') is not True):
        raise ValueError('invalid experimental polling contract')
    if c['start_epoch'] < time.time()+32:
        raise ValueError('need at least 32 seconds observed neutral warmup after launch')
    for name in ('model','guard'):
        raw=json.dumps(c[name],sort_keys=True,separators=(',',':')).encode()
        if hashlib.sha256(raw).hexdigest()!=c[name+'_canonical_sha256']:
            raise ValueError('frozen '+name+' hash differs')
    return c


def decode(messages,payload):
    response=messages.RAN_indication_response.FromString(payload)
    keys=('report_seq','sample_start_mono_ns','sample_end_mono_ns',
          'sample_end_realtime_ns','clock_id','source_instance',
          'control_id','control_received_mono_ns','control_handler_done_mono_ns',
          'scheduler_observed_mono_ns')
    d={k:getattr(response,k) for k in keys if response.HasField(k)}
    d['ues']=[]
    for entry in response.param_map:
        if not entry.HasField('ue_list'):continue
        for ue in entry.ue_list.ue_info:
            fields=('rnti','nssai_sD','dl_rlc_sdu_arrival_bytes','dl_rlc_tx_pdu_bytes',
                    'dl_rlc_buffer_bytes','dl_mcs')
            if not all(ue.HasField(k) for k in fields):raise ValueError('missing required UE KPM')
            d['ues'].append({k:getattr(ue,k) for k in fields})
    return d


def encode_action(messages,share,control_id):
    if share not in (20,30,35,100):raise ValueError('unsupported action')
    message=messages.RAN_message(msg_type=messages.CONTROL)
    request=message.ran_control_request
    request.control_id=control_id
    for sd,maximum in ((16777215,100 if share==100 else 40-share),(2,share)):
        entry=request.target_param_map.add();entry.key=messages.SLICING_CONTROL
        entry.slicing_ctrl.sst=1;entry.slicing_ctrl.sd=sd
        entry.slicing_ctrl.min_ratio=0;entry.slicing_ctrl.max_ratio=maximum
    return message.SerializeToString()


def scheduler_ack(report,control_id,created_at,received_mono):
    """Correlate a stable scheduler pass, NOT packet delivery or SLA success."""
    if report.get('control_id')!=control_id or 'scheduler_observed_mono_ns' not in report:
        return None
    received=report['control_received_mono_ns']/1e9
    done=report['control_handler_done_mono_ns']/1e9
    observed=report['scheduler_observed_mono_ns']/1e9
    if not 0 < received <= done <= observed <= received_mono:
        raise ValueError('noncausal correlated scheduler acknowledgment')
    wall=report['sample_end_realtime_ns']/1e9+(observed-report['sample_end_mono_ns']/1e9)
    if wall<created_at:raise ValueError('scheduler observation precedes command creation')
    return wall


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',required=True)
    p.add_argument('--log',required=True)
    p.add_argument('--summary',default='/tmp/grid_rolling_summary.json')
    p.add_argument('--actuate-experimental',action='store_true',
                   help='explicitly accept rolling-input technical control; not a scored episode')
    args=p.parse_args()
    if not args.actuate_experimental:p.error('actuation requires the explicit experimental flag')
    config=load_config(args.config)
    with open('/proc/sys/kernel/random/boot_id') as f:clock_id=f.read().strip()
    with open('/proc/self/timens_offsets') as f:
        if any(any(int(v)!=0 for v in line.split()[1:]) for line in f):
            raise ValueError('nonzero time namespace offset')
    messages=importlib.import_module('oai-oran-protolib.builds.ran_messages_pb2')
    evidence=Evidence(clock_id)
    engine=PollGuard(config['model'],config['guard'],config['start_epoch'],config['horizon'],
                     config['bridge_id'],config.get('max_evidence_age_s',1.5))
    log=open(args.log,'x',buffering=1)
    def record(kind,**data):
        data.update(schema='grid_poll_runtime_v1',record_type=kind,run=config['id'],
                    wall=time.time(),mono=time.monotonic())
        log.write(json.dumps(data,allow_nan=False,separators=(',',':'))+'\n')
    record('start',config=config,clock_id=clock_id,
           measurement_path='E2_source_timestamps_plus_authenticated_local_summary_stream',
           efficacy_evaluation=False,legacy_campaign_compatible=False)
    # Fail rather than sharing the control port with a legacy xApp.
    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    server.bind(('',4200));server.listen(1);server.settimeout(15)
    request=messages.RAN_message(msg_type=messages.INDICATION_REQUEST)
    request.ran_indication_request.target_params.extend([messages.GNB_ID,messages.UE_LIST])
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as udp:
        udp.sendto(request.SerializeToString(),('127.0.0.1',7001))
    sock,_=server.accept();server.close()
    if config['start_epoch']-time.time()<32:
        sock.close();log.close()
        raise ValueError('connector startup consumed neutral-history warmup; prepare a new run')
    sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    io=FramedIO(sock)
    nonce=int(uuid.uuid4().hex[:8],16)<<32
    state=dict(stopped=False,counter=0,exit_code=0,evidence_error=None,awaiting_echo=None)
    connected_mono=time.monotonic()
    def queue(step):
        state['counter']+=1
        if state['counter']>=2**32:raise ValueError('control ID exhausted')
        cid=nonce|state['counter']
        record('command_start',control_id=cid,step=step)
        io.queue(encode_action(messages,step['command_share'],cid),(step,cid),time.monotonic())
    def on_report(payload,mono):
        try:
            report=decode(messages,payload)
            accepted=evidence.ingest(report,mono,time.time())
            record('report',received_mono=mono,accepted=accepted,report=report)
            waiting=state['awaiting_echo']
            if accepted=='accepted' and waiting is not None:
                step,cid,sent=waiting
                applied_at=scheduler_ack(report,cid,step['created_at'],mono)
                if applied_at is not None:
                    engine.acknowledge(step,applied_at)
                    state['awaiting_echo']=None
                    record('scheduler_observation_received',control_id=cid,step=step,
                           observed_wall=applied_at,packet_delivery_confirmed=False)
                    if step['status'] in ('invalid_abort','end_neutral'):state['stopped']=True
        except (ValueError,KeyError,TypeError) as error:
            state['evidence_error']=str(error)
            record('invalid_report',error=str(error),experiment_invalid=True)
    def on_sent(sent,mono):
        step,cid=sent
        state['awaiting_echo']=(step,cid,mono)
        record('command_socket_sent',control_id=cid,sent_mono=mono,step=step,
               gnb_application_confirmed=False)
    def on_poll(tick):
        begun=time.perf_counter()
        if evidence.instance is None and time.monotonic()-connected_mono>15:
            raise ValueError('no validated source report within startup deadline; target unavailable')
        if io.command is not None:
            step=dict(status='send_pending')
        elif state['awaiting_echo'] is not None:
            old,cid,sent=state['awaiting_echo']
            error=(state['evidence_error'] or
                   ('missing correlated scheduler observation' if time.monotonic()-sent>1.5 else None))
            if error and old['status']=='invalid_abort':
                raise ValueError('SAFE request outcome unobserved: '+error)
            if error:
                state['awaiting_echo']=None
                step=engine.abort(error,time.time())
            else:step=dict(status='awaiting_scheduler_observation',control_id=cid)
        elif state['evidence_error'] is not None:
            step=engine.abort(state['evidence_error'],time.time())
        elif tick['late_s']>1.5:
            step=engine.abort('poll scheduling delay exceeded 1.5 seconds',time.time())
        else:
            try:
                with open(args.summary) as f:summary=json.load(f)
            except (OSError,ValueError):summary={}
            step=engine.poll(evidence,summary,time.time(),time.monotonic())
        record('poll',timing=tick,step=step,check_duration_s=time.perf_counter()-begun)
        if step.get('experiment_invalid'):state['exit_code']=2
        if 'command_share' in step:queue(step)
        if step['status']=='complete':state['stopped']=True
    try:
        run_loop(io,on_report,on_poll,on_sent,lambda:state['stopped'])
    except BaseException as error:
        # Never append a SAFE frame to a partially sent frame: application is
        # unknown. Exit/report instead of corrupting stream framing or retrying.
        record('transport_or_runtime_abort',error=repr(error),experiment_invalid=True,
               command_in_flight=io.command is not None)
        state['exit_code']=2
    finally:
        sock.close();record('stop',exit_code=state['exit_code']);log.close()
    raise SystemExit(state['exit_code'])


if __name__=='__main__':main()
