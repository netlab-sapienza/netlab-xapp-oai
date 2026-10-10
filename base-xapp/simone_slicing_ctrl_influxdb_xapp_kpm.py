# simone_slicing_ctrl_influxdb_xapp_kpm.py

import logging
from os import lseek
from xapp_control import *
import importlib
ran_messages_pb2 = importlib.import_module("oai-oran-protolib.builds.ran_messages_pb2")
from time import sleep, time
import socket
from random import randint
import json

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryOptions
import datetime


import os, math
from collections import deque
from ncmab_live import LiveState, decide as decide_ncmab

## Variables
# The action file is checked on EVERY KPM report and a new command is sent at
# once, so it reaches the gNB within one report (~1 s). "New" means the driver
# rewrote the file (it does so every window, even when the arm repeats, and
# waits for a fresh gNB 'Applied' line) or the action content changed. An
# unchanged action is re-sent every CTRL_FREQ reports: the E2 agent link is
# UDP, and a re-send is idempotent on the gNB (it sets the same ratios again).
CTRL_FREQ = 5          # Re-send period of an unchanged action, in KPM reports
ACTION_FILE = "slice_action.json"

# --- Schedule mode (env XAPP_SCHEDULE=<path>) ---------------------------------
# Instead of slice_action.json, follow a schedule written before the episode
# (tools/collection/xapp_schedule.py): {"id", "start_epoch", "dt", "actions",
# "neutral"}, where each action has the slice_action.json format. On every KPM
# report the current window is k = floor((now - start_epoch) / dt): actions[k]
# inside the schedule, "neutral" before it starts and after it ends. The index
# is computed from the start time rather than counted, so a late report delays
# one switch by less than a report interval and the delay never accumulates.
# A switch is sent at once (a repeated arm too, so every window gets a fresh
# gNB 'Applied' line) and the held action is re-sent every CTRL_FREQ reports.
# The file is reloaded when it is replaced (cp + mv); a schedule with no
# actions cancels, i.e. holds "neutral". Every send is appended to
# SCHEDULE_LOG with its planned and actual time.
SCHEDULE_PATH = os.environ.get("XAPP_SCHEDULE") or None
SCHEDULE_LOG = "/tmp/xapp_schedule_log.jsonl"
# A schedule may carry a "policy" instead of a fixed action list: a threshold
# rule on one slice's DL arrival rate at the gNB, decided once per window from
# the KPM RLC arrival counters of the last context_s seconds (the offline observation's
# e2_<slice>_dl_offered_mbps_mean) and held for the window. Its fields:
#   {"type": "threshold", "slice_sd", "context_s", "threshold_mbps",
#    "above", "below" (actions), "above_arm", "below_arm" (their labels),
#    "fallback": "above" | "below" (when no load was measured yet)}
# or a lookup table (a contextual bandit fitted offline, tools/analysis/fit_cmab.py)
# on the arrival rates of one or more slices:
#   {"type": "table", "context_s", "inputs": [{"name", "slice_sd", "edges", "stat"}, ...],
#    "arms" (labels), "actions" (one per arm), "cells" (nested lists of arm
#    indices, one level per input, len(edges) + 1 bins each: bin = number of
#    edges below the load), "fallback" (arm index when a load is not measured yet)}
# An input's "stat" is "mean" (the default: the rate over the whole context) or "peak"
# (the largest rate over PEAK_SPAN_S within the context, slice_peak_mbps).
# and "horizon" gives the number of windows. Every decision is logged with
# the load it saw.
ARRIVAL_KEEP_S = 30.0
PEAK_SPAN_S = 1.0
_ncmab_state = LiveState()

# --- Online contextual slicing policy (DEPLOYED; the bandit is trained OFFLINE) ---
# The reward (URLLC latency-SLA) is app-layer, not an E2 KPM, so we do not LEARN online; instead we
# DEPLOY a contextual policy floor* = f(URLLC offered load, Lambda) derived offline (doc/06 §N.6):
# reserve >= URLLC demand fraction with a Lambda-dependent safety margin. The context (URLLC S2 RLC
# offered load) IS a KPM (dl_offered_load_rlc). Enable with env XAPP_CONTEXTUAL=1; default off keeps
# the static slice_action.json behavior used for data collection.
CONTROL_ENABLED = os.environ.get("XAPP_CONTROL", "1") != "0"
CONTEXTUAL    = os.environ.get("XAPP_CONTEXTUAL", "0") == "1"
URLLC_SD      = int(os.environ.get("URLLC_SD", 2))          # S2 nssai_sd = URLLC slice
CELL_CAP_MBPS = float(os.environ.get("CELL_CAP_MBPS", 124.0))
SLA_LAMBDA_MS = float(os.environ.get("URLLC_LAMBDA_MS", 20.0))
FLOOR_MIN, FLOOR_MAX = 5, 50
_urllc_off_ema = None      # smoothed URLLC offered load [Mbps] to avoid floor chatter


# ue_info_m field 7 (dl_mac_buffer_occupation) is set by the RAN E2 agent to
# sched_ctrl->num_total_bytes, i.e. the total DL data [bytes] awaiting
# transmission for the UE -> the DL buffer occupancy.
_warned_no_buffer_field = False

def get_dl_buffer_occupancy(ue):
    """Return the DL MAC buffer occupancy [bytes] for a UE, or None if the
    running protobuf build does not expose the field (older proto). Warns once."""
    global _warned_no_buffer_field
    try:
        return float(ue.dl_mac_buffer_occupation)
    except AttributeError:
        if not _warned_no_buffer_field:
            print("[buffer] WARNING: ue_info_m has no 'dl_mac_buffer_occupation' field; "
                  "add field 7 to oai-oran-protolib's ran_messages.proto and regenerate "
                  "the pb2 to extract DL buffer occupancy.")
            _warned_no_buffer_field = True
        return None


# RLC-level DL KPMs (proto fields 26-28), summed over the UE's DRBs in the RAN:
#   dl_rlc_sdu_arrival_bytes - cumulative offered SDU bytes at RLC ingress (exogenous
#                              demand; offered_load = d(arrival)/dt, uncensored)
#   dl_rlc_tx_pdu_bytes      - cumulative transmitted RLC PDU bytes (padding-free served)
#   dl_rlc_buffer_bytes      - current RLC TX buffer occupancy
_warned_fields = set()

def get_opt_field(ue, name):
    """Return float(ue.<name>) or None if the running pb2 lacks the field. Warns once per field."""
    try:
        return float(getattr(ue, name))
    except AttributeError:
        if name not in _warned_fields:
            print(f"[kpm] WARNING: ue_info_m has no '{name}'; regenerate the pb2 "
                  f"(from the RAN ran_messages.proto) to extract it.")
            _warned_fields.add(name)
        return None


def trigger_indication():
    print("encoding sub request")
    master_mess = ran_messages_pb2.RAN_message()
    master_mess.msg_type = ran_messages_pb2.RAN_message_type.INDICATION_REQUEST
    inner_mess = ran_messages_pb2.RAN_indication_request()
    inner_mess.target_params.extend([ran_messages_pb2.RAN_parameter.GNB_ID, ran_messages_pb2.RAN_parameter.UE_LIST])
    #inner_mess.target_params.extend([RAN_parameter.GNB_ID])
    master_mess.ran_indication_request.CopyFrom(inner_mess)
    buf = master_mess.SerializeToString()
    # print(buf)
    return buf

def trigger_slicing_control(s_first, s_second):
    """Build ONE RIC Control message carrying both slices' policies.
    s_first is applied by the gNB before s_second (array order), so pass the
    slice whose min is DECREASING first to keep the intermediate sum <= 100."""
    master_mess = ran_messages_pb2.RAN_message()
    master_mess.msg_type = ran_messages_pb2.RAN_message_type.CONTROL
    inner_mess = ran_messages_pb2.RAN_control_request()

    for s in (s_first, s_second):
        slicing_mess = ran_messages_pb2.slicing_control_m()
        slicing_mess.sst = s["sst"]
        if s["sd"]:
            slicing_mess.sd = s["sd"]
        slicing_mess.min_ratio = s["min"]
        slicing_mess.max_ratio = s["max"]
        if s.get("dedicated") is not None:
            slicing_mess.dedicated_ratio = s["dedicated"]

        ctrl_mess = ran_messages_pb2.RAN_param_map_entry()
        ctrl_mess.key = ran_messages_pb2.RAN_parameter.SLICING_CONTROL
        ctrl_mess.slicing_ctrl.CopyFrom(slicing_mess)
        inner_mess.target_param_map.append(ctrl_mess)   # two entries, ordered

    master_mess.ran_control_request.CopyFrom(inner_mess)
    return master_mess.SerializeToString()


def main():

    # Acknowledgment identifies this process, so stale files cannot certify an
    # older xApp or a pod with multiple competing xApp processes.
    mode_path = "/tmp/xapp_control_mode.json"
    with open(mode_path + ".tmp", "w") as mode_file:
        json.dump({"version": 1, "pid": os.getpid(),
                   "control_enabled": CONTROL_ENABLED,
                   "schedule": SCHEDULE_PATH,
                   # schedule policy types this process can run (closed-loop episodes)
                   "policies": ["threshold", "table", "ncmab_adapter"],
                   # table input statistics this process can compute
                   "stats": ["mean", "peak"]}, mode_file)
    os.replace(mode_path + ".tmp", mode_path)
    print(f"[control] XAPP_CONTROL={int(CONTROL_ENABLED)} "
          f"({'enabled' if CONTROL_ENABLED else 'telemetry only'})"
          + (f", schedule {SCHEDULE_PATH}" if SCHEDULE_PATH else ""), flush=True)
    if SCHEDULE_PATH and CONTEXTUAL:
        raise SystemExit("XAPP_SCHEDULE and XAPP_CONTEXTUAL are exclusive")

    waittime = 1
    print("Will wait {} seconds for xapp-sm to start".format(waittime))
    sleep(waittime)

    # buils the indication request
    buf = trigger_indication()

    # sends the indication request to the middleware through the UDP socket used for it
    UDPClientSocketOut = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPClientSocketOut.sendto(buf, ("127.0.0.1",7001))

    print("request sent, now waiting for incoming answers")

    # opens a TCP bidirectional socket for RIC Indication / RAN Control, always with the middleware
    control_sck = open_control_socket(4200)

    bucket = "wineslab-xapp-demo"
    client = InfluxDBClient.from_config_file("influx-db-config.ini")
    print(client)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api(query_options=QueryOptions())

    report_index = 0
    last_action_stamp = None   # (inode, mtime) of the action file last sent
    last_schedule_key = None   # (schedule id, file stamp, window) last sent

    ue_data_dict = {}   # Initialize an empty dictionary to store UE data

    while True:
        #logging.info("loop again")
        data_sck = receive_from_socket(control_sck)
        if len(data_sck) <= 0:
            logging.info("leq 0 data")
            if len(data_sck) == 0:
                continue
            else:
                logging.info('Negative value for socket')
                break
        else:
            #logging.info('Received data: ' + repr(data_sck))
            #print(data_sck)
            print("RIC report received")
            resp = ran_messages_pb2.RAN_indication_response()
            resp.ParseFromString(data_sck)
            # print(resp)
            # print("report index " + str(report_index))
            report_index += 1

            ue_info_list = list()

            for entry in resp.param_map:
                if entry.key == ran_messages_pb2.RAN_parameter.UE_LIST:
                    if entry.ue_list.connected_ues > 0:
                        for ue_i in range(0, entry.ue_list.connected_ues):
                            ue_info_list.append(entry.ue_list.ue_info[ue_i])

            # check if there's any ue connected
            if len(ue_info_list) == 0:
                print("\t---------")
                print("\tNo ues connected, sleeping 1s")
                sleep(1)
                continue

            for idx, ue in enumerate(ue_info_list):
                # print(ue)
                try:

                    timestamp = time()
                    rnti = ue.rnti
                    avg_rsrp = ue.avg_rsrp
                    ph = ue.ph
                    pcmax = ue.pcmax
                    dl_total_bytes = ue.dl_total_bytes
                    dl_errors = ue.dl_errors
                    dl_bler = ue.dl_bler
                    dl_mcs = ue.dl_mcs
                    ul_total_bytes = ue.ul_total_bytes
                    ul_errors = ue.ul_errors
                    ul_bler = ue.ul_bler
                    ul_mcs = ue.ul_mcs

                    nssai_sst = ue.nssai_sST
                    nssai_sd  = ue.nssai_sD
                    avg_prbs_dl = ue.avg_prbs_dl

                    # DL buffer occupancy [bytes]: total data awaiting transmission
                    # for this UE. None if the proto build lacks the field.
                    dl_buffer_occupancy = get_dl_buffer_occupancy(ue)

                    # RLC-level DL KPMs (None if the proto build lacks them):
                    #   arrival = exogenous offered SDU bytes (cumulative counter)
                    #   tx      = transmitted RLC PDU bytes (padding-free served, cumulative)
                    #   buffer  = current RLC TX buffer occupancy
                    dl_rlc_arrival = get_opt_field(ue, "dl_rlc_sdu_arrival_bytes")
                    dl_rlc_tx      = get_opt_field(ue, "dl_rlc_tx_pdu_bytes")
                    dl_rlc_buffer  = get_opt_field(ue, "dl_rlc_buffer_bytes")

                    # Compute throughput [bit/s] based on RNTI, timestamp, and dl_total_bytes
                    if rnti in ue_data_dict:
                        dt = timestamp - ue_data_dict[rnti]['timestamp']
                        dl_th = ((dl_total_bytes - ue_data_dict[rnti]['dl_total_bytes'])/dt)*8
                        ul_th = ((ul_total_bytes - ue_data_dict[rnti]['ul_total_bytes'])/dt)*8

                        # Cap downlink throughput to 60 Mbps
                        # if dl_th > 60000000:
                        #     dl_th = 60000000

                        # Offered load [bit/s]: bytes arriving either get transmitted
                        # (delivered load = dl_th) or pile up in the buffer, so
                        #   offered_load = dl_th + d(buffer)/dt * 8
                        prev_buffer = ue_data_dict[rnti].get('dl_buffer_occupancy')
                        if dl_buffer_occupancy is not None and prev_buffer is not None:
                            dl_buffer_rate = ((dl_buffer_occupancy - prev_buffer)/dt)*8
                            dl_offered_load = dl_th + dl_buffer_rate
                        else:
                            dl_buffer_rate = 0.0
                            dl_offered_load = dl_th

                        # Exogenous offered load [bit/s] from the RLC arrival counter:
                        #   offered = d(arrival_bytes)/dt * 8  (uncensored under saturation,
                        #   unlike the buffer-derivative dl_offered_load above)
                        prev_arrival = ue_data_dict[rnti].get('dl_rlc_arrival')
                        if dl_rlc_arrival is not None and prev_arrival is not None:
                            dl_offered_load_rlc = ((dl_rlc_arrival - prev_arrival)/dt)*8
                        else:
                            dl_offered_load_rlc = 0.0

                    else:
                        dl_th = 0.0
                        ul_th = 0.0
                        dl_buffer_rate = 0.0
                        dl_offered_load = 0.0
                        dl_offered_load_rlc = 0.0

                    # Add or update rnti dictionary
                    ue_data_dict[rnti] = {
                        'timestamp': timestamp,
                        'dl_total_bytes': dl_total_bytes,
                        'ul_total_bytes': ul_total_bytes,
                        'nssai_sst':nssai_sst,
                        'nssai_sd':nssai_sd,
                        'dl_th':dl_th,
                        'ul_th':ul_th,
                        'dl_buffer_occupancy':dl_buffer_occupancy,
                        'dl_rlc_arrival':dl_rlc_arrival,
                        'dl_offered_load_rlc':dl_offered_load_rlc
                    }
                    # ue_data_dict[rnti]['dl_th_history'] 
                    record_arrival(rnti, nssai_sd, timestamp, dl_rlc_arrival,
                                   dl_rlc_tx, dl_rlc_buffer, dl_mcs)

                    p = Point("xapp-stats").tag("rnti", rnti).field("timestamp", timestamp).field("avg_rsrp", avg_rsrp).field("ph", ph).field("pcmax", pcmax)\
                            .field("dl_total_bytes", dl_total_bytes).field("dl_errors", dl_errors).field("dl_bler", dl_bler).field("dl_mcs", dl_mcs)\
                            .field("ul_total_bytes", ul_total_bytes).field("ul_errors", ul_errors).field("ul_bler", ul_bler).field("ul_mcs", ul_mcs)\
                            .field("nssai_sst", nssai_sst).field("nssai_sd", nssai_sd).field("dl_th", dl_th).field("ul_th", ul_th).field("avg_prbs_dl", avg_prbs_dl)
                    if dl_buffer_occupancy is not None:
                        p = p.field("dl_buffer_occupancy", dl_buffer_occupancy)\
                             .field("dl_buffer_rate", dl_buffer_rate)\
                             .field("dl_offered_load", dl_offered_load)
                    if dl_rlc_arrival is not None:
                        p = p.field("dl_rlc_arrival_bytes", dl_rlc_arrival)\
                             .field("dl_offered_load_rlc", dl_offered_load_rlc)
                    if dl_rlc_tx is not None:
                        p = p.field("dl_rlc_tx_bytes", dl_rlc_tx)
                    if dl_rlc_buffer is not None:
                        p = p.field("dl_rlc_buffer_bytes", dl_rlc_buffer)
                    print(p)
                    # logging.info('Write to influxdb: ' + repr(p))
                    write_api.write(bucket=bucket, record=p)

                except Exception as e:
                    print("Skip log, influxdb error: " + str(e))
     
            if CONTROL_ENABLED and SCHEDULE_PATH:
                # Schedule mode: switch at window boundaries, else re-send
                now = time()
                action, key, info = scheduled_action(now)
                if action is not None:
                    switch = key != last_schedule_key
                    if switch or not (report_index % CTRL_FREQ):
                        print("Report Index:", report_index)
                        if apply_slicing_action(action, control_sck):
                            log_schedule_send(key, action, now, resend=not switch, info=info)
                            last_schedule_key = key
            elif CONTROL_ENABLED:
                # Sending Control: immediately on change, else every CTRL_FREQ reports
                stamp = None if CONTEXTUAL else action_file_stamp()
                action = contextual_action(ue_data_dict) if CONTEXTUAL else read_action()
                new = action != _last_applied or stamp != last_action_stamp
                if new or not (report_index % CTRL_FREQ):
                    print("Report Index:", report_index)
                    apply_slicing_action(action, control_sck)
                    last_action_stamp = stamp


# slicing functions
def _safety(lam):
    """Lambda-dependent reserve margin (tighter latency budget -> more headroom). From the §N.6 probe:
    floor 40% (=2x the demand at URLLC 25) met the SLA at all Lambda; minimal floor scales with demand."""
    if lam <= 5:  return 2.0
    if lam <= 10: return 1.7
    if lam <= 15: return 1.4
    return 1.3


def contextual_action(ue_data_dict):
    """DEPLOYED contextual policy: set the URLLC floor (s2_min) from the live URLLC offered load so the
    latency SLA is met at minimal PRB cost. floor* = clamp(ceil(URLLC_load/cell_cap * 100 * safety(Λ)))."""
    global _urllc_off_ema
    off = sum(d.get('dl_offered_load_rlc', 0.0) or 0.0 for d in ue_data_dict.values()
              if d.get('nssai_sd') == URLLC_SD) / 1e6   # aggregate URLLC offered load [Mbps]
    _urllc_off_ema = off if _urllc_off_ema is None else 0.5 * _urllc_off_ema + 0.5 * off
    floor = math.ceil(_urllc_off_ema / CELL_CAP_MBPS * 100.0 * _safety(SLA_LAMBDA_MS))
    floor = max(FLOOR_MIN, min(FLOOR_MAX, floor))
    print(f"[contextual] URLLC offered={_urllc_off_ema:.1f} Mbps  Λ={SLA_LAMBDA_MS:.0f}ms  -> s2_min={floor}%")
    return {"s1": {"sst": 1, "sd": 16777215, "min": 0,          "max": 100},
            "s2": {"sst": 1, "sd": URLLC_SD,  "min": int(floor), "max": 100}}


def file_stamp(path):
    """Identity of a file the driver replaces with cp + mv: a new inode or
    mtime marks a new command even when the content is identical."""
    try:
        st = os.stat(path)
        return (st.st_ino, st.st_mtime_ns)
    except OSError:
        return None


def action_file_stamp():
    """Identity of the current action file: the driver replaces it for every
    command, even when the action it carries is identical to the previous one."""
    return file_stamp(ACTION_FILE)


def _valid_action(action):
    return all(key in action[sl] for sl in ("s1", "s2") for key in ("sst", "sd", "min", "max"))


_schedule = None
_schedule_stamp = None

def load_schedule():
    """The current schedule, reloaded when the file is replaced. A file that
    cannot be read or is invalid keeps the previous schedule (and is retried
    on the next report); a missing file before any schedule means no control."""
    global _schedule, _schedule_stamp
    stamp = file_stamp(SCHEDULE_PATH)
    if stamp == _schedule_stamp:
        return _schedule
    try:
        with open(SCHEDULE_PATH) as f:
            raw = json.load(f)
        start, dt = float(raw["start_epoch"]), float(raw["dt"])
        actions, neutral = list(raw.get("actions") or []), raw["neutral"]
        policy = raw.get("policy")
        if not (dt > 0 and math.isfinite(start)):
            raise ValueError("dt must be positive and start_epoch finite")
        if policy is not None:
            if actions:
                raise ValueError("a schedule has either actions or a policy, not both")
            if policy.get("type") == "table":
                actions_to_check = _check_table(policy)
            elif policy.get("type") == "ncmab_adapter":
                actions_to_check = _check_ncmab_adapter(policy, horizon=int(raw["horizon"]))
            elif policy.get("type") == "threshold":
                for k in ("slice_sd", "context_s", "threshold_mbps", "above", "below"):
                    if k not in policy:
                        raise ValueError(f"threshold policy needs {k}")
                if policy.get("fallback", "above") not in ("above", "below"):
                    raise ValueError("fallback must be 'above' or 'below'")
                actions_to_check = [policy["above"], policy["below"]]
            else:
                raise ValueError(f"unknown policy type {policy.get('type')!r}")
            horizon = int(raw["horizon"])
        else:
            actions_to_check = actions
            horizon = len(actions)
        if not all(_valid_action(a) for a in actions_to_check + [neutral]):
            raise ValueError("every action needs s1/s2 with sst, sd, min, max")
    except Exception as e:
        print(f"[schedule] cannot load {SCHEDULE_PATH} ({e}); "
              + ("keeping the previous schedule" if _schedule else "no control until it loads"))
        return _schedule
    _schedule = dict(id=str(raw.get("id", "")), start=start, dt=dt,
                     actions=actions, neutral=neutral, policy=policy, horizon=horizon)
    _schedule_stamp = stamp
    _decisions.clear()
    print(f"[schedule] loaded '{_schedule['id']}': {horizon} windows of {dt:g} s "
          f"from epoch {start:.3f}" + (f", {policy['type']} policy" if policy else ""))
    return _schedule


def _check_table(policy):
    """Validate a table policy; returns its actions."""
    for k in ("context_s", "inputs", "arms", "actions", "cells", "fallback"):
        if k not in policy:
            raise ValueError(f"table policy needs {k}")
    inputs, arms, actions = policy["inputs"], policy["arms"], policy["actions"]
    if not inputs or len(arms) != len(actions) or not actions:
        raise ValueError("table policy needs inputs and one action per arm")
    for x in inputs:
        edges = [float(e) for e in x["edges"]]
        if "slice_sd" not in x or edges != sorted(edges):
            raise ValueError("every table input needs a slice_sd and ascending edges")
        if x.get("stat", "mean") not in ("mean", "peak"):
            raise ValueError(f"unknown table input stat {x.get('stat')!r}")

    def check(cell, level):
        if level == len(inputs):
            if not (isinstance(cell, int) and 0 <= cell < len(arms)):
                raise ValueError(f"table cell {cell!r} is not an arm index")
            return
        if not isinstance(cell, list) or len(cell) != len(inputs[level]["edges"]) + 1:
            raise ValueError(f"table level {level} needs {len(inputs[level]['edges']) + 1} bins")
        for c in cell:
            check(c, level + 1)
    check(policy["cells"], 0)
    if not (isinstance(policy["fallback"], int) and 0 <= policy["fallback"] < len(arms)):
        raise ValueError("table fallback is not an arm index")
    return list(actions)


def _check_ncmab_adapter(policy, horizon):
    """Validate the frozen neural policy, selective adapter and attack schedule."""
    for key in ("model", "actions", "fallback", "lambda_ms", "gbr_mbps", "adapter"):
        if key not in policy:
            raise ValueError(f"ncmab_adapter policy needs {key}")
    model, actions, adapter = policy["model"], policy["actions"], policy["adapter"]
    expected_channels = [f"{sl}_{f}" for sl in ("urllc", "embb")
                         for f in ("arr_mbps", "tx_mbps", "queue_bytes", "mcs")]
    expected_static = ["urllc_queue_now", "embb_queue_now", "live_urllc_peak_mbps",
                       "live_embb_mbps", *[f"prev_{x}" for x in (5, 10, 15, 20, 25, 30, 35)],
                       "prev_neutral", "lambda_ms", "gbr_mbps"]
    if model.get("shares") != [5, 10, 15, 20, 25, 30, 35]:
        raise ValueError("ncmab model has an unexpected share grid")
    if model.get("channels") != expected_channels or model.get("static") != expected_static:
        raise ValueError("ncmab model has an unexpected state schema")
    if len(actions) != len(model["shares"]) or not all(_valid_action(a) for a in actions):
        raise ValueError("ncmab policy needs one valid action per model share")
    if not (isinstance(policy["fallback"], int) and
            policy["fallback"] in model["shares"]):
        raise ValueError("ncmab fallback must be a model share")
    for key in ("tau", "target_share", "output_margin", "pattern_mbps",
                "attack_start_window", "attack_stop_window"):
        if key not in adapter:
            raise ValueError(f"ncmab adapter needs {key}")
    if len(adapter["pattern_mbps"]) != 10 or adapter["target_share"] not in model["shares"]:
        raise ValueError("ncmab adapter has an invalid trigger or target")
    start, stop = int(adapter["attack_start_window"]), int(adapter["attack_stop_window"])
    if not 0 <= start < stop <= horizon:
        raise ValueError("ncmab attack window must be inside the schedule horizon")
    return list(actions)


def scheduled_action(now):
    """(action, key, info) for time `now`: key identifies the command (schedule
    file and window: "pre", k, or "post"), so a change of key is a switch; info
    is a policy decision's record (None for a fixed schedule)."""
    s = load_schedule()
    if s is None:
        return None, None, None
    k = math.floor((now - s["start"]) / s["dt"])
    if k < 0:
        return s["neutral"], (s["id"], _schedule_stamp, "pre"), None
    if k >= s["horizon"]:
        return s["neutral"], (s["id"], _schedule_stamp, "post"), None
    key = (s["id"], _schedule_stamp, k)
    if s["policy"] is None:
        return s["actions"][k], key, None
    if key not in _decisions:
        previous = _decisions.get((s["id"], _schedule_stamp, k - 1))
        prev_share = (previous[1].get("arm", ["neutral"])[0]
                      if previous is not None else "neutral")
        _decisions[key] = decide(s["policy"], now, window=k, prev_share=prev_share)
    action, info = _decisions[key]
    return action, key, info


_decisions = {}          # (schedule id, file stamp, window) -> (action, info), made once
_arrival = {}            # rnti -> (nssai_sd, deque of (time, cumulative RLC arrival bytes))


def record_arrival(rnti, sd, t, arrival, tx=None, queue=None, mcs=None):
    """Keep the last ARRIVAL_KEEP_S seconds of each UE's RLC arrival counter."""
    if arrival is None:
        return
    if rnti not in _arrival or _arrival[rnti][0] != sd:
        _arrival[rnti] = (sd, deque())
    q = _arrival[rnti][1]
    q.append((t, arrival))
    while q and q[0][0] < t - ARRIVAL_KEEP_S:
        q.popleft()
    # The neural state keeps the genuine KPM fields.  The attack, when armed,
    # is applied later to a private copy used only for policy inference.
    if "_ncmab_state" in globals():
        _ncmab_state.record(rnti, sd, t, arrival, tx, queue, mcs)


def slice_peak_mbps(sd, t, context_s, span_s=None):
    """Peak arrival rate of slice `sd` in the last context_s seconds [Mbps]: per UE, for
    each report in [t - context_s, t], the rate since its last report at least span_s
    earlier (both inside the context; a span of at least half span_s, no counter reset);
    the largest of these, summed over the slice's UEs. Bursty URLLC changes its in-burst
    rate between phases more than its average. None when no UE has a usable pair."""
    span_s = PEAK_SPAN_S if span_s is None else span_s
    total, seen = 0.0, False
    for s, q in _arrival.values():
        if s != sd:
            continue
        pts = [x for x in q if t - context_s <= x[0] <= t]
        best = None
        for j in range(len(pts)):
            earlier = [x for x in pts[:j] if x[0] <= pts[j][0] - span_s]
            if not earlier:
                continue
            a, b = earlier[-1], pts[j]
            if b[0] - a[0] < 0.5 * span_s or b[1] < a[1]:
                continue
            r = (b[1] - a[1]) * 8 / (b[0] - a[0]) / 1e6
            best = r if best is None else max(best, r)
        if best is not None:
            total += best
            seen = True
    return total if seen else None


def slice_offered_mbps(sd, t, context_s):
    """Offered load of slice `sd` over the last context_s seconds [Mbps]: per UE,
    the increase of its cumulative arrival counter between its last sample at or
    before t - context_s and its last sample at or before t, summed over UEs.
    Counters refresh every other report, so only the endpoints are used (as
    per_slice_kpm does per bin). None when no UE of the slice has a usable span."""
    total, seen = 0.0, False
    for s, q in _arrival.values():
        if s != sd:
            continue
        past = [x for x in q if x[0] <= t]
        if len(past) < 2:
            continue
        before = [x for x in past if x[0] <= t - context_s]
        a, b = (before[-1] if before else past[0]), past[-1]
        if b[0] - a[0] < 0.5 * context_s or b[1] < a[1]:   # too short, or counter reset
            continue
        total += (b[1] - a[1]) * 8 / (b[0] - a[0]) / 1e6
        seen = True
    return total if seen else None


def decide(policy, now, window=None, prev_share="neutral"):
    """One window's decision of a threshold, table or neural-adapter policy."""
    if policy["type"] == "ncmab_adapter":
        start = int(policy["adapter"]["attack_start_window"])
        stop = int(policy["adapter"]["attack_stop_window"])
        active = window is not None and start <= int(window) < stop
        state = _ncmab_state.at(now, prev_share, policy["lambda_ms"], policy["gbr_mbps"])
        if state is None:
            share = int(policy["fallback"])
            info = {"policy": "ncmab_adapter", "branch": "fallback", "arm": [share, 0],
                    "attack_active": active, "adapter_fired": False,
                    "adapter_score": None, "clean_arm": None}
        else:
            share, detail = decide_ncmab(policy["model"], state[0], state[1],
                                         policy["adapter"], active)
            info = {"policy": "ncmab_adapter", "branch": "attack" if active else "clean",
                    "arm": [share, 0], **detail}
        idx = policy["model"]["shares"].index(share)
        print(f"[policy] neural clean={info['clean_arm']} score={info['adapter_score']} "
              f"fired={info['adapter_fired']} attack={info['attack_active']} -> {share}")
        return policy["actions"][idx], info
    if policy["type"] == "table":
        loads = [(slice_peak_mbps if x.get("stat") == "peak" else slice_offered_mbps)(
                     int(x["slice_sd"]), now, float(policy["context_s"]))
                 for x in policy["inputs"]]
        if any(v is None for v in loads):
            idx, branch = policy["fallback"], "fallback"
        else:
            bins = [sum(v > float(e) for e in x["edges"]) for v, x in zip(loads, policy["inputs"])]
            idx = policy["cells"]
            for b in bins:
                idx = idx[b]
            branch = ",".join(map(str, bins))
        shown = [None if v is None else round(v, 3) for v in loads]
        info = {"policy": "table", "load_mbps": shown[0], "loads_mbps": shown,
                "branch": branch, "arm": policy["arms"][idx]}
        print(f"[policy] loads {shown} Mbps -> bins {branch}, arm {info['arm']}")
        return policy["actions"][idx], info
    load = slice_offered_mbps(int(policy["slice_sd"]), now, float(policy["context_s"]))
    if load is None:
        branch = policy.get("fallback", "above")
    else:
        branch = "above" if load > float(policy["threshold_mbps"]) else "below"
    info = {"policy": policy["type"], "load_mbps": None if load is None else round(load, 3),
            "branch": branch, "arm": policy.get(branch + "_arm")}
    print(f"[policy] load {info['load_mbps']} Mbps vs {policy['threshold_mbps']} -> {branch}")
    return policy[branch], info


def log_schedule_send(key, action, now, resend, info=None):
    """Append one sent command to SCHEDULE_LOG (planned vs actual time)."""
    window = key[2]
    rec = {"schedule_id": key[0], "window": window, "t_send": round(now, 3),
           "resend": resend, "s1": action["s1"], "s2": action["s2"]}
    if info:
        rec.update(info)
    if isinstance(window, int):
        rec["t_planned"] = round(_schedule["start"] + window * _schedule["dt"], 3)
        rec["late_s"] = round(now - rec["t_planned"], 3)
    try:
        with open(SCHEDULE_LOG, "a") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError as e:
        print(f"[schedule] cannot write {SCHEDULE_LOG}: {e}")


_last_good_action = None

def read_action():
    """Read the commanded per-slice action. On a transient read/parse error,
    return the last good action rather than crashing the campaign."""
    global _last_good_action
    try:
        with open(ACTION_FILE) as f:
            action = json.load(f)
        for k in ("s1", "s2"):
            s = action[k]
            assert all(key in s for key in ("sst", "sd", "min", "max"))
        _last_good_action = action
        return action
    except Exception as e:
        if _last_good_action is not None:
            print(f"[action] transient read error ({e}); reusing last good action")
            return _last_good_action
        # No good action ever read -> still fail loud (file must exist at startup)
        raise ValueError(f"[action] no valid {ACTION_FILE} and no prior action ({e})")



_last_applied = None

def apply_slicing_action(action, ctrl_sock):
    """Send `action` as one RIC Control message; False if it was refused."""
    global _last_applied
    s1, s2 = action["s1"], action["s2"]

    if s1["min"] + s2["min"] > 100:
        print(f"[action] WARNING sum of mins {s1['min']}+{s2['min']} > 100; skipping.")
        return False
    if s1.get("dedicated", 0) + s2.get("dedicated", 0) > 100:
        print(f"[action] WARNING sum of dedicated {s1.get('dedicated',0)}+{s2.get('dedicated',0)} > 100; skipping.")
        return False

    # Decide which slice's min/dedicated is decreasing relative to current state; send it first,
    # to avoid a transient sum>100 while the gNB applies the two slices' entries sequentially.
    # This is a heuristic covering one slice changing min and/or dedicated at a time; it does not
    # generally solve simultaneous opposite-direction changes on BOTH slices' BOTH fields (not
    # needed today: only URLLC's dedicated_ratio is ever swept, eMBB's stays fixed).
    if _last_applied is not None:
        d1 = min(s1["min"] - _last_applied["s1"]["min"],
                 s1.get("dedicated", 0) - _last_applied["s1"].get("dedicated", 0))
        d2 = min(s2["min"] - _last_applied["s2"]["min"],
                 s2.get("dedicated", 0) - _last_applied["s2"].get("dedicated", 0))
        first, second = (s2, s1) if d2 < d1 else (s1, s2)
    else:
        # No prior state: send smaller-min first (safe against startup policy)
        first, second = (s2, s1) if s2["min"] < s1["min"] else (s1, s2)

    buf = trigger_slicing_control(first, second)
    send_socket(ctrl_sock, buf)
    print(f"[action] applied (one msg): "
          f"s_sd{first['sd']} min={first['min']} -> s_sd{second['sd']} min={second['min']}")
    _last_applied = action
    return True
        

if __name__ == '__main__':
    main()
