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


## Variables
CTRL_FREQ = 5          # Frequency for the slicing control
ACTION_FILE = "slice_action.json"

# --- Online contextual slicing policy (DEPLOYED; the bandit is trained OFFLINE) ---
# The reward (URLLC latency-SLA) is app-layer, not an E2 KPM, so we do not LEARN online; instead we
# DEPLOY a contextual policy floor* = f(URLLC offered load, Lambda) derived offline (doc/06 §N.6):
# reserve >= URLLC demand fraction with a Lambda-dependent safety margin. The context (URLLC S2 RLC
# offered load) IS a KPM (dl_offered_load_rlc). Enable with env XAPP_CONTEXTUAL=1; default off keeps
# the static slice_action.json behavior used for data collection.
import os, math
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
     
            if not (report_index % CTRL_FREQ):
                print("Report Index:", report_index) 
                    
                # Sending Control
                action = contextual_action(ue_data_dict) if CONTEXTUAL else read_action()
                apply_slicing_action(action, control_sck)


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
    global _last_applied
    s1, s2 = action["s1"], action["s2"]

    if s1["min"] + s2["min"] > 100:
        print(f"[action] WARNING sum of mins {s1['min']}+{s2['min']} > 100; skipping.")
        return
    if s1.get("dedicated", 0) + s2.get("dedicated", 0) > 100:
        print(f"[action] WARNING sum of dedicated {s1.get('dedicated',0)}+{s2.get('dedicated',0)} > 100; skipping.")
        return

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
        

if __name__ == '__main__':
    main()

