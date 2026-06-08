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

                    # Compute throughput [Mbps] based on RNTI, timestamp, and dl_total_bytes
                    if rnti in ue_data_dict:
                        dl_th = ((dl_total_bytes - ue_data_dict[rnti]['dl_total_bytes'])/(timestamp - ue_data_dict[rnti]['timestamp']))*8
                        ul_th = ((ul_total_bytes - ue_data_dict[rnti]['ul_total_bytes'])/(timestamp - ue_data_dict[rnti]['timestamp']))*8
                        
                        # Cap downlink throughput to 60 Mbps
                        # if dl_th > 60000000:
                        #     dl_th = 60000000

                    else:
                        dl_th = 0.0
                        ul_th = 0.0

                    # Add or update rnti dictionary
                    ue_data_dict[rnti] = {
                        'timestamp': timestamp,
                        'dl_total_bytes': dl_total_bytes,
                        'ul_total_bytes': ul_total_bytes,
                        'nssai_sst':nssai_sst,
                        'nssai_sd':nssai_sd,
                        'dl_th':dl_th,
                        'ul_th':ul_th
                    }
                    # ue_data_dict[rnti]['dl_th_history'] 

                    p = Point("xapp-stats").tag("rnti", rnti).field("timestamp", timestamp).field("avg_rsrp", avg_rsrp).field("ph", ph).field("pcmax", pcmax)\
                            .field("dl_total_bytes", dl_total_bytes).field("dl_errors", dl_errors).field("dl_bler", dl_bler).field("dl_mcs", dl_mcs)\
                            .field("ul_total_bytes", ul_total_bytes).field("ul_errors", ul_errors).field("ul_bler", ul_bler).field("ul_mcs", ul_mcs)\
                            .field("nssai_sst", nssai_sst).field("nssai_sd", nssai_sd).field("dl_th", dl_th).field("ul_th", ul_th).field("avg_prbs_dl", avg_prbs_dl)
                    print(p)
                    # logging.info('Write to influxdb: ' + repr(p))
                    write_api.write(bucket=bucket, record=p)

                except Exception as e:
                    print("Skip log, influxdb error: " + str(e))
     
            if not (report_index % CTRL_FREQ):
                print("Report Index:", report_index) 
                    
                # Sending Control
                action = read_action()
                apply_slicing_action(action, control_sck)


# slicing functions
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

    # Decide which slice's min is decreasing relative to current state; send it first.
    if _last_applied is not None:
        d1 = s1["min"] - _last_applied["s1"]["min"]
        d2 = s2["min"] - _last_applied["s2"]["min"]
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

