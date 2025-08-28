import src.e2ap_xapp as e2ap_xapp
from ran_messages_pb2 import *
from time import sleep, time
from ricxappframe.e2ap.asn1 import IndicationMsg
import sys
# add the path to the compiled protobuf files
sys.path.append("oai-oran-protolib/builds/")
import csv
from datetime import datetime

# DEFINE CSV FILE SETTINGS
CSV_FILENAME = "ran_metrics.csv"
CSV_HEADER = [
    "timestamp",
    "cell_load_prb",
    "rnti",
    "rsrp_dbm",
    "ber_dl",
    "ber_ul",
    "mcs_dl",
    "mcs_ul"
]

def setup_csv_file():
    """Creates the CSV file and writes the header row."""
    try:
        with open(CSV_FILENAME, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
        print(f"Successfully created and initialized '{CSV_FILENAME}'")
    except IOError as e:
        print(f"Error: Could not write to CSV file: {e}")
        sys.exit(1)

def xappLogic():
    setup_csv_file()

    # instantiate xapp
    connector = e2ap_xapp.e2apXapp()

    # get gnbs connected to RIC
    gnb_id_list = connector.get_gnb_id_list()
    print("{} gNB connected to RIC, listing:".format(len(gnb_id_list)))
    for gnb_id in gnb_id_list:
        print(gnb_id)
    print("---------")

    # change to a 500ms polling loop
    # the main loop will now periodically send a request and process the response
    target_gnb = None
    if gnb_id_list:
        target_gnb = gnb_id_list[0]
    
    polling_interval = 0.5 # 500ms

    while target_gnb:
        start_time = time() # for timing the loop

        # 1. build and send the indication request inside the loop
        print(f"Sending indication request to gNB {target_gnb}...")
        e2sm_buffer = e2sm_report_request_buffer()
        connector.send_e2ap_sub_request(e2sm_buffer, target_gnb)

        # 2. read incoming messages
        messgs = connector.get_queued_rx_message()
        if not messgs:
            print("No messages received in this interval.")
        else:
            print("{} messages received, processing...".format(len(messgs)))
            for msg in messgs:
                # 3. process only RIC Indication messages
                if msg["message type"] == connector.RIC_IND_RMR_ID:
                    print("RIC Indication received from gNB {}".format(msg["meid"]))
                    
                    # decode the outer ASN.1 message
                    indm = IndicationMsg()
                    indm.decode(msg["payload"])
                    
                    # parse the inner Protobuf E2SM payload
                    response = RAN_indication_response()
                    response.ParseFromString(indm.indication_message)

                    # 4. extract data and write to CSV
                    process_indication_response(response)
                else:
                    print("Unrecognized E2AP message received from gNB {}".format(msg["meid"]))
        
        # 5. sleep to maintain the polling interval
        elapsed_time = time() - start_time
        sleep_duration = polling_interval - elapsed_time
        if sleep_duration > 0:
            print(f"Sleeping for {sleep_duration:.2f}s...")
            sleep(sleep_duration)
        print("____")

    print("No gNBs connected. Exiting.")

def process_indication_response(response: RAN_indication_response):
    """Parses the indication response and writes the data to the CSV file."""
    metrics = {}

    # iterate through the key-value map in the response
    for entry in response.param_map:
        if entry.key == RAN_parameter.Value("CELL_LOAD"):
            metrics['cell_load'] = entry.int64_value
        elif entry.key == RAN_parameter.Value("UE_LIST"):
            metrics['ue_list'] = entry.ue_list.ue_info

    if 'ue_list' in metrics:
        timestamp = datetime.now().isoformat()
        cell_load = metrics.get('cell_load', -1)

        try:
            with open(CSV_FILENAME, 'a', newline='') as f:
                writer = csv.writer(f)
                
                # write one row for each UE found in the response
                for ue in metrics['ue_list']:
                    row_data = [
                        timestamp,
                        cell_load,
                        ue.rnti,
                        f"{ue.rsrp:.2f}",
                        f"{ue.ber_dl:.4f}",
                        f"{ue.ber_ul:.4f}",
                        ue.mcs_dl,
                        ue.mcs_ul
                    ]
                    writer.writerow(row_data)
            
            print(f"[{timestamp}] Data for {len(metrics['ue_list'])} UEs saved to CSV.")
        except IOError as e:
            print(f"Error: Could not write to CSV file: {e}")

def e2sm_report_request_buffer():
    """Builds the E2SM payload for an Indication Request."""
    master_mess = RAN_message()
    master_mess.msg_type = RAN_message_type.Value("INDICATION_REQUEST")
    
    inner_mess = RAN_indication_request()
    
    # add CELL_LOAD to the requested parameters
    # request both the UE list and the cell load.
    requested_params = [RAN_parameter.Value("UE_LIST"), RAN_parameter.Value("CELL_LOAD")]
    inner_mess.target_params.extend(requested_params)
    master_mess.ran_indication_request.CopyFrom(inner_mess)
    buf = master_mess.SerializeToString()
    return buf

if __name__ == "__main__":
    xappLogic()