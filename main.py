 #-*- coding=utf-8 -*-
import os
import sys
import json
import time

import demandBuildPlan
import DynamicProgram
import SplitSchedule
import Message
import Log  

if __name__ == "__main__":
    ##########################
    # pre file information
    ##########################
    CUSTOMER = "MTK0428"
    result_log2 = []
    # input_parameter = [程式路徑, WP#, KUNNR, KNOZS]
    input_parameter = sys.argv

    print(input_parameter)

    if len(input_parameter)!=1:
        CUSTOMER = input_parameter[1]

    DATA_FOLDER = "../csvdata/" + CUSTOMER + "/"
    RESULT_FOLDER = "../output/{}_{}/".format(CUSTOMER, time.strftime("%H,%M,%S", time.localtime()) )

    if not os.path.exists(RESULT_FOLDER):
        os.makedirs(RESULT_FOLDER)

    COMBINE_RULE_NAME = "Combine Rule.csv"
    DEMANDFORM_FILENAME = "Demand Form.csv"
    DEVICE_SETTING_FILENAME = "Device Setting.csv"
    DIE_RELEASE_RULE_FILENAME = "Die Releasing Rule.csv"
    INVENTORY_FILENAME = "Inventory.csv"
    LOT_LIMIT_FILENAME = "Lot Limit.csv"
    LOT_SELECTION_FILENAME = "Lot Selection.csv"
    PID_FILENAME = "PID.csv"
    SPLIT_RULE_FILENAME = "Split Rule.csv"
    SPLIT_SCHEDULE_FILENAME = "Split Schedule.csv"
    SPLIT_WAFER_ID_FILENAME = "Split WaferId.csv"

    ##########################
    # module0: program start
    ##########################
    log2 = {
        'program start':[]
    }
    start_time = time.time()
    log2["program start"].append(time.strftime("%Y%m%d", time.localtime(start_time)))
    log2["program start"].append(time.strftime("%H%M%S", time.localtime(start_time)))
    result_log2.append(log2)

    ##########################
    # module1: demand form
    ##########################
    print("----- Start demand form -----")
    demandform, log2 = demandBuildPlan.demandBuildPlan(DATA_FOLDER, RESULT_FOLDER, DEMANDFORM_FILENAME, CUSTOMER, input_parameter)
    result_log2.append(log2)
    print("----- Build demand form finish -----")
    
    ##########################
    # module2: dynamic program
    ##########################
    print("----- Start dynamic program -----")
    demandform, log2 = DynamicProgram.dynamicProgram(demandform, DATA_FOLDER, RESULT_FOLDER, DEVICE_SETTING_FILENAME, DIE_RELEASE_RULE_FILENAME, INVENTORY_FILENAME, LOT_LIMIT_FILENAME, LOT_SELECTION_FILENAME, PID_FILENAME, CUSTOMER, input_parameter)
    result_log2 += log2
    print("----- Build dynamic program finish -----")
    
    ##########################
    # module3: split chedule
    ##########################
    print("----- Start split schedule  -----")
    demandform, all_schedule, log2 = SplitSchedule.splitSchedule(demandform, DATA_FOLDER, RESULT_FOLDER, SPLIT_RULE_FILENAME, SPLIT_SCHEDULE_FILENAME, SPLIT_WAFER_ID_FILENAME, COMBINE_RULE_NAME, input_parameter)
    result_log2 += log2
    print("----- Build split schedule finish -----")

    ##########################
    # module4: set message and save file
    ##########################    
    print("----- Start set message  -----")
    all_schedule = Message.setMessage(demandform, all_schedule, RESULT_FOLDER, CUSTOMER)
    print("----- Build set message finish -----")

    ##########################
    # module5: program end
    ##########################
    log2 = {
        'program end':[],
        'exe time':0
    }
    end_time = time.time()
    log2["program end"].append(time.strftime("%Y%m%d", time.localtime(end_time)))
    log2["program end"].append(time.strftime("%H%M%S", time.localtime(end_time)))
    log2["exe time"] = end_time - start_time
    result_log2.append(log2)

    ##########################
    # module6: set log and save file
    ##########################    
    
    print("----- Start set Log  -----")
    Log.setLogFile(DATA_FOLDER, RESULT_FOLDER, DEMANDFORM_FILENAME, result_log2, CUSTOMER, input_parameter)
    print("----- Build set Log finish -----")

    buf = os.path.join(RESULT_FOLDER, "log_{}_2.json".format(CUSTOMER))
    with open(buf, 'w') as outfile:
        json.dump(result_log2, outfile, sort_keys=False, indent=4)
    