 #-*- coding=utf-8 -*-
import os
import pandas as pd
import json

import RWcsv
import orcl_connect
import time

config = pd.read_csv('./config.csv')

# merge the demand build plan
def demandBuildPlan(data_folder, result_folder, data_filename, customer, input_parameter):
    log2 = {
        'demand sorted':[],
        'exe time':[]
    }
    start_time = time.time()
    resultDevice = []

    # 讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        original_bp = RWcsv.readCSV(data_filename, data_folder)
    elif config['RUN_WAY'][0] == 'ORACLE':
        original_bp = orcl_connect.getBuildPlan( orcl_connect.getOrclConnectCur(), input_parameter )

    # sort the demand with KUNNR, group, priority
    original_bp = original_bp.sort_values(by=['KUNNR','Z_DEMAND_GRP','Z_PRIORITY'])
    
    for index, bp_row in original_bp.iterrows():
        instr_time = time.localtime()

        bp_row_dict = bp_row.to_dict()
        # 'split' for whether the demand is split
        bp_row_dict['split'] = 0
        # 'msg' for the situation of the demand
        bp_row_dict['msg'] = 0
        resultDevice.append(bp_row_dict)

        temp_list = []
        temp_list.append("{},{},demand form:{}".format(bp_row_dict['Z_ASSY_DEV_TYPE'],bp_row_dict['Z_ITEM'],index+1))
        temp_list.append(time.strftime("%Y%m%d", instr_time))
        temp_list.append(time.strftime("%H%M%S", instr_time))
        log2['demand sorted'].append(temp_list)

    end_time = time.time()
    exe_time = round(end_time - start_time, 2)
    start_list = [time.strftime("%Y%m%d", time.localtime(start_time)),time.strftime("%H%M%S", time.localtime(start_time))]
    end_list = [time.strftime("%Y%m%d", time.localtime(end_time)),time.strftime("%H%M%S", time.localtime(end_time))]
    log2["exe time"].append([start_list, end_list, exe_time])

    buf = os.path.join(result_folder, "BPMerge{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(resultDevice, outfile, sort_keys=False, indent=4)  
    
    return resultDevice, log2