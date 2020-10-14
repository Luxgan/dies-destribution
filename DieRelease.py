 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json
import time

import RWcsv
import orcl_connect

config = pd.read_csv('./config.csv')

# get the die release rule  
def getDieRelease(demandform, data_folder, result_folder, die_release_name, customer, input_parameter):
    log2 = {
        'die release rule':[],
        'exe time':[]
    }
    start_time = time.time()

    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    is_available = True
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, die_release_name)):
            die_release = RWcsv.readCSV(die_release_name, data_folder)
            print("load die release success.")
        else:
            is_available = False
    elif config['RUN_WAY'][0] == 'ORACLE':
        die_release = orcl_connect.getDieReleasingRule( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(die_release)!=0:
            print("load die release success.")
        else:
            is_available = False

    if is_available:
        for demandform_row in demandform:
            instr_time = time.localtime()
            log_list2= []
            demandform_row['die release'] = {}

            for index, die_release_row in die_release.iterrows():
                if die_release_row['Z_ASSY_DEV_TYPE']==demandform_row['Z_ASSY_DEV_TYPE']:
                    die_release_row_dict = die_release_row.to_dict()
                    demandform_row['die release'] = die_release_row_dict
                    log_list2.append("die release: {}".format(index+1))
            
            temp_list = []            
            temp_list.append(time.strftime("%Y%m%d", instr_time))
            temp_list.append(time.strftime("%H%M%S", instr_time))
            device_log = {
                demandform_row['Z_ASSY_DEV_TYPE']:log_list2,
                demandform_row['Z_ITEM']:"",
                "exe time":temp_list
            }
            log2['die release rule'].append(device_log)
    else:
        for demandform_row in demandform:
            demandform_row['die release'] = {}
        print("no die release file.")
    
    end_time = time.time()
    exe_time = round(end_time - start_time, 2)
    start_list = [time.strftime("%Y%m%d", time.localtime(start_time)),time.strftime("%H%M%S", time.localtime(start_time))]
    end_list = [time.strftime("%Y%m%d", time.localtime(end_time)),time.strftime("%H%M%S", time.localtime(end_time))]
    log2["exe time"].append([start_list, end_list, exe_time])

    buf = os.path.join(result_folder, "die release{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(demandform, outfile, sort_keys=False, indent=4)
    
    return demandform, log2
