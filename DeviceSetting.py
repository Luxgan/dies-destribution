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

# get device setting
def getDeviceSetting(demandform, data_folder, result_folder, device_setting_name, customer, input_parameter):
    log2 = {
        'device setting':[],
        'exe time':[]
    }
    start_time = time.time()
    device_index = []
    no_device_index = []

    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    is_available = True
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, device_setting_name)):
            device_setting = RWcsv.readCSV(device_setting_name, data_folder)
            print("load device setting success.")
        else:
            is_available = False
    elif config['RUN_WAY'][0] == 'ORACLE':
        device_setting = orcl_connect.getDeviceSetting( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(device_setting)!=0:
            print("load device setting success.")
        else:
            is_available = False

    if is_available:
        for demandform_index in range(len(demandform)):
            instr_time = time.localtime()
            log_list2 = []
            demandform[demandform_index]['device setting'] = {}

            # search the same assy device in device setting
            select_device = device_setting[device_setting['Z_ASSY_DEV_TYPE'] == demandform[demandform_index]['Z_ASSY_DEV_TYPE']]

            # drop the same Z_LOT_POSNR in device setting
            item_list = select_device.drop_duplicates('Z_LOT_POSNR','first').sort_values(by=['Z_LOT_POSNR'])
            item_list = list(item_list['Z_LOT_POSNR'])

            # append the device setting list, because there will be oen or more available device
            for item in item_list:
                item = int(item) if not pd.isnull(item) else 1
                demandform[demandform_index]['device setting'][item] = []

            for index, select_device_row in select_device.iterrows():
                select_device_row_dict = select_device_row.to_dict()
                # if the Z_LOT_POSNR is empty, it is dummy
                if pd.isnull(select_device_row_dict['Z_LOT_POSNR']) and len(select_device)!=1:
                    continue
                item = int(select_device_row_dict['Z_LOT_POSNR']) if not pd.isnull(select_device_row_dict['Z_LOT_POSNR']) else 1
                demandform[demandform_index]['device setting'][item].append(select_device_row_dict)
                log_list2.append("device setting:{}".format(index+1))
            if len(select_device)==0:
                no_device_index.append(demandform_index)
                demandform[demandform_index]['msg'] = 2
            else:
                device_index.append(demandform_index)

            temp_list = []            
            temp_list.append(time.strftime("%Y%m%d", instr_time))
            temp_list.append(time.strftime("%H%M%S", instr_time))
            device_log = {
                demandform[demandform_index]['Z_ASSY_DEV_TYPE']:log_list2,
                demandform[demandform_index]['Z_ITEM']:"",
                "exe time":temp_list
            }
            log2['device setting'].append(device_log)
    else:
        for demandform_row in demandform:
            demandform_row['device setting'] = {}
        print("no device setting file.")

    end_time = time.time()
    exe_time = round(end_time - start_time, 2)
    start_list = [time.strftime("%Y%m%d", time.localtime(start_time)),time.strftime("%H%M%S", time.localtime(start_time))]
    end_list = [time.strftime("%Y%m%d", time.localtime(end_time)),time.strftime("%H%M%S", time.localtime(end_time))]
    log2["exe time"].append([start_list, end_list, exe_time])

    #wafer_device = [demandform[i] for i in device_index]
    wafer_device = demandform
    no_wafer_device = [demandform[i] for i in no_device_index]

    buf = os.path.join(result_folder, "wafer device{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(wafer_device, outfile, sort_keys=False, indent=4)
    buf = os.path.join(result_folder, "no wafer device{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(no_wafer_device, outfile, sort_keys=False, indent=4)
    
    return wafer_device, log2
