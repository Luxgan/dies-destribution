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

# get PID 
def getDieDevice(demandform, data_folder, result_folder, pid_name, customer, input_parameter):
    log2 = {
        'die device':[],
        'exe time':[]
    }
    start_time = time.time()
    pid_index = []
    no_pid_index = []

    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    is_available = True
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, pid_name)):
            pid = RWcsv.readCSV(pid_name, data_folder)
        else:
            is_available = False
    elif config['RUN_WAY'][0] == 'ORACLE':
        pid = orcl_connect.getPID( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(pid)==0:
            is_available = False

    if is_available:
        print("load pid success.")

        for demandform_index in range(len(demandform)):
            instr_time = time.localtime()
            log_list2 = []
            demandform[demandform_index]['die device'] = []

            # search the same assy device in PID
            select_PID = pid[pid['MFG_DEV_NAME'] == demandform[demandform_index]['Z_ASSY_DEV_TYPE']]

            if len(select_PID)!=0:
                # save the index of the demand has pid 
                pid_index.append(demandform_index)

                # drop the same DIE_SEQ PID
                select_PID = select_PID.drop_duplicates('DIE_SEQ','first').sort_values(by=['DIE_SEQ'])

                for index, select_PID_row in select_PID.iterrows():
                    select_PID_row_dict = select_PID_row.to_dict()

                    if not pd.isnull(select_PID_row_dict['Z_DIE_DEV_TYPE']):
                        # if PID has dummy, pass the dummy
                        if select_PID_row_dict['Z_DIE_DEV_TYPE'].find("DUMMY")!=(-1) or select_PID_row_dict['Z_DIE_DEV_TYPE'].find("dummy")!=(-1) or\
                            select_PID_row_dict['Z_DIE_DEV_TYPE'].find("SPACER")!=(-1) or select_PID_row_dict['Z_DIE_DEV_TYPE'].find("sapcer")!=(-1):
                            print("Dummy " + demandform[demandform_index]['Z_ASSY_DEV_TYPE'])
                            log_list2.append("PID:{}".format(index+1))
                            continue
                    demandform[demandform_index]['die device'].append(select_PID_row_dict)
                    log_list2.append("PID:{}".format(index+1))
                
                # calculate whether the PID use the same device
                demandform[demandform_index]['device combine'] = {}

                # the same device list
                demandform[demandform_index]['device combine']['device_list'] = []

                # the same device film type
                demandform[demandform_index]['device combine']['device_film'] = []

                # the same device ratio
                demandform[demandform_index]['device combine']['device_ratio'] = []

                for device_setting_row in demandform[demandform_index]['die device']:
                    temp = 0

                    for device_index in range(len(demandform[demandform_index]['device combine']['device_list'])):
                        if device_setting_row['Z_DIE_DEV_TYPE']==demandform[demandform_index]['device combine']['device_list'][device_index]:
                            if device_setting_row['CMP_ITM_ID']==demandform[demandform_index]['device combine']['device_film'][device_index]:
                                temp = 1
                                break
                    # if there is no the same device, append the new one
                    if temp==0:
                        demandform[demandform_index]['device combine']['device_list'].append(device_setting_row['Z_DIE_DEV_TYPE'])
                        demandform[demandform_index]['device combine']['device_film'].append(device_setting_row['CMP_ITM_ID'])
                        demandform[demandform_index]['device combine']['device_ratio'].append(1)
                    # if not, add the original ratio
                    else:
                        demandform[demandform_index]['device combine']['device_ratio'][ demandform[demandform_index]['device combine']['device_list'].index(device_setting_row['Z_DIE_DEV_TYPE']) ] += 1
            else:
                demandform[demandform_index]['msg'] = 1
                no_pid_index.append(demandform_index)

            temp_list = []            
            temp_list.append(time.strftime("%Y%m%d", instr_time))
            temp_list.append(time.strftime("%H%M%S", instr_time))
            device_log = {
                demandform[demandform_index]['Z_ASSY_DEV_TYPE']:log_list2,
                demandform[demandform_index]['Z_ITEM']:"",
                "exe time":temp_list
            }
            log2['die device'].append(device_log)
    else:
        for demandform[demandform_index] in demandform:
            demandform[demandform_index]['die device'] = []
        print("no pid file.")
        
    end_time = time.time()
    exe_time = round(end_time - start_time, 2)
    start_list = [time.strftime("%Y%m%d", time.localtime(start_time)),time.strftime("%H%M%S", time.localtime(start_time))]
    end_list = [time.strftime("%Y%m%d", time.localtime(end_time)),time.strftime("%H%M%S", time.localtime(end_time))]
    log2["exe time"].append([start_list, end_list, exe_time])
    
    #die_device = [demandform[i] for i in pid_index]
    die_device = demandform
    no_die_device = [demandform[i] for i in no_pid_index]
    
    buf = os.path.join(result_folder, "die device{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(die_device, outfile, sort_keys=False, indent=4)
    buf = os.path.join(result_folder, "no die device{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(no_die_device, outfile, sort_keys=False, indent=4)
    
    return die_device, log2
