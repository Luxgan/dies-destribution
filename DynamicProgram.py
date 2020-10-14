 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json
import datetime
import time

import RWcsv
import orcl_connect
import PID
import DeviceSetting
import SameGroup
import DieRelease
import SplitWaferID
import Knapsack

config = pd.read_csv('./config.csv')

# 動態規劃
def dynamicProgram(demandform, data_folder, result_folder, device_setting_name, die_release_name, inventory_name, lot_limit_name, lot_selec_name, pid_name, customer, input_parameter):
    all_log_2 = []

    # load necessary file
    #讀取資料的方式 (讀Excel檔或從資料庫取資料)    
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, lot_selec_name)):
            lot_selection = RWcsv.readCSV(lot_selec_name, data_folder)
            print("load lot selection success.")
        else:
            print("no lot selection file.")
    elif config['RUN_WAY'][0] == 'ORACLE':
        lot_selection = orcl_connect.getLotSelection( orcl_connect.getOrclConnectCur(), input_parameter )
        print("load lot selection success.")        


    lot_limit_flag = False     # 是否有lot limit的flag
    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, lot_limit_name)):
            lot_limit = RWcsv.readCSV(lot_limit_name, data_folder)
            if len(lot_limit)!=0:
                lot_limit_flag = True
            print("load lot limit success.")
        else:
            print("no lot limit file.")        
    elif config['RUN_WAY'][0] == 'ORACLE':
        lot_limit = orcl_connect.getLotLimit( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(lot_limit)!=0:
            lot_limit_flag = True


    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, inventory_name)):
            inventory = RWcsv.readCSV(inventory_name, data_folder)

            inventory['lot limit'] = pd.Series(np.zeros((inventory.shape[0]), dtype=np.int))
            inventory['film number'] = pd.Series(["00"]*inventory.shape[0])
            print("load inventory success.")
        else:
            print("no inventory file.")
    elif config['RUN_WAY'][0] == 'ORACLE':
        inventory = orcl_connect.getInventory( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(inventory)!=0:
            inventory['lot limit'] = pd.Series(np.zeros((inventory.shape[0]), dtype=np.int))
            inventory['film number'] = pd.Series(["00"]*inventory.shape[0])
            print("load inventory success.")
        else:
            print("no inventory file.")
    

    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, pid_name)):
            pid = RWcsv.readCSV(pid_name, data_folder)
            print("load pid success.")
        else:
            print("no pid file.")
    elif config['RUN_WAY'][0] == 'ORACLE':
        pid = orcl_connect.getPID( orcl_connect.getOrclConnectCur(), input_parameter )


    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, device_setting_name)):
            device_setting = RWcsv.readCSV(device_setting_name, data_folder)
            print("load device setting success.")
        else:
            print("no device setting file.")
    elif config['RUN_WAY'][0] == 'ORACLE':
        device_setting = orcl_connect.getDeviceSetting( orcl_connect.getOrclConnectCur(), input_parameter )
    

    waferID_flag = False      #是否有 Split WaferId 的flag
    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, "Split WaferId.csv")):
            split_wafer_id = RWcsv.readCSV("Split WaferId.csv", data_folder)
            if len(split_wafer_id)!=0:
                waferID_flag = True
            print("load split wafer id success.")
        else:
            print("no split wafer id file.")
    elif config['RUN_WAY'][0] == 'ORACLE':
        split_wafer_id = orcl_connect.getSplitWaferID( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(split_wafer_id)!=0:
            waferID_flag = True
        print("load split wafer id success.")

    ##########################
    # demand form modify
    ##########################    
    # check PID to get die device
    demandform, log2 = PID.getDieDevice(demandform, data_folder, result_folder, pid_name, customer, input_parameter)
    all_log_2.append(log2)

    # check device setting
    demandform, log2 = DeviceSetting.getDeviceSetting(demandform, data_folder, result_folder, device_setting_name, customer, input_parameter)
    all_log_2.append(log2)

    # set un-know group
    # 將有使用相同device setting但是不同的assy device群組在一起
    demandform = SameGroup.getSameGroup(demandform)

    # check die release
    demandform, log2 = DieRelease.getDieRelease(demandform, data_folder, result_folder, die_release_name, customer, input_parameter)
    all_log_2.append(log2)

    ##########################
    # inventory modify
    ##########################    
    #check eng
    #inventory = setNotEng(inventory)

    log2 = {
        'distribution':[],
        'exe time':[]
    }
    start_time = time.time()

    total_qty = 0            # 紀錄產品所需demand
    die_device_ratio = []            #紀錄使用相同device setting的次數
    group_flag = demandform[0]['Z_DEMAND_GRP']     # 紀錄當前demand group，若改變則更新修正後inventory
    original_inventory = inventory                #若group_flag改變則更新修正後inventory

    for demandform_row in demandform:
        log_list2 = []
        current_qty = 0                # 同一個windows中分配的數量
        demandform_row['current qty'] = 0
        demandform_row['demand wafer'] = {}
        demandform_row['demand batch'] = {}
        demandform_row['shortage'] = ""
        drop_index = []                # 儲存不能被使用wafer的dataframe中的index
        is_shortage = False
        die_release_num_list = []       # 儲存產品的每個device setting分配量

        # 決定產品的數量種類(EA/PC)
        if (pd.isnull(demandform_row['Z_DIE_QTY'])):
            total_qty = demandform_row['Z_WAFER_QTY']
            demandform_row['qty type'] = "PC"
        else:
            total_qty = demandform_row['Z_DIE_QTY']
            demandform_row['qty type'] = "EA"
        
        # 如果PID 中die seq與 device setting 不一致則跳下一個產品
        if len(demandform_row['die device'])!=len(demandform_row['device setting']) and demandform_row['msg']==0:
            demandform_row['msg'] = 3
            instr_time = time.localtime()
            device_log = {
                demandform_row['Z_ASSY_DEV_TYPE']:[["WO creation fail",time.strftime("%Y%m%d", instr_time),time.strftime("%H%M%S", instr_time)]],
                demandform_row['Z_ITEM']:""

            }
            log2['distribution'].append(device_log)
            continue
        # 如果已經發生 WO creation fail 則跳下一個產品
        if demandform_row['msg'] != 0:
            instr_time = time.localtime()
            device_log = {
                demandform_row['Z_ASSY_DEV_TYPE']:[["WO creation fail",time.strftime("%Y%m%d", instr_time),time.strftime("%H%M%S", instr_time)]],
                demandform_row['Z_ITEM']:""
            }
            log2['distribution'].append(device_log)
            continue
        # 若已分配完畢則跳下一筆 (若有同group且無priority則會被設為1)
        if demandform_row['split'] == 1:
            instr_time = time.localtime()
            device_log = {
                demandform_row['Z_ASSY_DEV_TYPE']:[["group shortage",time.strftime("%Y%m%d", instr_time),time.strftime("%H%M%S", instr_time)]],
                demandform_row['Z_ITEM']:""
            }
            log2['distribution'].append(device_log)
            continue
        # 若同group有較小priority的產品因為 WO creation fail 未分配，則後續不分配
        if not pd.isnull(demandform_row['Z_PRIORITY']):
            if str(demandform_row['Z_PRIORITY'])!="1":
                temp_flag = False
                for temp_row in demandform:
                    if temp_row['Z_DEMAND_GRP']==demandform_row['Z_DEMAND_GRP'] and temp_row['Z_PRIORITY']<demandform_row['Z_PRIORITY']:
                        if temp_row['msg']!=0:
                            demandform_row['msg'] = 5
                            
                            instr_time = time.localtime()
                            device_log = {
                                demandform_row['Z_ASSY_DEV_TYPE']:[["priority restriction",time.strftime("%Y%m%d", instr_time),time.strftime("%H%M%S", instr_time)]],
                                demandform_row['Z_ITEM']:""
                            }
                            log2['distribution'].append(device_log)

                            temp_flag = True
                            break
                    if temp_row['Z_DEMAND_GRP']==demandform_row['Z_DEMAND_GRP'] and temp_row['Z_PRIORITY']==demandform_row['Z_PRIORITY']:
                        break
                
                if temp_flag:
                    continue

        # 儲存使用相同device setting的數量
        die_device_ratio = []
        die_device_ratio = demandform_row['device combine']['device_ratio']
        
        # 若有lot limit，記錄列表中產品
        if lot_limit_flag:
            lot_limit_list = list(lot_limit['Z_WAFER_DEV_TYPE'].drop_duplicates())
            for num in range(len(lot_limit_list)):
                lot_limit_list[num] = lot_limit_list[num].replace("%","")

        # 若group_flag改變(當前group完成跳至下一個group)，更新inventory狀態
        if group_flag!=demandform_row['Z_DEMAND_GRP']:
            original_inventory = inventory
            group_flag = demandform_row['Z_DEMAND_GRP']

        # 紀錄 Single die 儲存分配結果
        if len(demandform_row['die device'])==1:
            demandform_row['demand wafer']['1'] = []
            demandform_row['demand batch']['1'] = []
        # mcm die
        else:
            for die_device_row in demandform_row['die device']:
                demandform_row['demand wafer'][str(int(die_device_row['DIE_SEQ']))] = []
                demandform_row['demand batch'][str(int(die_device_row['DIE_SEQ']))] = []

        die_device_index = 0       # 用來決定要取的PID資料(有些產品有重複使用的device setting，可以不用每個都抓)
        die_level_list = []        # 用來記錄該產品使用到的PID index(經過核對相同原料後，有重複使用的原料可以不用再跑一遍)
        temp_inventory = inventory # 用來記錄本次產品分配前的inventory狀況
        MATNR_flag = False         # 用來確認是否有MATNR找不到的問題，若有則需要另外做處理
        for ratio_index in range(len(demandform_row['device combine']['device_ratio'])):
            # 若已分配完畢則跳下一筆 
            if demandform_row['split'] == 1:
                continue
            if ratio_index!=0:
                die_device_index += demandform_row['device combine']['device_ratio'][ratio_index-1]

            if len(demandform_row['die device'])==1:
                die_level = 1     # 根據PID中的DIE_SEQ，決定要取的device setting                
                die_level_list.append(die_level)
            else:
                die_level = (demandform_row['die device'][(die_device_index)]['DIE_SEQ'])
                die_level_list.append(die_level)
            current_qty = 0
            drop_index = []

            instr_time = time.localtime()
            info_list = ["die sequence {}".format(die_level), time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
            log_list2.append(info_list) 
            
            # re-merge device setting through lot selection
            instr_time = time.localtime()
            info_list = ["check lot selection by device setting", time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
            log_list2.append(info_list) 
            first_use_index = []        # 需要先被使用的 index
            not_use_lot_index = []     # 不能被使用的 index
            for device_setting_index in range(len(demandform_row['device setting'][die_level])):
                wafer_device_list = lot_selection[lot_selection['Z_WAFER_DEV_TYPE']==demandform_row['device setting'][die_level][device_setting_index]['Z_WAFER_DEV_TYPE']]
                if len(wafer_device_list)!=0:
                    for index, wafer_device_list_row in wafer_device_list.iterrows():
                        if wafer_device_list_row['Z_CHARG'] is None:
                            if wafer_device_list_row['Z_TYPE']==1:
                                first_use_index.append(device_setting_index)
                                instr_time = time.localtime()
                                log_str = "lot selection:{}, first use: ASSY_DEV:{}".format(index+1,demandform_row['device setting'][1][device_setting_index]['Z_ASSY_DEV_TYPE'])
                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                log_list2.append(info_list) 
                            elif wafer_device_list_row['Z_TYPE']==2:
                                if demandform_row['Z_ASSY_DEV_TYPE'].find(wafer_device_list_row['Z_ASSY_DEV_TYPE'].strip('%'))==-1:
                                    not_use_lot_index.append(device_setting_index)
                                    instr_time = time.localtime()
                                    log_str = "lot selection:{}, spicific use in ASSY_DEV:{} Z_DEV_TYP:{}".format(index+1,wafer_device_list_row['Z_ASSY_DEV_TYPE'],demandform_row['device setting'][1][device_setting_index]['Z_WAFER_DEV_TYPE'])
                                    info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                    log_list2.append(info_list) 
                            else:
                                if pd.isnull(wafer_device_list_row['Z_ASSY_DEV_TYPE']) or\
                                    demandform_row['Z_ASSY_DEV_TYPE'].find(wafer_device_list_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                                    not_use_lot_index.append(device_setting_index)
                                    instr_time = time.localtime()
                                    log_str = "lot selection:{}, can not use in ASSY_DEV:{} Z_DEV_TYP:{}".format(index+1,wafer_device_list_row['Z_ASSY_DEV_TYPE'],demandform_row['device setting'][1][device_setting_index]['Z_WAFER_DEV_TYPE'])
                                    info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                    log_list2.append(info_list)
            use_lot_index = []
            for device_setting_index in range(len(demandform_row['device setting'][die_level])):
                if device_setting_index not in not_use_lot_index:
                    if device_setting_index not in first_use_index:
                        use_lot_index.append(device_setting_index)
            use_lot = [demandform_row['device setting'][die_level][i] for i in use_lot_index]
            first_use = [demandform_row['device setting'][die_level][i] for i in first_use_index]
            demandform_row['device setting'][die_level] = first_use + use_lot


            # search the batch in inventory for use
            batch_pc_list = None
            device_setting_list = []
            for device_setting_row in demandform_row['device setting'][die_level]:
                if device_setting_row['Z_WAFER_DEV_TYPE'] in device_setting_list:
                    continue
                else:
                    device_setting_list.append(device_setting_row['Z_WAFER_DEV_TYPE'])
                if batch_pc_list is None:
                    batch_pc_list = inventory[inventory['Z_DEV_TYP']==device_setting_row['Z_WAFER_DEV_TYPE']]
                    batch_pc_list = batch_pc_list.sort_values(by=['RTNO','ZWAFER_ID','ZBIN'])
                else:
                    next_list = inventory[inventory['Z_DEV_TYP']==device_setting_row['Z_WAFER_DEV_TYPE']]
                    next_list = next_list.sort_values(by=['RTNO','ZWAFER_ID','ZBIN'])

                    batch_pc_list = pd.concat([batch_pc_list, next_list], axis=0)

            if batch_pc_list is None or len(batch_pc_list)==0:
                demandform_row['msg'] = 7
                instr_time = time.localtime()
                log_str = "無庫存"
                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                log_list2.append(info_list)
                continue
            # batch_pc_list是所有可以用的wafer
            # batch_list是batch_pc_list中只有batch的資訊
            batch_list = batch_pc_list.drop_duplicates('RTNO','first') if batch_pc_list is not None else None

            if len(batch_pc_list)!=0:
                # check the device setting spcode and zbin
                # 因為有分single與mcm，先針對全部的device setting做檢查
                # 發現有不可用的從 batch_pc_list 移除
                devcie_setting_spcode_flag = False
                # 首先檢查device setting是否有限制
                for device_setting_row in demandform_row['device setting'][die_level]:
                    if not pd.isnull(device_setting_row['Z_Value']) or\
                        not pd.isnull(device_setting_row['Z_BIN']):
                        devcie_setting_spcode_flag = True
                # 若有限制則開始排除wafer
                if devcie_setting_spcode_flag:
                    drop_index = []
                    # 走訪全部可用wafer
                    for batch_index, batch_list_row in batch_pc_list.iterrows():
                        drop_flag = False
                        bin_type = []
                        for device_setting_row in demandform_row['device setting'][die_level]:
                            # 檢查device setting spcode
                            if not pd.isnull(device_setting_row['Z_Value']):
                                if device_setting_row['Z_Column']=="SP1" and device_setting_row['Z_Table']=="Z_ASEWP" and batch_list_row['zase_wp_z_c_spcod1']!=device_setting_row['Z_Value']:
                                    drop_flag = True
                                    break
                            # 檢查bin並且整理所有可用bin
                            if not pd.isnull(device_setting_row['Z_BIN']):
                                temp_bin = device_setting_row['Z_BIN'].split('+') if type(device_setting_row['Z_BIN'])==str else [str(int(device_setting_row['Z_BIN']))]
                                bin_type += temp_bin
                        # 如果有限制bin，檢查wafer的bin是否有被包含
                        if len(bin_type)!=0:
                            string = batch_list_row['ZBIN'] if type(batch_list_row['ZBIN'])==str else str(int(batch_list_row['ZBIN']))
                            if string not in bin_type:
                                drop_flag = True
                            
                        if drop_flag:
                            drop_index.append(batch_index)

                    batch_pc_list = batch_pc_list.drop(drop_index)

                # chech the batch in lot selection 
                instr_time = time.localtime()
                log_str = "check lot selection by batch"
                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                log_list2.append(info_list)
                first_use_index = []
                for batch_index, batch_list_row in batch_list.iterrows():
                    lot_batch_list = lot_selection[lot_selection['Z_CHARG']==batch_list_row['RTNO']]
                    if len(lot_batch_list)!=0:
                        for index, lot_batch_list_row in lot_batch_list.iterrows():
                            # 規則中的先用
                            if lot_batch_list_row['Z_TYPE']==1:
                                # 如果產品為空或有指定產品
                                if pd.isnull(lot_batch_list_row['Z_ASSY_DEV_TYPE']) or\
                                    demandform_row['Z_ASSY_DEV_TYPE'].find(lot_batch_list_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                                    # 若有指定wafer id
                                    if not pd.isnull(lot_batch_list_row['Z_W_SERIAL_NO']):
                                        drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]
                                        wafer_num_list = str(lot_batch_list_row['Z_W_SERIAL_NO']).split(',')
                                        temp = {
                                            'pri': wafer_lot_row['Z_PRIORITY'],
                                            'index':[]
                                        }
                                        if waferID_flag:
                                            temp_batch = SplitWaferID.getSplitWaferID([drop_list], input_parameter, -1)
                                            drop_list = temp_batch
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            if str(drop_row['ZWAFER_ID']) in wafer_num_list:
                                                temp['index'].append(batch_drop_index)
                                                instr_time = time.localtime()
                                                log_str = "lot selection:{}, first use: RTNO:{},LOTNO:{},ID:{}".format(index+1,drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                log_list2.append(info_list)
                                        first_use_index.append(temp)
                                        
                                    # 若沒有指定wafer id
                                    else:
                                        drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]
                                        temp = {
                                            'pri': lot_batch_list_row['Z_PRIORITY'],
                                            'index':[]
                                        }
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            temp['index'].append(batch_drop_index)
                                            instr_time = time.localtime()
                                            log_str = "lot selection:{}, first use: RTNO:{},LOTNO:{},ID:{}".format(index+1,drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                            log_list2.append(info_list)
                                        first_use_index.append(temp)
                            # 規則中的限用
                            elif lot_batch_list_row['Z_TYPE']==2:
                                if (pd.isnull(lot_batch_list_row['Z_ASSY_DEV_TYPE'])) or\
                                    demandform_row['Z_ASSY_DEV_TYPE'].find(lot_batch_list_row['Z_ASSY_DEV_TYPE'].strip('%'))==-1:
                                    drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]                                    
                                    if waferID_flag:
                                        temp_batch = SplitWaferID.getSplitWaferID([drop_list], input_parameter, -1)
                                        drop_list = temp_batch
                                    if not (pd.isnull(lot_batch_list_row['Z_W_SERIAL_NO'])):
                                        wafer_num_list = lot_batch_list_row['Z_W_SERIAL_NO'].split(',')
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            if drop_row['ZWAFER_ID'] in wafer_num_list:                                    
                                                batch_pc_list = batch_pc_list.drop(batch_drop_index)
                                                instr_time = time.localtime()
                                                log_str = "lot selection:{}, spicific use in ASSY_DEV:{}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index+1,lot_batch_list_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                log_list2.append(info_list)
                                                # 如果限用的wafer已經被先用抓到，要排除掉
                                                for row in first_use_index:
                                                    if batch_drop_index in row['index']:
                                                        row['index'].remove(batch_drop_index)
                                    else:
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            batch_pc_list = batch_pc_list.drop(batch_drop_index)
                                            instr_time = time.localtime()
                                            log_str = "lot selection:{}, spicific use in ASSY_DEV:{}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index+1,lot_batch_list_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                            log_list2.append(info_list)
                                            # 如果限用的wafer已經被先用抓到，要排除掉
                                            for row in first_use_index:
                                                if batch_drop_index in row['index']:
                                                    row['index'].remove(batch_drop_index)
                            # 規則中的禁用
                            else:
                                if pd.isnull(lot_batch_list_row['Z_ASSY_DEV_TYPE']) or \
                                    demandform_row['Z_ASSY_DEV_TYPE'].find(lot_batch_list_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                                    drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]
                                    if waferID_flag:
                                        temp_batch = SplitWaferID.getSplitWaferID([drop_list], input_parameter, -1)
                                        drop_list = temp_batch
                                        
                                    if not pd.isnull(lot_batch_list_row['Z_W_SERIAL_NO']):
                                        wafer_num_list = lot_batch_list_row['Z_W_SERIAL_NO'].split(',')
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            if drop_row['ZWAFER_ID'] in wafer_num_list:                                    
                                                batch_pc_list = batch_pc_list.drop(batch_drop_index)  
                                                instr_time = time.localtime()
                                                log_str = "lot selection:{}, can not use in {}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index+1,lot_batch_list_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                log_list2.append(info_list)
                                                for row in first_use_index:
                                                    if batch_drop_index in row['index']:
                                                        row['index'].remove(batch_drop_index) 
                                    else:                                            
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            if batch_drop_index in list(batch_pc_list.index):
                                                batch_pc_list = batch_pc_list.drop(batch_drop_index)    
                                                instr_time = time.localtime()
                                                log_str = "lot selection:{}, can not use in {}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index+1,lot_batch_list_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                log_list2.append(info_list)
                                                for row in first_use_index:
                                                    if batch_drop_index in row['index']:
                                                        row['index'].remove(batch_drop_index)
                # 針對有先用的部分進行wafer的調整
                temp2 = None
                i_index = 1000
                # 這邊目的為找到最小的priority初始值，因為可能有priority前面的先被使用掉了
                if len(first_use_index)!=0:
                    if first_use_index[0]['pri']!='%':
                        num = 1
                        for row in first_use_index:
                            if pd.isnull(row['pri']):
                                row['pri'] = num
                                num += 1
                            i_index = int(row['pri']) if int(row['pri'])<i_index else i_index
                # 按照priority來排序可以被使用的wafer
                for num in range(len(first_use_index)):
                    for row in first_use_index:
                        row['pri'] = int(row['pri']) if type(row['pri'])!=str else row['pri']
                        # 因為優先順序排序可能不同，所以個別抓到後再累增index
                        if str(row['pri'])==str(num+i_index) or str(row['pri'])=='%':
                            temp = batch_pc_list.loc[row['index']]
                            batch_pc_list = batch_pc_list.drop(row['index'])
                            if temp2 is None:
                                temp2 = temp
                            else:
                                temp2 = pd.concat([temp2, temp], axis=0)
                            break
                batch_pc_list = pd.concat([temp2, batch_pc_list], axis=0)

                # chech the wafer lot in lot selection 
                # 這部分與上面檢查batch的部分差不多
                # 也是依照先用、限用、禁用做篩選
                first_use_index = []                        
                wafer_lot = lot_selection[lot_selection['Z_WAFER_LOT'].notna()]   # 抓取 wafer lot中有值的欄位
                instr_time = time.localtime()
                log_str = "check lot selection by wafer lot"
                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                log_list2.append(info_list)
                for batch_index, batch_list_row in batch_list.iterrows():
                    for index2, wafer_lot_row in wafer_lot.iterrows():
                        if batch_list_row['LOTNO'].find(wafer_lot_row['Z_WAFER_LOT'].replace("%",""))!=(-1):
                            if wafer_lot_row['Z_TYPE']==1:
                                if pd.isnull(wafer_lot_row['Z_ASSY_DEV_TYPE']) or\
                                    demandform_row['Z_ASSY_DEV_TYPE'].find(wafer_lot_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                                    drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]
                                    if len(drop_list)==0:
                                        continue
                                    if not pd.isnull(wafer_lot_row['Z_W_SERIAL_NO']):
                                        wafer_num_list = str(wafer_lot_row['Z_W_SERIAL_NO']).split(',')
                                        temp = {
                                            'pri': wafer_lot_row['Z_PRIORITY'],
                                            'index':[]
                                        }
                                        if waferID_flag:
                                            temp_batch = SplitWaferID.getSplitWaferID([drop_list], input_parameter, -1)
                                            drop_list = temp_batch
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            if str(drop_row['ZWAFER_ID']) in wafer_num_list:
                                                temp['index'].append(batch_drop_index)
                                                instr_time = time.localtime()
                                                log_str = "lot selection:{}, first use: RTNO:{},LOTNO:{},ID:{}".format(index2+1,drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                log_list2.append(info_list)
                                        first_use_index.append(temp)
                                    else:
                                        temp = {
                                            'pri': wafer_lot_row['Z_PRIORITY'],
                                            'index':[]
                                        }
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            temp['index'].append(batch_drop_index)
                                            instr_time = time.localtime()
                                            log_str = "lot selection:{}, first use: RTNO:{},LOTNO:{},ID:{}".format(index2+1,drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                            log_list2.append(info_list)
                                        first_use_index.append(temp)
                            elif wafer_lot_row['Z_TYPE']==2:
                                if demandform_row['Z_ASSY_DEV_TYPE'].find(wafer_lot_row['Z_ASSY_DEV_TYPE'].strip('%'))==-1:
                                    drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]
                                    
                                    if waferID_flag:
                                        temp_batch = SplitWaferID.getSplitWaferID([drop_list], input_parameter, -1)
                                        drop_list = temp_batch
                                    if not pd.isnull(wafer_lot_row['Z_W_SERIAL_NO']):
                                        wafer_num_list = str(wafer_lot_row['Z_W_SERIAL_NO']).split(',')
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            for num in wafer_num_list:
                                                id_str = str(drop_row['ZWAFER_ID'])
                                                if id_str.find(num)!=-1:
                                                    batch_pc_list = batch_pc_list.drop(batch_drop_index)
                                                    instr_time = time.localtime()
                                                    log_str = "lot selection:{}, spicific use in ASSY_DEV:{}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index2+1,wafer_lot_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                    info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                    log_list2.append(info_list)
                                                    for row in first_use_index:
                                                        if batch_drop_index in row['index']:
                                                            row['index'].remove(batch_drop_index)
                                    else:
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            batch_pc_list = batch_pc_list.drop(batch_drop_index)
                                            instr_time = time.localtime()
                                            log_str = "lot selection:{}, spicific use in ASSY_DEV:{}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index2+1,wafer_lot_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                            log_list2.append(info_list)
                                            for row in first_use_index:
                                                if batch_drop_index in row['index']:
                                                    row['index'].remove(batch_drop_index)
                            else:
                                if pd.isnull(wafer_lot_row['Z_ASSY_DEV_TYPE']) or\
                                    demandform_row['Z_ASSY_DEV_TYPE'].find(wafer_lot_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                                    drop_list = batch_pc_list[batch_pc_list['RTNO']==batch_list_row['RTNO']]
                                    
                                    if waferID_flag:
                                        temp_batch = SplitWaferID.getSplitWaferID([drop_list], input_parameter, -1)
                                        drop_list = temp_batch
                                    if not pd.isnull(wafer_lot_row['Z_W_SERIAL_NO']):
                                        wafer_num_list = str(wafer_lot_row['Z_W_SERIAL_NO']).split(',')
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            for num in wafer_num_list:
                                                id_str = str(drop_row['ZWAFER_ID'])
                                                if id_str.find(num)!=-1:
                                                    batch_pc_list = batch_pc_list.drop(batch_drop_index)
                                                    instr_time = time.localtime()
                                                    log_str = "lot selection:{}, can not use in ASSY_DEV:{}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index2+1,wafer_lot_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                                    info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                                    log_list2.append(info_list)

                                                    for row in first_use_index:
                                                        if batch_drop_index in row['index']:
                                                            row['index'].remove(batch_drop_index)
                                    else:
                                        for batch_drop_index, drop_row in drop_list.iterrows():
                                            batch_pc_list = batch_pc_list.drop(batch_drop_index)    
                                            instr_time = time.localtime()
                                            log_str = "lot selection:{}, can not use in ASSY_DEV:{}, 排除:RTNO:{},LOTNO:{},ID:{}".format(index2+1,wafer_lot_row['Z_ASSY_DEV_TYPE'],drop_row['RTNO'],drop_row['LOTNO'],drop_row['ZWAFER_ID'])
                                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                            log_list2.append(info_list)
                                            for row in first_use_index:
                                                if batch_drop_index in row['index']:
                                                    row['index'].remove(batch_drop_index)
                temp2 = None
                i_index = 1000
                if len(first_use_index)!=0:
                    if first_use_index[0]['pri']!='%':
                        num = 1
                        for row in first_use_index:
                            if pd.isnull(row['pri']):
                                row['pri'] = num
                                num += 1
                            i_index = int(row['pri']) if int(row['pri'])<i_index else i_index
                for num in range(len(first_use_index)):
                    for row in first_use_index:
                        row['pri'] = int(row['pri']) if type(row['pri'])!=str else row['pri']
                        if str(row['pri'])==str(num+i_index) or str(row['pri'])=='%':
                            temp = batch_pc_list.loc[row['index']]
                            batch_pc_list = batch_pc_list.drop(row['index'])
                            if temp2 is None:
                                temp2 = temp
                            else:
                                temp2 = pd.concat([temp2, temp], axis=0)
                            break
                batch_pc_list = pd.concat([temp2, batch_pc_list], axis=0)

                # 開始分配產品
                pre_wafer_id = ""      # 前一片的wafer id
                pre_batch_id = ""      # 前一片的batch id
                pre_lot_id = ""      # 前一片的lot id
                batch_die_count = 0    # 統計同一個batch中的總die量
                batch_pc_count = 0     # 統計同一個batch中的總片數
                same_wafer = []     #儲存的是相同wafer id但不同的bin號
                grind_thickness_std = demandform_row['die device'][die_device_index]['GRIND_THI_ASS_QTY']      # 該產品要求的研磨厚度
                grind_thickness_max = demandform_row['die device'][die_device_index]['GRIND_THI_MAX_QTY']
                grind_thickness_min = demandform_row['die device'][die_device_index]['GRIND_THI_MIN_QTY']
                die_seq = demandform_row['die device'][die_device_index]['DIE_SEQ']
                film_number = str(int(demandform_row['die device'][die_device_index]['CMP_ITM_ID'])) if\
                    not pd.isnull(demandform_row['die device'][die_device_index]['CMP_ITM_ID']) else "00" # 該產品要求的film type
                film_type = demandform_row['die device'][die_device_index]['CMP_ITM_NAME']

                temp_wafer = []      #儲存的是相同batch id但不同的wafer id
                temp_all_wafer = []   #儲存的是不同batch id
                temp_batch_die = []   #儲存的是不同batch id的統計資料
                temp_drop = []        # 用來存該片的data frame index，如果確認可以分配，則讀取這個並從inventory中移除
                # 抓取第一片的截止日期
                if len(batch_pc_list)!=0:
                    str_grdate = str((batch_pc_list.loc[:,'ZGRDATE'].tolist())[0])
                    fst_grdate = datetime.datetime( int(str_grdate[0:4]), int(str_grdate[4:6]), int(str_grdate[6:8])) 
                else:
                    continue

                left_wafer = True        # 當全部 batch_pc_list 走完後沒有進到中間的條件，需在最後走一次條件
                finish_distribute = False    #完成分配後則直接跳出迴圈
                total_space = 0           # 日期區間發生改變時，統計之前日期區間的總量
                bound_grdate = fst_grdate +  datetime.timedelta(days=1)    # 第一片的日期區間，與區間間隔
                prev_grdate = fst_grdate           # 前一個日期區間的日期
                for row_index, batch_pc_list_row in batch_pc_list.iterrows():
                    # 如果餘量已經等餘0，直接跳過
                    if int(batch_pc_list_row['Z_REST_DIE'])==0:
                        instr_time = time.localtime()
                        log_str = "餘die為0, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                        info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                        log_list2.append(info_list)
                        continue
                    # if customer is different
                    if (not pd.isnull(batch_pc_list_row['KUNNR'])) and batch_pc_list_row['KUNNR']!=demandform_row['KUNNR']:
                        instr_time = time.localtime()
                        log_str = "KUNNR not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                        info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                        log_list2.append(info_list)
                        continue
                    # if grind thinkness is not fit
                    if (not pd.isnull(grind_thickness_std)) and (not pd.isnull(batch_pc_list_row['GRIND_THINK'])): 
                        if grind_thickness_std!=0:
                            if batch_pc_list_row['GRIND_THINK']>=grind_thickness_max or batch_pc_list_row['GRIND_THINK']<=grind_thickness_min:
                                instr_time = time.localtime()
                                log_str = "GRIND_THINK not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                log_list2.append(info_list)
                                continue
                    #if film type is not fit
                    if (not pd.isnull(film_type)) and (not pd.isnull(batch_pc_list_row['MTRL_TYPE'])):
                        if film_type!=batch_pc_list_row['MTRL_TYPE'] and (film_number[0:2]!="14" or batch_pc_list_row['film number'][0:2]!="14"):
                            instr_time = time.localtime()
                            log_str = "film type not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                            log_list2.append(info_list)
                            continue

                    # 如果有 MATNR 但是film type和GRIND_THINK為空，需要去PID找資訊
                    if ((not pd.isnull(grind_thickness_std)) or (not pd.isnull(film_type))) and (not pd.isnull(batch_pc_list_row['MATNR'])):
                        if (pd.isnull(batch_pc_list_row['MTRL_TYPE']) and pd.isnull(batch_pc_list_row['GRIND_THINK'])) or\
                            film_number[0:2]=="14":
                            match_pid = pid[pid['Z_MFGID'].str.contains(batch_pc_list_row['MATNR'])]
                            temp_pid = match_pid.drop_duplicates('DIE_SEQ','first').sort_values(by=['DIE_SEQ'])

                            # 如果用inventory中的MATNR欄位找不到PID對應，則繼續分配
                            if len(match_pid)!=0:
                                if len(temp_pid)==1:
                                    die_seq = 0
                                else:
                                    if die_seq==0:
                                        die_seq = 1
                                match_pid = match_pid[match_pid['DIE_SEQ']==die_seq]

                                d = dict(match_pid.iloc[0])
                                temp_film_type = d['CMP_ITM_NAME']
                                temp_film_num = str(d['CMP_ITM_ID']) if not pd.isnull(d['CMP_ITM_ID']) else "00"
                                temp_gring = d['GRIND_THI_ASS_QTY']

                                if (not pd.isnull(grind_thickness_std)) and (not pd.isnull(temp_gring)):
                                    if grind_thickness_std!=0:
                                        if temp_gring>=grind_thickness_max or temp_gring<=grind_thickness_min:
                                            instr_time = time.localtime()
                                            log_str = "GRIND_THINK not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                                            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                            log_list2.append(info_list)
                                            continue
                                if (not pd.isnull(film_type)) and (not pd.isnull(temp_film_type)):
                                    if film_type!=temp_film_type and (film_number[0:2]!="14" or temp_film_num[0:2]!="14"):
                                        instr_time = time.localtime()
                                        log_str = "film type not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                                        info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                        log_list2.append(info_list)
                                        continue
                            else:
                                instr_time = time.localtime()
                                log_str = "MATNR not fount in PID, MATNR:{},RTNO:{},ID:{}".format(batch_pc_list_row['MATNR'],batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                log_list2.append(info_list)
                                demandform_row['msg'] = 8
                                MATNR_flag = True
                                break

                    # if lot limit group is different
                    if (batch_pc_list_row['lot limit']!=0) and demandform_row['die device'][die_device_index]['CUST_INST_NO']!=batch_pc_list_row['lot limit']:
                        instr_time = time.localtime()
                        log_str = "lot limit not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                        info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                        log_list2.append(info_list)
                        continue
                    # if device need bumping wafer and bumping flag is not set
                    if len(demandform_row['die device'])==1:
                        device_num = 1
                    else:
                        device_num = demandform_row['die device'][die_device_index]['DIE_SEQ']
                    # 如果有Z_BUMP_FLAG條件，需判斷是否可用
                    flag = False
                    for each_device in demandform_row['device setting'][device_num]:
                        if each_device['Z_WAFER_DEV_TYPE']==batch_pc_list_row['Z_DEV_TYP'] and each_device['Z_BUMP_FLAG']=="X" and batch_pc_list_row['ZBUMPING']!="X":
                            flag = True
                            break
                    if flag:
                        instr_time = time.localtime()
                        log_str = "bump flag not fit, 排除:RTNO:{},ID:{}".format(batch_pc_list_row['RTNO'],batch_pc_list_row['ZWAFER_ID'])
                        info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                        log_list2.append(info_list)
                        continue

                    # 定義一開始的變數
                    if batch_die_count==0:
                        pre_batch_id = batch_pc_list_row['RTNO']
                        pre_wafer_id = batch_pc_list_row['ZWAFER_ID']
                        pre_lot_id = batch_pc_list_row['LOTNO']
                    batch_id = batch_pc_list_row['RTNO']
                    wafer_id = batch_pc_list_row['ZWAFER_ID']
                    lot_id = batch_pc_list_row['LOTNO']

                    # 如果當前這片wafer id與前一片不同，進行合併
                    if pre_wafer_id!=wafer_id or\
                        (pre_wafer_id==wafer_id and pre_batch_id!=batch_id):
                        temp_wafer.append(same_wafer)
                        same_wafer = []
                        pre_wafer_id = wafer_id
                        batch_pc_count += 1

                    # 如果當前這片batch id與前一片不同，且已經有分配，進行合併，目的為止紀錄單個batch的所需資訊
                    if batch_die_count!=0 and pre_batch_id!=batch_id:
                        batch_die_total = {
                            'substitute': 0,
                            'batch id' : pre_batch_id,
                            'lot id': pre_lot_id,
                            'pc' : batch_pc_count,
                            'count' : batch_die_count,
                            'drop_index' : temp_drop,
                            'partial' : []
                        }
                        temp_batch_die.append(batch_die_total)
                        temp_all_wafer.append(temp_wafer)
                        pre_batch_id = batch_id
                        pre_lot_id = lot_id
                        pre_wafer_id = wafer_id
                        batch_die_count = 0
                        batch_pc_count = 0
                        temp_drop = []
                        temp_wafer = []

                    # cut the space with bound grdate
                    zgrdata = str(batch_pc_list_row['ZGRDATE'])
                    pc_grdate = datetime.datetime( int(zgrdata[0:4]), int(zgrdata[4:6]), int(zgrdata[6:8]))
                    if pc_grdate != prev_grdate:
                        # modify the space of date
                        # 如果前一個日期區間的日期比現在的大，調整前一個日期(先用的狀況)
                        if pc_grdate < prev_grdate:
                            prev_grdate = pc_grdate
                        # 如果前一個日期區間的日期比現在的小，使前一個日期依照依照步伐往後面移動(QCM步伐=1，MTK=3)
                        else:
                            while pc_grdate > prev_grdate:
                                prev_grdate = prev_grdate + datetime.timedelta(days=1)
                        # 如果累加量+當前量已經大於等於所需demand，進行動態規劃
                        if total_space + current_qty >= total_qty * die_device_ratio[ratio_index] and demandform_row['Z_OVER']!="Y":
                            index = 0          #drop index
                            batch_wafer = []   # 最終被使用的全部batch資訊(暫存用)
                            die_count = 0      # 當前日期區間被分配的總die量
                            pc_count = 0       # 當前日期區間被分配的總片量
                            drop_index = []
                            qty_type_count = 0  # 紀錄EA/PC當前的數量
                            partial_index = []

                            # 如果日期區間中只有一個batch，則直接分配
                            if len(temp_batch_die)==1:
                                release_batch = False
                                # 如果有需要，切割wafer id
                                if waferID_flag:
                                    temp_all_wafer = SplitWaferID.getSplitWaferID(temp_all_wafer, input_parameter)
                                # 從當前日期區間的batch開始分配
                                for batch_row in temp_all_wafer:
                                    current_batch = []
                                    for wafer_row in batch_row:
                                        wafer_array = []
                                        for wafer_bin in wafer_row:
                                            # qty_type_count根據EA/PC紀錄不同的值
                                            # ratio用來決定EA/PC要取的值是 Z_REST_DIE 或一片
                                            if demandform_row['qty type']=='EA':
                                                qty_type_count = die_count
                                                ratio = wafer_bin['Z_REST_DIE']
                                            else:
                                                qty_type_count = pc_count
                                                ratio = 1
                                            
                                            # 如果累加量+當前日期區間量+該片wafer小於demand，加總 
                                            if total_space + qty_type_count + ratio < total_qty * die_device_ratio[ratio_index]:
                                                die_count += wafer_bin['Z_REST_DIE']
                                                drop_index.append(temp_batch_die[0]['drop_index'][index])
                                                wafer_array.append(wafer_bin)
                                            # 若超過則對該片wafer做調整
                                            else:
                                                # 如果沒有 die release rule，正常處理
                                                if len(demandform_row['die release'])==0 and demandform_row['Z_OVER']!="Y":
                                                    if demandform_row['qty type']=='EA':
                                                        left_die = wafer_bin['Z_REST_DIE'] - (total_qty * die_device_ratio[ratio_index] - die_count - total_space)
                                                        die_count += wafer_bin['Z_REST_DIE'] - left_die
                                                    else:
                                                        left_die = 0
                                                        die_count += wafer_bin['Z_REST_DIE']

                                                    wafer_bin['Z_REST_DIE'] -= left_die
                                                    wafer_array.append(wafer_bin)

                                                    if left_die==0 or demandform_row['qty type']=='PC':
                                                        drop_index.append(temp_batch_die[0]['drop_index'][index])
                                                    else:
                                                        d = dict(inventory.loc[temp_batch_die[0]['drop_index'][index]])
                                                        d['Z_REST_DIE'] = left_die
                                                        if (not pd.isnull(grind_thickness_std)) and (pd.isnull(wafer_bin['GRIND_THINK'])): 
                                                            d['GRIND_THINK'] = grind_thickness_std
                                                        if (not pd.isnull(film_type)) and (pd.isnull(wafer_bin['MTRL_TYPE'])):
                                                            d['MTRL_TYPE'] = film_type
                                                            d['film number'] = film_number
                                                        if lot_limit_flag:
                                                            for limit in lot_limit_list:
                                                                if wafer_bin['Z_DEV_TYP'].find(limit)!=(-1) and (wafer_bin['lot limit']==0):
                                                                    d['lot limit'] = demandform_row['die device'][die_device_index]['CUST_INST_NO']
                                                                    break
                                                        inventory.loc[temp_batch_die[0]['drop_index'][index]] = pd.Series(d)
                                                        partial_index.append(temp_batch_die[0]['drop_index'][index])

                                                    finish_distribute = True
                                                    if demandform_row['qty type']=='EA':
                                                        break
                                                # 如果有die release rule
                                                else:
                                                    # 若是batch則直接保留原本的資訊並結束分配
                                                    if demandform_row['die release']['Z_CONDITION']=='Batch':
                                                        release_batch = True
                                                        die_count = temp_batch_die[0]['count']
                                                        drop_index = temp_batch_die[0]['drop_index']
                                                        finish_distribute = True
                                                        break
                                                    # 如果是wafer id則分配完這一片wafer
                                                    elif demandform_row['die release']['Z_CONDITION']=='WaferID':
                                                        die_count += wafer_bin['Z_REST_DIE']
                                                        drop_index.append(temp_batch_die[0]['drop_index'][index])
                                                        wafer_array.append(wafer_bin)
                                                        finish_distribute = True
                                            index += 1

                                        # 如果 die release rule 是batch，保留原本的資訊
                                        if release_batch:
                                            current_batch = batch_row
                                            pc_count = temp_batch_die[0]['pc']
                                        # 如果不是，則加總當前資訊
                                        else:
                                            current_batch.append(wafer_array)
                                            pc_count += 1

                                        # 如果已完成分配則直接停止
                                        if finish_distribute:
                                            batch_wafer.append(current_batch)
                                            break
                                    if finish_distribute:
                                        break

                                # 將被分配的wafer資訊儲存起來
                                demandform_row['demand wafer'][str(die_level)] += batch_wafer

                                # 統計與分類batch的資訊並儲存起來
                                temp_batch_die[0]['pc'] = pc_count
                                temp_batch_die[0]['count'] = die_count
                                temp_batch_die[0]['drop_index'] = drop_index
                                temp_batch_die[0]['partial'] = partial_index
                                demandform_row['demand batch'][str(die_level)] += temp_batch_die
                                temp_wafer = []
                                temp_batch_die = []
                                if demandform_row['qty type']=='EA':
                                    total_space += die_count
                                else:
                                    total_space += pc_count
                                current_qty = 0
                            # 如果日期區間中有兩個batch以上，則動態規劃
                            else:
                                print("dynamic  " + demandform_row['Z_ASSY_DEV_TYPE'])
                                # 分別設定動態規劃所需參數
                                n = len(temp_batch_die)  # 物品數量
                                w = int(total_qty * die_device_ratio[ratio_index] - total_space)    # 背包載重(剩餘未分配結果)
                                cost = np.ones(n,dtype=int)   # 物品權重
                                weight = []    # 物品價值(每一個batch的總die量)
                                for batch_row in temp_batch_die:
                                    if demandform_row['qty type'] == "PC":
                                        if batch_row['pc'] >= w:
                                            weight.append(w)
                                        else:
                                            weight.append(int(batch_row['pc']))
                                            
                                    else:
                                        if batch_row['count'] >= w:
                                            weight.append(w)
                                        else:
                                            weight.append(int(batch_row['count']))

                                # 進行排序，回傳結果會為最接近，!!!但是不會超過demand，因此需要再補一個batch
                                # re是被取用的batch index
                                re = Knapsack.setKnapsack(n,w,cost,weight)

                                # 計算動態規劃已分配的量，看還缺多少
                                sum = 0
                                substitute = 0
                                for item in re:
                                    sum += weight[item]
                                # 如果分配量小於demand，要找一個batch來補
                                if sum < w:
                                    for item in range(n):
                                        # 找到最前面不再動態規劃中的batch，分配缺少的量
                                        if item not in re:
                                            re.append(item)
                                            substitute = item
                                            weight[item] = w - sum
                                            break
                                
                                # 依據動態規劃的取法，分配demand
                                release_batch = False

                                batch_infor = []     # 儲存全部被分配batch的資訊(暫存用)
                                # 走訪當前日期區間中全部的batch
                                for batch_row_index in range(len(temp_all_wafer)):
                                    # 如果有超投flag，則全部投完
                                    if demandform_row['Z_OVER']!="Y":
                                        # 如果該片batch沒有在動態規劃的list中，則跳過
                                        if batch_row_index not in re:
                                            continue
                                        # 若有，則將其從list中移除並分配該batch
                                        re.remove(batch_row_index)

                                    index = 0       # drop index
                                    die_count = 0      # 當前總die量
                                    pc_count = 0       # 當前總片量
                                    drop_index = []
                                    partial_index = []
                                    current_batch = []     # 儲存當前batch中被分配的wafer資訊
                                    next_batch = False     # 如果當前batch已分配完畢，直接跳下一個batch
                                    qty_type_count = 0   # 紀錄EA/PC當前的數量
                                    
                                    # 進行wafer id切割
                                    if waferID_flag:
                                        temp_batch = SplitWaferID.getSplitWaferID(temp_all_wafer, input_parameter, batch_row_index)
                                        temp_all_wafer[batch_row_index] = temp_batch

                                    # 讀取目標batch中wafer資訊
                                    for wafer_row in temp_all_wafer[batch_row_index]:
                                        wafer_array = []
                                        # 讀取 wafer 中 bin 資訊
                                        for wafer_bin in wafer_row:
                                            if demandform_row['qty type']=='EA':
                                                qty_type_count = die_count
                                                ratio = wafer_bin['Z_REST_DIE']
                                            else:
                                                qty_type_count = pc_count
                                                ratio = 1
                                            # 如果當前量+該片wafer小於動態規劃後這一個batch須被分配的量，加總
                                            if qty_type_count + ratio < weight[batch_row_index]:
                                                die_count += wafer_bin['Z_REST_DIE']
                                                drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                                wafer_array.append(wafer_bin)
                                            # 如果當前量+該片wafer等於動態規劃後這一個batch須被分配的量，加總，跳下一個batch
                                            elif qty_type_count + ratio == weight[batch_row_index]:
                                                die_count += wafer_bin['Z_REST_DIE']
                                                drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                                wafer_array.append(wafer_bin)

                                                next_batch = True
                                                if demandform_row['qty type']=='EA':
                                                    break
                                            # 如果當前量+該片wafer大於動態規劃後這一個batch須被分配的量，計算餘量
                                            else:
                                                # 如果沒有有die release rule
                                                if len(demandform_row['die release'])==0 and demandform_row['Z_OVER']!="Y":
                                                    if demandform_row['qty type']=='EA':
                                                        left_die = wafer_bin['Z_REST_DIE'] - (weight[batch_row_index] - die_count)
                                                        die_count += wafer_bin['Z_REST_DIE'] - left_die
                                                    else:
                                                        left_die = 0
                                                        die_count += wafer_bin['Z_REST_DIE']
                                                    wafer_bin['Z_REST_DIE'] -= left_die
                                                    wafer_array.append(wafer_bin)

                                                    # 如果沒有餘量，從inventory中移除
                                                    if left_die==0 or demandform_row['qty type']=='PC':
                                                        drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                                    # 如果有餘量，則修改inventory
                                                    else:
                                                        d = dict(inventory.loc[temp_batch_die[batch_row_index]['drop_index'][index]])
                                                        d['Z_REST_DIE'] = left_die
                                                        if (not pd.isnull(grind_thickness_std)) and (pd.isnull(wafer_bin['GRIND_THINK'])): 
                                                            d['GRIND_THINK'] = grind_thickness_std
                                                        if (not pd.isnull(film_type)) and (pd.isnull(wafer_bin['MTRL_TYPE'])):
                                                            d['MTRL_TYPE'] = film_type
                                                            d['film number'] = film_number
                                                        if lot_limit_flag:
                                                            for limit in lot_limit_list:
                                                                if wafer_bin['Z_DEV_TYP'].find(limit)!=(-1) and (wafer_bin['lot limit']==0):
                                                                    d['lot limit'] = demandform_row['die device'][die_device_index]['CUST_INST_NO']
                                                                    break
                                                        inventory.loc[temp_batch_die[batch_row_index]['drop_index'][index]] = pd.Series(d)
                                                        partial_index.append(temp_batch_die[batch_row_index]['drop_index'][index])

                                                    # 已分配完畢，直接跳下一片batch
                                                    next_batch = True
                                                    if demandform_row['qty type']=='EA':
                                                        break
                                                # 如果有die release rule
                                                else:# 若是batch則直接保留原本的資訊並結束分配
                                                    if demandform_row['die release']['Z_CONDITION']=='Batch':
                                                        release_batch = True
                                                        die_count = temp_batch_die[batch_row_index]['count']
                                                        drop_index = temp_batch_die[batch_row_index]['drop_index']
                                                        next_batch = True
                                                        break
                                                    # 如果是wafer id則分配完這一片wafer或有設條件要超投
                                                    elif demandform_row['die release']['Z_CONDITION']=='WaferID':
                                                        die_count += wafer_bin['Z_REST_DIE']
                                                        drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                                        wafer_array.append(wafer_bin)
                                                        next_batch = True
                                                    
                                            index += 1

                                        # 如果 die release rule 是batch，保留原本的資訊
                                        if release_batch:
                                            current_batch = batch_row
                                            pc_count = temp_batch_die[batch_row_index]['pc']
                                        # 如果不是，則加總當前資訊
                                        else:
                                            current_batch.append(wafer_array)
                                            pc_count += 1

                                        # 若要直接跳下一片batch，保留該batch資訊，跳下一片
                                        if next_batch:
                                            batch_wafer.append(current_batch)
                                            break                                    

                                    # 統計batch的資訊並儲存起來
                                    temp_batch_die[batch_row_index]['pc'] = pc_count
                                    temp_batch_die[batch_row_index]['count'] = die_count
                                    temp_batch_die[batch_row_index]['drop_index'] = drop_index
                                    temp_batch_die[batch_row_index]['partial'] = partial_index
                                    if batch_row_index == substitute:
                                        temp_batch_die[batch_row_index]['substitute'] = 1
                                    batch_infor.append(temp_batch_die[batch_row_index])

                                    if demandform_row['qty type']=='EA':
                                        total_space += die_count
                                    else:
                                        total_space += pc_count

                                    # 如果動態規劃的batch接分配完畢，直接結束
                                    if len(re)==0:
                                        break

                                # 將資訊儲存起來
                                demandform_row['demand wafer'][str(die_level)] += batch_wafer
                                demandform_row['demand batch'][str(die_level)] += batch_infor
                                temp_wafer = []
                                temp_batch_die = []
                                current_qty = 0
                                finish_distribute = True

                            # 因為分配完畢，不需要考慮是否有剩下未分累部分
                            left_wafer = False
                            break
                        # 如果沒有則把當前量累加至累加量
                        else:
                            # 累加已經被分配的部分
                            total_space += current_qty
                            # 若有需要，切分wafer id
                            if waferID_flag:
                                temp_all_wafer = SplitWaferID.getSplitWaferID(temp_all_wafer, input_parameter)
                            demandform_row['demand wafer'][str(die_level)] += temp_all_wafer
                            demandform_row['demand batch'][str(die_level)] += temp_batch_die
                            temp_all_wafer = []
                            temp_batch_die = []
                            current_qty = 0
                    
                    # 若已經動態規劃，則不需再分配
                    if finish_distribute:
                        break

                    # 把當前的wafer資訊加到當前量
                    if demandform_row['qty type']=='EA':
                        current_qty += batch_pc_list_row['Z_REST_DIE']
                    else:
                        current_qty += 1
                    batch_die_count += batch_pc_list_row['Z_REST_DIE']
                    batch_pc_list_row_dict = batch_pc_list_row.to_dict()
                    
                    same_wafer.append(batch_pc_list_row_dict)
                    temp_drop.append(row_index)
                
                if MATNR_flag:
                    break

                # 如果沒有動態規劃到，batch_pc_list 就已分配完畢，則還需整理剩下的batch資訊
                # 以下整理的方法與上面的部份一樣
                if left_wafer and batch_die_count!=0:
                    # 先將最後剩下的batch資訊結合起來
                    temp_wafer.append(same_wafer)
                    batch_pc_count += 1
                    temp_all_wafer.append(temp_wafer)
                    batch_die_total = {
                        'substitute': 0,
                        'batch id' : pre_batch_id,
                        'lot id': pre_lot_id,
                        'pc' : batch_pc_count,
                        'count' : batch_die_count,
                        'drop_index' : temp_drop,
                        'partial' : []
                    }
                    temp_batch_die.append(batch_die_total)

                    # 若剩餘量有超過demand，則進行動態規劃
                    if total_space + current_qty >= total_qty * die_device_ratio[ratio_index] and demandform_row['Z_OVER']!="Y":
                        index = 0          #drop index
                        batch_wafer = []
                        die_count = 0
                        pc_count = 0
                        drop_index = []
                        partial_index = []
                        qty_type_count = 0

                        if len(temp_batch_die)==1:
                            release_batch = False                            
                            if waferID_flag:
                                temp_all_wafer = SplitWaferID.getSplitWaferID(temp_all_wafer, input_parameter)
                            for batch_row in temp_all_wafer:
                                current_batch = []
                                for wafer_row in batch_row:
                                    wafer_array = []
                                    for wafer_bin in wafer_row:
                                        # qty_type_count根據EA/PC紀錄不同的值
                                        # ratio用來決定EA/PC要取的值是 Z_REST_DIE 或一片
                                        if demandform_row['qty type']=='EA':
                                            qty_type_count = die_count
                                            ratio = wafer_bin['Z_REST_DIE']
                                        else:
                                            qty_type_count = pc_count
                                            ratio = 1
                                        if total_space + qty_type_count + ratio < total_qty * die_device_ratio[ratio_index]:
                                            die_count += wafer_bin['Z_REST_DIE']
                                            drop_index.append(temp_batch_die[0]['drop_index'][index])
                                            wafer_array.append(wafer_bin)
                                        else:
                                            if len(demandform_row['die release'])==0 and demandform_row['Z_OVER']!="Y":
                                                if demandform_row['qty type']=='EA':
                                                    left_die = wafer_bin['Z_REST_DIE'] - (total_qty * die_device_ratio[ratio_index] - die_count - total_space)
                                                    die_count += wafer_bin['Z_REST_DIE'] - left_die
                                                else:
                                                    left_die = 0
                                                    die_count += wafer_bin['Z_REST_DIE']
                                                wafer_bin['Z_REST_DIE'] -= left_die
                                                wafer_array.append(wafer_bin)

                                                if left_die==0 or demandform_row['qty type']=='PC':
                                                    drop_index.append(temp_batch_die[0]['drop_index'][index])
                                                else:
                                                    d = dict(inventory.loc[temp_batch_die[0]['drop_index'][index]])
                                                    d['Z_REST_DIE'] = left_die
                                                    if (not pd.isnull(grind_thickness_std)) and (pd.isnull(wafer_bin['GRIND_THINK'])): 
                                                        d['GRIND_THINK'] = grind_thickness_std
                                                    if (not pd.isnull(film_type)) and (pd.isnull(wafer_bin['MTRL_TYPE'])):
                                                        d['film number'] = film_number
                                                        d['MTRL_TYPE'] = film_type
                                                    if lot_limit_flag:
                                                        for limit in lot_limit_list:
                                                            if wafer_bin['Z_DEV_TYP'].find(limit)!=(-1) and (wafer_bin['lot limit']==0):
                                                                d['lot limit'] = demandform_row['die device'][die_device_index]['CUST_INST_NO']
                                                                break
                                                    inventory.loc[temp_batch_die[0]['drop_index'][index]] = pd.Series(d)
                                                    partial_index.append(temp_batch_die[0]['drop_index'][index])

                                                finish_distribute = True
                                                if demandform_row['qty type']=='EA':
                                                    break
                                            else:
                                                if demandform_row['die release']['Z_CONDITION']=='Batch':
                                                    release_batch = True
                                                    die_count = temp_batch_die[0]['count']
                                                    drop_index = temp_batch_die[0]['drop_index']
                                                    finish_distribute = True
                                                    break
                                                elif demandform_row['die release']['Z_CONDITION']=='WaferID':
                                                    die_count += wafer_bin['Z_REST_DIE']
                                                    drop_index.append(temp_batch_die[0]['drop_index'][index])
                                                    wafer_array.append(wafer_bin)
                                                    finish_distribute = True
                                        index += 1

                                    if release_batch:
                                        current_batch = batch_row
                                        pc_count = temp_batch_die[0]['pc']
                                    else:
                                        current_batch.append(wafer_array)
                                        pc_count += 1

                                    if finish_distribute:
                                        batch_wafer.append(current_batch)
                                        break
                                if finish_distribute:
                                    break

                            demandform_row['demand wafer'][str(die_level)] += batch_wafer

                            temp_batch_die[0]['pc'] = pc_count
                            temp_batch_die[0]['count'] = die_count
                            temp_batch_die[0]['drop_index'] = drop_index
                            temp_batch_die[0]['partial'] = partial_index
                            demandform_row['demand batch'][str(die_level)] += temp_batch_die
                            temp_wafer = []
                            temp_batch_die = []
                            if demandform_row['qty type']=='EA':
                                total_space += die_count
                            else:
                                total_space += pc_count
                            current_qty = 0
                        else:
                            print("dynamic  " + demandform_row['Z_ASSY_DEV_TYPE'])
                            n = len(temp_batch_die)
                            w = int(total_qty * die_device_ratio[ratio_index] - total_space)
                            cost = np.ones(n,dtype=int)
                            weight = []
                            for batch_row in temp_batch_die:
                                if demandform_row['qty type'] == "PC":
                                    if batch_row['pc'] >= w:
                                        weight.append(w)
                                    else:
                                        weight.append(int(batch_row['pc']))
                                            
                                else:
                                    if batch_row['count'] >= w:
                                        weight.append(w)
                                    else:
                                        weight.append(int(batch_row['count']))

                            re = Knapsack.setKnapsack(n,w,cost,weight)

                            sum = 0
                            substitute = 0
                            for item in re:
                                sum += weight[item]
                            if sum < w:
                                for item in range(n):
                                    if item not in re:
                                        re.append(item)
                                        substitute = item
                                        weight[item] = w - sum
                                        break
                                
                            release_batch = False
                            batch_infor = []
                            for batch_row_index in range(len(temp_all_wafer)):
                                if demandform_row['Z_OVER']!="Y":
                                    if batch_row_index not in re:
                                        continue                                
                                    re.remove(batch_row_index)

                                index = 0
                                die_count = 0
                                pc_count = 0
                                drop_index = []
                                partial_index = []
                                current_batch = []
                                next_batch = False
                                qty_type_count = 0   # 紀錄EA/PC當前的數量

                                if waferID_flag:
                                    temp_batch = SplitWaferID.getSplitWaferID(temp_all_wafer, input_parameter, batch_row_index)
                                    temp_all_wafer[batch_row_index] = temp_batch

                                for wafer_row in temp_all_wafer[batch_row_index]:
                                    wafer_array = []
                                    for wafer_bin in wafer_row:
                                        if demandform_row['qty type']=='EA':
                                            qty_type_count = die_count
                                            ratio = wafer_bin['Z_REST_DIE']
                                        else:
                                            qty_type_count = pc_count
                                            ratio = 1
                                        if qty_type_count + ratio < weight[batch_row_index]:
                                            die_count += wafer_bin['Z_REST_DIE']
                                            drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                            wafer_array.append(wafer_bin)
                                        elif qty_type_count + ratio == weight[batch_row_index]:
                                            die_count += wafer_bin['Z_REST_DIE']
                                            drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                            wafer_array.append(wafer_bin)

                                            next_batch = True
                                            if demandform_row['qty type']=='EA':
                                                break
                                        else:
                                            if len(demandform_row['die release'])==0 and demandform_row['Z_OVER']!="Y":
                                                if demandform_row['qty type']=='EA':
                                                    left_die = wafer_bin['Z_REST_DIE'] - (weight[batch_row_index] - die_count)
                                                    die_count += wafer_bin['Z_REST_DIE'] - left_die
                                                else:
                                                    left_die = 0
                                                    die_count += wafer_bin['Z_REST_DIE']
                                                wafer_bin['Z_REST_DIE'] -= left_die
                                                wafer_array.append(wafer_bin)

                                                if left_die==0 or demandform_row['qty type']=='PC':
                                                    drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                                else:
                                                    d = dict(inventory.loc[temp_batch_die[batch_row_index]['drop_index'][index]])
                                                    d['Z_REST_DIE'] = left_die
                                                    if (not pd.isnull(grind_thickness_std)) and (pd.isnull(wafer_bin['GRIND_THINK'])): 
                                                        d['GRIND_THINK'] = grind_thickness_std
                                                    if (not pd.isnull(film_type)) and (pd.isnull(wafer_bin['MTRL_TYPE'])):
                                                        d['film number'] = film_number
                                                        d['MTRL_TYPE'] = film_type
                                                    if lot_limit_flag:
                                                        for limit in lot_limit_list:
                                                            if wafer_bin['Z_DEV_TYP'].find(limit)!=(-1) and (wafer_bin['lot limit']==0):
                                                                d['lot limit'] = demandform_row['die device'][die_device_index]['CUST_INST_NO']
                                                                break
                                                    inventory.loc[temp_batch_die[batch_row_index]['drop_index'][index]] = pd.Series(d)
                                                    partial_index.append(temp_batch_die[batch_row_index]['drop_index'][index])

                                                next_batch = True
                                                if demandform_row['qty type']=='EA':
                                                    break
                                            else:
                                                if demandform_row['die release']['Z_CONDITION']=='Batch':
                                                    release_batch = True
                                                    die_count = temp_batch_die[batch_row_index]['count']
                                                    drop_index = temp_batch_die[batch_row_index]['drop_index']
                                                    next_batch = True
                                                    break
                                                elif demandform_row['die release']['Z_CONDITION']=='WaferID':
                                                    die_count += wafer_bin['Z_REST_DIE']
                                                    drop_index.append(temp_batch_die[batch_row_index]['drop_index'][index])
                                                    wafer_array.append(wafer_bin)
                                                    next_batch = True
                                        index += 1

                                    if release_batch:
                                        current_batch = batch_row
                                        pc_count = temp_batch_die[batch_row_index]['pc']
                                    else:
                                        current_batch.append(wafer_array)
                                        pc_count += 1
                                        
                                    if next_batch:
                                        batch_wafer.append(current_batch)
                                        break                                    

                                temp_batch_die[batch_row_index]['pc'] = pc_count
                                temp_batch_die[batch_row_index]['count'] = die_count
                                temp_batch_die[batch_row_index]['drop_index'] = drop_index
                                temp_batch_die[batch_row_index]['partial'] = partial_index
                                if batch_row_index == substitute:
                                    temp_batch_die[batch_row_index]['substitute'] = 1
                                batch_infor.append(temp_batch_die[batch_row_index])
                                if demandform_row['qty type']=='EA':
                                    total_space += die_count
                                else:
                                    total_space += pc_count

                                if len(re)==0:
                                    break

                            demandform_row['demand wafer'][str(die_level)] += batch_wafer
                            demandform_row['demand batch'][str(die_level)] += batch_infor
                            temp_wafer = []
                            temp_batch_die = []
                            current_qty = 0
                            finish_distribute = True
                    # 若無則直接記錄下來
                    else:
                        if waferID_flag:
                            temp_all_wafer = SplitWaferID.getSplitWaferID(temp_all_wafer, input_parameter)
                        demandform_row['demand wafer'][str(die_level)] += temp_all_wafer
                        demandform_row['demand batch'][str(die_level)] += temp_batch_die
                
                # 如果有設定priority或有要超投則修改inventory
                if (not pd.isnull(demandform_row['Z_PRIORITY'])) or\
                    (not pd.isnull(demandform_row['Z_OVER'])) or\
                    total_space + current_qty >= total_qty * die_device_ratio[ratio_index]:
                        # 如果產品是因為priority而分配，但是沒有超過demand，另外紀錄，最後對全部的device做修正
                        if total_space + current_qty < total_qty * die_device_ratio[ratio_index]:
                            is_shortage = True
                        
                        die_release_num_list.append( (total_space + current_qty)//die_device_ratio[ratio_index] )
                        for batch_row in demandform_row['demand batch'][str(die_level)]:
                            inventory = inventory.drop(batch_row['drop_index'])
                        #inventory = inventory.reset_index(drop=True)
                # 若無則還原inventory(目的為排除同group沒有priority的部分)
                else: 
                    # 找到有相同group的產品
                    drop_index = []
                    for temp_index in range(len(demandform)):
                        if demandform[temp_index]['Z_DEMAND_GRP']==demandform_row['Z_DEMAND_GRP']:
                            drop_index.append(temp_index)

                    # 如果相同group的不只一個，將相同group的產品資訊移除
                    if len(drop_index)!=1:
                        for temp_index in drop_index:
                            demandform[temp_index]['msg'] = 4
                            try:
                                for index in range(len(demandform[temp_index]['demand wafer'])):
                                    demandform[temp_index]['demand wafer'][str(index+1)] = []
                                for index in range(len(demandform[temp_index]['demand batch'])):
                                    demandform[temp_index]['demand batch'][str(index+1)] = []
                            except:
                                print(demandform[temp_index]['Z_DEMAND_GRP'])                            

                            # 標記為已分配(為了不再重複跑同group)
                            demandform[temp_index]['split'] = 1

                        # 還原分配本次產品被消耗的inventory
                        inventory = original_inventory
                    else:
                        die_release_num_list.append( (total_space + current_qty)//die_device_ratio[ratio_index] )

                demandform_row['current qty'] = (total_space + current_qty)//die_device_ratio[ratio_index]

        if MATNR_flag:
            device_log = {
                demandform_row['Z_ASSY_DEV_TYPE']:log_list2,
                demandform_row['Z_ITEM']:""
            }
            log2['distribution'].append(device_log)
            continue

        die_release_num_list = list(set(die_release_num_list))
        # 如果有其中一個device setting發生shortage，調整其他的device setting
        if (is_shortage or len(die_release_num_list)!=1):
            inventory = temp_inventory
            device_distribute = []
            minimal = 1000000
            # 計算每一個device setting各被分配多少量，並且找出其中最少的(應為shortage)
            # die_level_list 為分配過程中使用到的PID
            for die_level_index in range(len(die_level_list)):
                num = 0
                for batch_row in demandform_row['demand batch'][str(die_level_list[die_level_index])]:
                    if demandform_row['qty type']=="EA":
                        num += batch_row['count']
                    else:
                        num += batch_row['pc']
                num2 = num // die_device_ratio[die_level_index]
                device_distribute.append(num)
                if num2 < minimal:
                    minimal = num2

            # 如果有其中一個device setting找不到原料或已被用完，則不繼續調整其他device setting
            if minimal!=0:
                # 依序檢查device setting是否有超過最小的可分配量
                for device_index in range(len(device_distribute)):
                    #若超過了則重新分配
                    if device_distribute[device_index]> minimal * die_device_ratio[device_index]:
                        temp_wafer_info = []
                        temp_batch_info = []
                        current_batch = []
                        count = 0
                        index = 0
                        finish_distribute = False
                        grind_thickness_std = demandform_row['die device'][device_index]['GRIND_THI_ASS_QTY']      # 該產品要求的研磨厚度
                        film_type = demandform_row['die device'][device_index]['CMP_ITM_NAME']
                        film_number = str(int(demandform_row['die device'][device_index]['CMP_ITM_ID'])) if\
                            not pd.isnull(demandform_row['die device'][device_index]['CMP_ITM_ID']) else "00"  # 該產品要求的film type
                        # 讀取整理的batch資訊
                        for batch_row_index in range(len(demandform_row['demand batch'][str(die_level_list[device_index])])):
                            if demandform_row['qty type']=="EA":
                                current = demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['count']
                            else:
                                current = demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['pc']
                            # 如果當前量+該batch沒有超過，則保留原本batch資訊
                            if count + current < minimal * die_device_ratio[device_index]:
                                temp_wafer_info.append(demandform_row['demand wafer'][str(die_level_list[device_index])][batch_row_index])
                                temp_batch_info.append(demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index])
                                count += current
                            # 超過了則重新分配該batch
                            else:
                                index = 0
                                drop_index = []
                                die_count = 0      # 當前日期區間被分配的總die量
                                pc_count = 0       # 當前日期區間被分配的總片量
                                for wafer_row in demandform_row['demand wafer'][str(die_level_list[device_index])][batch_row_index]:
                                    wafer_array = []
                                    for wafer_bin in wafer_row:
                                        if demandform_row['qty type']=='EA':
                                            qty_type_count = die_count
                                            ratio = wafer_bin['Z_REST_DIE']
                                        else:
                                            qty_type_count = pc_count
                                            ratio = 1
                                        if count + qty_type_count + ratio < minimal * die_device_ratio[device_index]:
                                            die_count += wafer_bin['Z_REST_DIE']
                                            drop_index.append(demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['drop_index'][index])
                                            wafer_array.append(wafer_bin)
                                        else:
                                            if demandform_row['qty type']=='EA':
                                                left_die = wafer_bin['Z_REST_DIE'] - (minimal * die_device_ratio[device_index] - die_count - count)
                                                die_count += wafer_bin['Z_REST_DIE'] - left_die
                                            else:
                                                left_die = 0
                                                die_count += wafer_bin['Z_REST_DIE']

                                            wafer_bin['Z_REST_DIE'] -= left_die
                                            wafer_array.append(wafer_bin)

                                            if left_die==0 or demandform_row['qty type']=='PC':
                                                drop_index.append(demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['drop_index'][index])
                                            else:
                                                if len(demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['drop_index'])==index:
                                                    d_index = demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['partial'][0]
                                                else:
                                                    d_index = demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['drop_index'][index]
                                                d = dict(inventory.loc[d_index])
                                                d['Z_REST_DIE'] = left_die
                                                if (not pd.isnull(grind_thickness_std)) and (pd.isnull(wafer_bin['GRIND_THINK'])): 
                                                    d['GRIND_THINK'] = grind_thickness_std
                                                if (not pd.isnull(film_type)) and (pd.isnull(wafer_bin['MTRL_TYPE'])):
                                                    d['film number'] = film_number
                                                    d['MTRL_TYPE'] = film_type
                                                if lot_limit_flag:
                                                    for limit in lot_limit_list:
                                                        if wafer_bin['Z_DEV_TYP'].find(limit)!=(-1) and (wafer_bin['lot limit']==0):
                                                            d['lot limit'] = demandform_row['die device'][device_index]['CUST_INST_NO']
                                                            break
                                                inventory.loc[d_index] = pd.Series(d)

                                            finish_distribute = True
                                            if demandform_row['qty type']=='EA':
                                                break
                                        index += 1
                                    # 儲存當前的wafer(同一個batch)
                                    current_batch.append(wafer_array)
                                    pc_count += 1

                                    # 如果到達分配上限，則停止分配
                                    if finish_distribute:
                                        break 
                                
                                # 將更新的batch資訊整理，紀錄起來
                                temp_wafer_info.append(current_batch)
                                batch_die_total = {
                                    'substitute': 0,
                                    'batch id' : demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['batch id'],
                                    'lot id': demandform_row['demand batch'][str(die_level_list[device_index])][batch_row_index]['lot id'],
                                    'pc' : pc_count,
                                    'count' : die_count,
                                    'drop_index' : drop_index,
                                    'partial' : [d_index]                                
                                }
                                temp_batch_info.append(batch_die_total)
                                break

                        demandform_row['demand wafer'][str(die_level_list[device_index])] = temp_wafer_info
                        demandform_row['demand batch'][str(die_level_list[device_index])] = temp_batch_info

                    for batch_row in demandform_row['demand batch'][str(die_level_list[device_index])]:
                        inventory = inventory.drop(batch_row['drop_index'])
                #inventory = inventory.reset_index(drop=True)
                demandform_row['current qty'] = minimal

        # 若沒有額外問題，標記為已分配
        demandform_row['split'] = 1

        device_log = {
            demandform_row['Z_ASSY_DEV_TYPE']:log_list2,
            demandform_row['Z_ITEM']:""
        }
        log2['distribution'].append(device_log)
    
    end_time = time.time()
    exe_time = round(end_time - start_time, 2)
    start_list = [time.strftime("%Y%m%d", time.localtime(start_time)),time.strftime("%H%M%S", time.localtime(start_time))]
    end_list = [time.strftime("%Y%m%d", time.localtime(end_time)),time.strftime("%H%M%S", time.localtime(end_time))]
    log2["exe time"].append([start_list, end_list, exe_time])

    all_log_2.append(log2)

    
    # 檢查分配結果中是否有shortage的部分
    for demandform_index in range(len(demandform)):
        # 如果有發生 WO creation、priority、group等問題，則不需再考慮shortage
        if demandform[demandform_index]['msg']!=0:
            continue

        # 如果是mcm
        if len(demandform[demandform_index]['die device'])!=1:
            check_list = []
            # 檢查每一個device是否都有分配符合demand
            die_device_index = 0
            for ratio_index in range(len(demandform[demandform_index]['device combine']['device_ratio'])):
                if ratio_index!=0:
                    die_device_index += demandform[demandform_index]['device combine']['device_ratio'][ratio_index-1]
                if len(demandform[demandform_index]['die device'])==1:
                    die_level = 1
                else:
                    die_level = (demandform[demandform_index]['die device'][(die_device_index)]['DIE_SEQ'])

                total = demandform[demandform_index]['device combine']['device_ratio'][ratio_index] * demandform[demandform_index]['current qty']
                qty = 0
                if len(demandform[demandform_index]['demand batch'])!=0:
                    for batch_row in demandform[demandform_index]['demand batch'][str(die_level)]:
                        qty += batch_row['count']

                # 如果分配量為0，代表已經有缺原料
                if qty == 0:
                    check_list.append(0)
                # 如果分配量小於demand，代表有shortage
                elif qty < total:
                    check_list.append(1)
                # 若無，則符合demand
                else:
                    check_list.append(2)

            if 0 in check_list:
                demandform[demandform_index]['msg'] = 7
            elif 1 in check_list:
                demandform[demandform_index]['msg'] = 6
        # 如果是single             
        else:
            # 檢查最後分配結果是否有符合demand
            if demandform[demandform_index]['qty type']=="EA":
                if demandform[demandform_index]['current qty']==0:
                    demandform[demandform_index]['msg'] = 7
                elif demandform[demandform_index]['current qty'] < demandform[demandform_index]['Z_DIE_QTY']:
                    demandform[demandform_index]['msg'] = 6
            else:
                if demandform[demandform_index]['current qty']==0:
                    demandform[demandform_index]['msg'] = 7
                elif demandform[demandform_index]['current qty'] < demandform[demandform_index]['Z_WAFER_QTY']:
                    demandform[demandform_index]['msg'] = 6

    buf = os.path.join(result_folder, "demand distribute{}.json".format(customer))
    with open(buf, 'w') as outfile:
        json.dump(demandform, outfile, sort_keys=False, indent=4)
    
    return demandform, all_log_2
