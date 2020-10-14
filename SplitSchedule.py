 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json
import datetime
import copy
import time
from itertools import combinations

import RWcsv
import orcl_connect

config = pd.read_csv('./config.csv')

# 切分結果
def splitSchedule(demandform, data_folder, result_folder, split_rule_filename, split_schedule_filename, split_wafer_id_filename, comebine_rule_name, input_parameter):
    all_log_2 = []

    # load necessary file
    # 讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        split_rule = RWcsv.readCSV(split_rule_filename, data_folder)
    elif config['RUN_WAY'][0] == 'ORACLE':
        split_rule = orcl_connect.getSplitRule( orcl_connect.getOrclConnectCur(), input_parameter )
    print("load split rule success.")

    # 讀取資料的方式 (讀Excel檔或從資料庫取資料)
    split_schedule_file_available = False
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, split_schedule_filename)):
            split_schedule = RWcsv.readCSV(split_schedule_filename, data_folder)
            split_schedule_file_available = True
    elif config['RUN_WAY'][0] == 'ORACLE':
        split_schedule = orcl_connect.getSplitSchedule( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(split_schedule)!=0:
            split_schedule_file_available = True
    print("load split schedule success.")

    # 讀取資料的方式 (讀Excel檔或從資料庫取資料)
    combine_rule_file_available = False
    if config['RUN_WAY'][0] == 'EXCEL':
        if os.path.isfile(os.path.join(data_folder, comebine_rule_name)):
            combine_rule = RWcsv.readCSV(comebine_rule_name, data_folder)
            combine_rule_file_available = True
    elif config['RUN_WAY'][0] == 'ORACLE':
        combine_rule = orcl_connect.getCombineRule( orcl_connect.getOrclConnectCur(), input_parameter )
        if len(combine_rule)!=0:
            combine_rule_file_available = True
    print("load combine rule success.")

    log2 = {
        'split schedule':[],
        'exe time':[]
    }
    start_time = time.time()

    all_schedule = []   # 用來儲存全部分配結果

    for demandform_row in demandform:
        log_list2 = []

        # 如果分配結果不是success或是shortage則不拆分schedule
        if demandform_row['msg']!=0 and demandform_row['msg']!=6:
            total_qty = demandform_row['Z_WAFER_QTY'] if (pd.isnull(demandform_row['Z_DIE_QTY'])) else demandform_row['Z_DIE_QTY']

            # 如果是沒有找到PID device setting等，不列出device setting資訊
            if demandform_row['msg']==1 or demandform_row['msg']==2 or demandform_row['msg']==3:
                result = [
                    demandform_row['Z_WORK_ORDER'],    # WP
                    "1011",                     # WERKS
                    demandform_row['KUNNR'],     # KUNNR
                    demandform_row['KONZS'],     # KONZS
                    demandform_row['Z_ITEM'],     # WP#iten
                    demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                    total_qty,          # demand
                    "",                              # die level
                    "",             # wafer device
                    "",              # batch id
                    "",        # wafer lot
                    "",                         # wafer id
                    "",             #bin
                    "",                # wafer qty
                    "",      # each level die qty
                    "",              # total level die qty
                    demandform_row['Z_DEMAND_GRP']
                ]
                all_schedule.append(result)

            #否則列出device setting資訊
            elif demandform_row['msg']==4 or demandform_row['msg']==5 or demandform_row['msg']==7 or demandform_row['msg']==8:
                for ratio_index in range(len(demandform_row['die device'])):
                    if len(demandform_row['die device'])==1:
                        die_level = 1
                    else:
                        die_level = (demandform_row['die device'][(ratio_index)]['DIE_SEQ'])

                    result = [
                        demandform_row['Z_WORK_ORDER'],    # WP
                        "1011",                     # WERKS
                        demandform_row['KUNNR'],     # KUNNR
                        demandform_row['KONZS'],     # KONZS
                        demandform_row['Z_ITEM'],     # WP#iten
                        demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                        total_qty,                                  # demand
                        die_level*10000,                         # die level
                        demandform_row['device setting'][(die_level)][0]['Z_WAFER_DEV_TYPE'],             # wafer device
                        "",              # batch id
                        "",        # wafer lot
                        "",                         # wafer id
                        "",             #bin
                        "",                # wafer qty
                        "",      # each level die qty
                        "",              # total level die qty
                        demandform_row['Z_DEMAND_GRP']
                    ]
                    all_schedule.append(result)
            continue

        # 尋找split rule並抓取該rule的index
        device_split_rule = split_rule[split_rule['Z_ASSY_DEV_TYPE']==demandform_row['Z_ASSY_DEV_TYPE']]
        index = split_rule[split_rule['Z_ASSY_DEV_TYPE']==demandform_row['Z_ASSY_DEV_TYPE']].index
        split_type = ""
        by_PCEA = ""
        std_size = 0
        min_size = 0
        combine_num = 1

        if len(index)!=0:
            instr_time = time.localtime()
            log_str = "split rule:{}".format(index[0]+1)
            info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
            log_list2.append(info_list)

        # 讀取demand
        if (pd.isnull(demandform_row['Z_DIE_QTY'])):
            total_qty = demandform_row['Z_WAFER_QTY']
        else:
            total_qty = demandform_row['Z_DIE_QTY']

        # 設定split rule
        # 如果沒有找到，針對包含%的規則再找一次
        if len(device_split_rule)==0:
            index = []  
            for temp_index, rule_row in split_rule.iterrows():
                if demandform_row['Z_ASSY_DEV_TYPE'].find(rule_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                    device_split_rule = rule_row
                    index.append(temp_index)
                    break

            #如果沒有找到，則給定default規則
            if len(device_split_rule)==0:
                split_type = "last"
                std_size = 10000000
                min_size = 0
                by_PCEA = "EA"

            # 如果有找到對應規則，抓取相關參數
            else:
                if device_split_rule['Z_AVG_FLAG']=="V":
                    split_type = "avg"
                    std_size = int(device_split_rule['Z_STD_QTY'])
                elif device_split_rule['Z_MRG_LASTLOT']=="V":
                    split_type = "last"
                    std_size = int(device_split_rule['Z_STD_QTY'])
                    min_size = int(device_split_rule['Z_MIN_QTY'])
                else:
                    split_type = "none"
                    std_size = int(device_split_rule['Z_STD_QTY'])
                by_PCEA = device_split_rule['Z_D_MEINS']

        # 如果有找到對應規則，抓取相關參數
        else:
            if device_split_rule.iloc[0,8] == "V":
                split_type = "avg"
                std_size = int(device_split_rule.iloc[0,5])
            elif device_split_rule.iloc[0,9] =="V":
                split_type = "last"
                std_size = int(device_split_rule.iloc[0,5])
                min_size = int(device_split_rule.iloc[0,6])
            else:
                split_type = "none"
                std_size = int(device_split_rule.iloc[0,5])
            by_PCEA = device_split_rule.iloc[0,4]

        # 搜尋是否有split schedule
        split_schedule_flag = False
        if split_schedule_file_available:
            if len(split_schedule)!=0:
                # 針對split schedule中 device setting欄位搜尋
                wafer_device_group = []
                for die_device in demandform_row['die device']:
                    if len(demandform_row['die device'])==1:
                        device_index = 1
                    else:
                        device_index = die_device['DIE_SEQ']
                    # 走訪產品所需的全部device setting
                    for device_setting_row in demandform_row['device setting'][(device_index)]:
                        rule_list = []
                        # split schedulr只取出device setting有值的欄位
                        temp_schedule = split_schedule[split_schedule['Z_WAFER_DEV_TYPE'].notna()]
                        for index, schedule_row in temp_schedule.iterrows():
                            if device_setting_row['Z_WAFER_DEV_TYPE']==schedule_row['Z_WAFER_DEV_TYPE']:
                                schedule_row_dict = dict(schedule_row)
                                rule_list.append(schedule_row_dict)

                                instr_time = time.localtime()
                                log_str = "split schedule:{}".format(index+1)
                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                log_list2.append(info_list)

                        if len(rule_list)!=0:
                            wafer_device_group.append(rule_list)

                # 針對split schedule中 wafer lot 欄位搜尋
                lot_id_group = []
                for die_device in demandform_row['die device']:
                    if len(demandform_row['die device'])==1:
                        device_index = 1
                    else:
                        device_index = die_device['DIE_SEQ']

                    # 走訪產品所使用到的全部batch
                    for batch_row in demandform_row['demand batch'][str(device_index)]:
                        group_list = []

                        # split schedulr只取出 wafer lot id 有值的欄位
                        temp_schedule = split_schedule[split_schedule['Z_WAFER_LOT'].notna()]
                        for index, schedule_row in temp_schedule.iterrows():
                            if batch_row['lot id'].find(schedule_row['Z_WAFER_LOT'].replace("%",""))!=-1:
                                instr_time = time.localtime()
                                log_str = "split schedule:{}".format(index+1)
                                info_list = [log_str, time.strftime("%Y%m%d", instr_time), time.strftime("%H%M%S", instr_time)]
                                log_list2.append(info_list)
                                group_name = str(int(schedule_row['Z_GRP_NO'])) if type(schedule_row['Z_GRP_NO'])!=type("string") else str(schedule_row['Z_GRP_NO'])
                                id_list = str(schedule_row['Z_W_SERIAL_NO']).split(",")
                                for index in range(len(id_list)):
                                    id_list[index] = "0"+id_list[index] if len(id_list[index])==1 else id_list[index]
                                    
                                # 將找到的規則整理成固定格式
                                not_found = True
                                for group_row in group_list:
                                    if group_row['grp']==group_name:
                                        not_found = False
                                        pre_id = group_row['wafer id']
                                        pre_id += id_list
                                        group_row['wafer id'] = pre_id
                                        break
                                if not_found:
                                    infor = {
                                        "grp":group_name,     # group的名稱
                                        "wafer lot": schedule_row['Z_WAFER_LOT'],  # 有符合規則的batch
                                        "target": batch_row['lot id'],    # split schedule中的值
                                        "wafer id": id_list    #需被拆分的id
                                    }
                                    group_list.append(infor)
                        if len(group_list)!=0:
                            lot_id_group.append(group_list)
                if len(wafer_device_group) or len(lot_id_group)!=0:
                    split_schedule_flag = True

        # 搜尋是否有combine rule
        if combine_rule_file_available:
            if len(combine_rule)!=0:
                rule = combine_rule[combine_rule['Z_ASSY_DEV_TYPE']==demandform_row['Z_ASSY_DEV_TYPE']]
                rule = combine_rule[combine_rule['KUNNR']==demandform_row['KUNNR']]
                if len(rule)!=0:
                    for index, combine_row in rule.iterrows():
                        if combine_row['Z_ASSY_DEV_TYPE']=="A%":
                            combine_num = combine_row['Z_NUMOF_LOT']
                            break
                        elif demandform_row['Z_ASSY_DEV_TYPE'].find(combine_row['Z_ASSY_DEV_TYPE'].strip('%'))!=-1:
                            combine_num = combine_row['Z_NUMOF_LOT']
                            break
        
        # 分配產品為single的部分
        if len(demandform_row['die device'])==1:
            #continue
            print('{} single {} {}'.format(split_type,by_PCEA,demandform_row["Z_ASSY_DEV_TYPE"]))
            # 如果有split schedule，則拆分batch
            if split_schedule_flag:
                batch_sequence = []   # 用來站存分配的batch資訊
                wafer_sequence = []   # 用來站存分配的wafer資訊
                # 檢查針對device setting的規則是否為空，若有則拆分
                if len(wafer_device_group)!=0:
                    for wafer_device_group_row in wafer_device_group:
                        # 將規則中的bin拆成list
                        bin_list = []
                        if not pd.isnull(wafer_device_group_row[0]['Z_BIN']):
                            bin_list = str(wafer_device_group_row[0]['Z_BIN']).split("+")
                        # 將規則中的id拆成list
                        id_list = []
                        if not pd.isnull(wafer_device_group_row[0]['Z_W_SERIAL_NO']):
                            id_list = str(wafer_device_group_row[0]['Z_W_SERIAL_NO']).split(",")
                            for index in range(len(id_list)):
                                id_list[index] = "0"+id_list[index] if len(id_list[index])==1 else id_list[index]

                        # 走訪batch的資訊
                        for batch_row_index in range(len(demandform_row['demand wafer']['1'])):
                            # 檢查device setting是否符合，是則對batch做拆分
                            if demandform_row['demand wafer']['1'][batch_row_index][0][0]['Z_DEV_TYP']==wafer_device_group_row[0]['Z_WAFER_DEV_TYPE']:
                                pre_batch_info = demandform_row['demand batch']['1'][batch_row_index].copy()    # 用來記錄不在規則中的batch資訊
                                split_batch_info = pre_batch_info.copy()                 # 用來記錄在規則中的資訊
                                split_batch_info['pc'] = 0
                                split_batch_info['count'] = 0

                                none_wafer_list = []    #記錄不在規則中的wafer資訊(同一片batch，不同wafer)
                                split_wafer_list = []   #記錄在規則中的wafer資訊(同一片batch，不同wafer)
                                pc_count = 0     # 統計規則中wafer的片數
                                die_count = 0   # 統計規則中die的片數
                                for wafer_row in demandform_row['demand wafer']['1'][batch_row_index]:
                                    none_bin = []
                                    split_bin = []
                                    for wafer_bin in wafer_row:
                                        # 如果規則長度不為0且這一遍wafer有符合規則，記錄到規則group中
                                        if (len(bin_list)!=0 and (wafer_bin['ZBIN'] in bin_list) ) or\
                                            (len(id_list)!=0 and (wafer_bin['ZWAFER_ID'] in id_list) ):
                                            split_bin.append(wafer_bin)
                                            die_count += wafer_bin['Z_REST_DIE']
                                        # 沒有則記錄到一般group中
                                        else:
                                            none_bin.append(wafer_bin)
                                    # 如果一般group中有剩下的wafer，記錄起來
                                    if len(none_bin)!=0:
                                        none_wafer_list.append(none_bin)
                                    # 如果一般group中沒有，代表這個wafer全部符合規則，要扣一片wafer
                                    else:
                                        pre_batch_info['pc'] -= 1
                                    # 如果規則group中有wafer，代表有wafer符合規則，要加一片wafer
                                    if len(split_bin)!=0:
                                        split_wafer_list.append(split_bin)
                                        pc_count += 1
                                
                                # 同個batch全部wafer都走過一遍後，整理資訊
                                pre_batch_info['count'] -= die_count     # 扣除符合規則的die量
                                split_batch_info['pc'] = pc_count        # 紀錄符合規則的片數
                                split_batch_info['count'] = die_count    # 紀錄符合規則的die數
                                
                                # 將拆分資訊紀錄起來，如果長度為0，代表沒有該group，則不要紀錄
                                if len(pre_batch_info)!=0:
                                    batch_sequence.append(pre_batch_info)
                                if die_count!=0:
                                    batch_sequence.append(split_batch_info)
                                if len(none_wafer_list)!=0:
                                    wafer_sequence.append(none_wafer_list)
                                if die_count!=0:
                                    wafer_sequence.append(split_wafer_list)
                            # 如果沒有符合規則，則保留原本的batch資訊
                            else:
                                batch_sequence.append(demandform_row['demand batch']['1'][batch_row_index].copy())
                                wafer_sequence.append(demandform_row['demand wafer']['1'][batch_row_index].copy())
                    # 將拆分完結果存回紀錄中
                    demandform_row['demand batch']['1'] = batch_sequence
                    demandform_row['demand wafer']['1'] = wafer_sequence

                batch_sequence = []
                wafer_sequence = []
                # 檢查針對wafer lot的規則是否為空，若有則拆分
                if len(lot_id_group)!=0:
                    # 走訪規則中不同的wafer lot限制
                    for lot_id_group_row in lot_id_group:
                        # 走訪 batch資訊
                        for batch_row_index in range(len(demandform_row['demand wafer']['1'])):
                            # 如果該片batch符合wafer lot 的規則，則進行拆分
                            if demandform_row['demand batch']['1'][batch_row_index]['lot id']==lot_id_group_row[0]['target']:
                                pre_batch_info = demandform_row['demand batch']['1'][batch_row_index].copy()
                                split_batch_info = []

                                none_wafer_list = []
                                split_wafer_list = []
                                die_count = []
                                pc_count = []
                                # 由於wafer lot要拆分的group不只一組，因此初始化成陣列
                                for num in range(len(lot_id_group_row)):
                                    split_batch_info.append(pre_batch_info.copy())
                                    split_batch_info[num]['pc'] = 0
                                    split_batch_info[num]['count'] = 0

                                    split_wafer_list.append([])
                                    die_count.append(0)
                                    pc_count.append(0)
                                # 走訪 wafer 資訊
                                for wafer_row in demandform_row['demand wafer']['1'][batch_row_index]:
                                    none_bin = []
                                    split_bin = []
                                    # 由於wafer lot要拆分的group不只一組，因此初始化成陣列
                                    for num in range(len(lot_id_group_row)):
                                        split_bin.append([])
                                    # 走訪 bin 資訊
                                    for wafer_bin in wafer_row:
                                        no_fit = True
                                        # 走訪拆分group的資訊
                                        for dif_group_index in range(len(lot_id_group_row)):
                                            # 如果有符合 group 中的 wafer id 限制，則儲存在規則group中
                                            if wafer_bin['ZWAFER_ID'] in lot_id_group_row[dif_group_index]['wafer id']:
                                                no_fit = False
                                                split_bin[dif_group_index].append(wafer_bin)
                                                die_count[dif_group_index] += wafer_bin['Z_REST_DIE']
                                        # 如果沒有符合規則則存在一般group中
                                        if no_fit:
                                            none_bin.append(wafer_bin)

                                    if len(none_bin)!=0:
                                        none_wafer_list.append(none_bin)
                                    else:
                                        pre_batch_info['pc'] -= 1
                                    
                                    for split_bin_row_index in range(len(split_bin)):
                                        if len(split_bin[split_bin_row_index])!=0:
                                            split_wafer_list[split_bin_row_index].append(split_bin[split_bin_row_index])
                                            pc_count[split_bin_row_index] += 1
                                    
                                for num in range(len(lot_id_group_row)):
                                    pre_batch_info['count'] -= die_count[num]
                                    split_batch_info[num]['pc'] = pc_count[num]
                                    split_batch_info[num]['count'] = die_count[num]

                                if len(pre_batch_info)!=0:
                                    batch_sequence.append(pre_batch_info)
                                for num in range(len(lot_id_group_row)):
                                    if die_count[num]!=0:
                                        batch_sequence.append(split_batch_info[num])
                                if len(none_wafer_list)!=0:
                                    wafer_sequence.append(none_wafer_list)
                                for num in range(len(lot_id_group_row)):
                                    if die_count[num]!=0:
                                        wafer_sequence.append(split_wafer_list[num])
                            else:
                                batch_sequence.append(demandform_row['demand batch']['1'][batch_row_index].copy())
                                wafer_sequence.append(demandform_row['demand wafer']['1'][batch_row_index].copy())
                    
                    demandform_row['demand batch']['1'] = batch_sequence
                    demandform_row['demand wafer']['1'] = wafer_sequence

            # 根據目前的batch狀態與split規則，提前算出拆分數列
            split_sequence = []
            # 如果規則是avg
            if split_type == "avg":
                for batch_row_index in range(len(demandform_row['demand batch']['1'])):
                    EAPC_str = "count" if by_PCEA=='EA' else "pc"     # EA要看資訊中的'count' PC要看資訊中的'pc'
                    batch_qty = demandform_row['demand batch']['1'][batch_row_index][EAPC_str]  # 讀取目前batch的總量(依據EAPC_str選擇要取的量)

                    # 計算最適合的 avg 數量
                    ratio = batch_qty // std_size if batch_qty % std_size==0 else batch_qty // std_size + 1
                    avg_schedule = batch_qty // ratio if batch_qty%ratio==0 else batch_qty // ratio + 1

                    # 將batch的總量依照avg分配完畢
                    while batch_qty!=0:
                        if batch_qty < avg_schedule:
                            avg_schedule = batch_qty
                        split_sequence.append(avg_schedule)
                        batch_qty -= avg_schedule
            # 如果規則是last
            elif split_type =="last":
                for batch_row_index in range(len(demandform_row['demand batch']['1'])):
                    EAPC_str = "count" if by_PCEA=='EA' else "pc"
                    batch_qty = demandform_row['demand batch']['1'][batch_row_index][EAPC_str]

                    while batch_qty!=0:
                        # 檢查目前的batch量扣掉std_size後會不會小於min_size
                        left_qty = batch_qty - std_size
                        # 如果沒有小於，則直接紀錄std_size
                        if left_qty>=min_size:
                            split_sequence.append(std_size)
                            batch_qty = left_qty
                        # 如果小於，則記錄目前的batch量(代表尾批會小於min，要合併)
                        else:
                            split_sequence.append(batch_qty)
                            batch_qty = 0
            # 如果規則是none
            else:
                for batch_row_index in range(len(demandform_row['demand batch']['1'])):
                    EAPC_str = "count" if by_PCEA=='EA' else "pc"
                    batch_qty = demandform_row['demand batch']['1'][batch_row_index][EAPC_str]

                    while batch_qty!=0:
                        left_qty = batch_qty - std_size
                        # 如果batch剩餘量不到std_size，則記錄剩餘量
                        if batch_qty - std_size<=0:
                            split_sequence.append(batch_qty)
                            batch_qty = 0
                        else:
                            split_sequence.append(std_size)
                            batch_qty = left_qty
            
            level_schedule = []      # 用來儲存每次拆分的schedule中，儲存的wafer資訊
            wafer_id = []
            die_count = 0          # 統計該次拆分schedule的總die量
            pc_count = 0           # 統計該次拆分schedule的總片數
            sequence_index = 0     # 拆分數列的index
            PC_split_flag = False  # 如果split rule是依照EA拆分時，flag會啟動
            EA_split_flag = False  # 如果split rule是依照PC拆分時，flag會啟動
            # 開始拆分schedule
            # 走訪device setting中的每個batch
            for demand_wafer_row in demandform_row['demand wafer']['1']:
                # 走訪batch中的每個wafer
                for wafer_row in demand_wafer_row:
                    # 走訪wafer中的每個bin
                    for bin_row in wafer_row:
                        # 根據規則直接調整門檻的參數，節省重複大量code的部分
                        if by_PCEA=='EA':
                            qty_type_count = die_count
                            ratio = bin_row['Z_REST_DIE']
                        else:
                            qty_type_count = pc_count
                            ratio = 1

                        # 判斷是否壘加量+當前wafer是否有超過拆分的數量
                        # 寫while迴圈是因為當前wafer的die量可能很多，足夠應付下一次的拆分數量
                        # 例如當前wafer有10000die，拆分量為1000, 1000共2000這樣
                        while qty_type_count + ratio>=split_sequence[sequence_index]:
                            if qty_type_count + ratio>=split_sequence[sequence_index]:
                                if by_PCEA=='EA':
                                    left_die = bin_row['Z_REST_DIE'] - (split_sequence[sequence_index] - die_count)
                                    die_count += bin_row['Z_REST_DIE'] - left_die
                                else:
                                    left_die = 0
                                    die_count += bin_row['Z_REST_DIE']

                                # 從level_schedule尋找是否有同batch同bin的其他wafer id，需要歸類在同一個種類中
                                not_found = True
                                for level_row_index in range(len(level_schedule)):
                                    # 判斷是否為同batch
                                    if level_schedule[level_row_index]['RTNO']==bin_row['RTNO']:
                                        # 判斷是否為同bin
                                        if level_schedule[level_row_index]['BIN']==bin_row['ZBIN']:
                                            # 如果有相符的則之後不用再新增種類
                                            not_found = False
                                            # 根據進場量和餘量對照該wafer為哪一個標籤，如果進場量!=餘量，另作處理
                                            if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE'] or left_die!=0:  #進場量!=餘量
                                                # 如果目前沒有滿片的且也沒有第一片非滿片的，則該片wafer為第一片非滿片
                                                if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                                    wafer_type = "not"
                                                # 若不符合則為第二片非滿片
                                                else:
                                                    wafer_type = "secnot"
                                            # 若相同則為滿片
                                            else:
                                                wafer_type = "full"

                                            id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])
                                            
                                            # 檢查該種類同標籤中是否有其他的wafer，有則合併資料
                                            if len(level_schedule[level_row_index][wafer_type])!=0:
                                                wafer_id = level_schedule[level_row_index][wafer_type][0][11].split(',')
                                                wafer_id.append(id)
                                                wafer_id.sort()
                                                level_schedule[level_row_index][wafer_type][0][11] = ','.join(wafer_id)
                                                level_schedule[level_row_index][wafer_type][0][13] = len(wafer_id)
                                                level_schedule[level_row_index][wafer_type][0][14] += bin_row['Z_REST_DIE'] - left_die
                                            # 如果沒有則新增資料
                                            else:
                                                result = [
                                                    demandform_row['Z_WORK_ORDER'],    # WP
                                                    "1011",                     # WERKS
                                                    demandform_row['KUNNR'],     # KUNNR
                                                    demandform_row['KONZS'],     # KONZS
                                                    demandform_row['Z_ITEM'],     # WP#iten
                                                    demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                                    total_qty,    # demand
                                                    1,                        # die level
                                                    bin_row['Z_DEV_TYP'],    # wafer device
                                                    bin_row['RTNO'],              # batch id
                                                    bin_row['LOTNO']  ,        # wafer lot
                                                    id,                         # wafer id
                                                    bin_row['ZBIN'],             #bin
                                                    1,                # wafer qty
                                                    bin_row['Z_REST_DIE']-left_die,      # each level die qty
                                                    die_count,              # total level die qty
                                                    demandform_row['Z_DEMAND_GRP']
                                                ]
                                                level_schedule[level_row_index][wafer_type].append(result)
                                # 如果前面沒咬找到相符種類(同batch同bin)，則新增種類
                                if not_found:
                                    text = {
                                        'full' : [],
                                        'not' : [],
                                        'secnot' : [],
                                        'BIN' : bin_row['ZBIN'],
                                        'RTNO' : bin_row['RTNO']
                                    }
                                    level_schedule.append(text)

                                    id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])

                                    result = [
                                        demandform_row['Z_WORK_ORDER'],    # WP
                                        "1011",                     # WERKS
                                        demandform_row['KUNNR'],     # KUNNR
                                        demandform_row['KONZS'],     # KONZS
                                        demandform_row['Z_ITEM'],     # WP#iten
                                        demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                        total_qty,    # demand
                                        1,                        # die level
                                        bin_row['Z_DEV_TYP'],    # wafer device
                                        bin_row['RTNO'],                          # batch id
                                        bin_row['LOTNO']  ,        # wafer lot
                                        id,                      # wafer id
                                        bin_row['ZBIN'],             #bin
                                        1,                # wafer qty
                                        bin_row['Z_REST_DIE']-left_die,      # each level die qty
                                        die_count,              # total level die qty
                                        demandform_row['Z_DEMAND_GRP']
                                    ]
                                    level_row_index = len(level_schedule)-1
                                    # 根據進場量與餘量，調整標籤
                                    if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                        if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                            level_schedule[level_row_index]['not'].append(result)
                                        else:
                                            level_schedule[level_row_index]['secnot'].append(result)
                                    else:
                                        level_schedule[level_row_index]['full'].append(result)
                                
                                # 如果split rule為EA，則將level_schedule整理輸出結果
                                if by_PCEA=='EA':
                                    die_seq_num = 0   # 用來記錄不同level的數字
                                    # 走訪level_schedule中所有種類(排序為 較先的batch->該batch中較先的bin->該batch中較後的bin->較後的batch->該batch中較先的bin->...)
                                    for level_row_index in range(len(level_schedule)):
                                        if len(level_schedule[level_row_index]['not'])!=0:
                                            # 在這邊調整該筆結果的dei level(因為是single，所以只會有10000)
                                            level_schedule[level_row_index]['not'][0][7] = 10000 + die_seq_num
                                            # 在這邊調整該次拆分數量統計到的總die量
                                            level_schedule[level_row_index]['not'][0][15] = die_count
                                            # 在single中，整理結果是直接儲存到所有結果的all_schedule中
                                            all_schedule.append(level_schedule[level_row_index]['not'][0])
                                            die_seq_num += 1
                                        if len(level_schedule[level_row_index]['full'])!=0:
                                            level_schedule[level_row_index]['full'][0][7] = 10000 + die_seq_num
                                            level_schedule[level_row_index]['full'][0][15] = die_count
                                            all_schedule.append(level_schedule[level_row_index]['full'][0])
                                            die_seq_num += 1
                                        if len(level_schedule[level_row_index]['secnot'])!=0:
                                            level_schedule[level_row_index]['secnot'][0][7] = 10000 + die_seq_num
                                            level_schedule[level_row_index]['secnot'][0][15] = die_count
                                            all_schedule.append(level_schedule[level_row_index]['secnot'][0])
                                            die_seq_num += 1

                                    bin_row['Z_REST_DIE'] = left_die   # 將這片剩下的die量存回去，讓下輪繼續分配
                                    die_count = 0            # 重置各數量
                                    pc_count = 0
                                    wafer_id = []
                                    level_schedule = []
                                    sequence_index += 1       # !!!!!由於當前拆分數量結束，要繼續拆分下一個!!!!!
                                    # 如果這一片wafer剩餘量為0，則設定EA_split_flag可以直接跳下一個bin
                                    if left_die==0:
                                        EA_split_flag = True
                                        break
                                # 如果split rule為PC，則要分配完同id的所有bin在整理輸出結果
                                else:
                                    PC_split_flag = True
                                    break
                            # 重新設定門檻數值，讓下一次while判斷
                            if by_PCEA=='EA':
                                qty_type_count = die_count
                                ratio = bin_row['Z_REST_DIE']
                            else:
                                qty_type_count = pc_count
                                ratio = 1
                        # 如果這兩個flag有啟動，代表有達到拆分數量了
                        # 如果EA_split_flag被設定，代表這個bin沒有餘量，直接繼續分配下一個bin
                        # 如果PC_split_flag被設定，代表要分配完這一個wafer，直接繼續分配下一個bin
                        if EA_split_flag or PC_split_flag:
                            EA_split_flag = False
                            continue

                        # 如果這片bin還有餘量，則將這片整理進level_schedule中
                        not_found = True
                        for level_row_index in range(len(level_schedule)):
                            if level_schedule[level_row_index]['RTNO']==bin_row['RTNO']:
                                if level_schedule[level_row_index]['BIN']==bin_row['ZBIN']:
                                    not_found = False
                                    if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                        if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                            wafer_type = "not"
                                        else:
                                            wafer_type = "secnot"
                                    else:
                                        wafer_type = "full"

                                    id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])
                                        
                                    if len(level_schedule[level_row_index][wafer_type])!=0:
                                        wafer_id = level_schedule[level_row_index][wafer_type][0][11].split(',')
                                        wafer_id.append(id)
                                        wafer_id.sort()
                                        level_schedule[level_row_index][wafer_type][0][11] = ','.join(wafer_id)
                                        level_schedule[level_row_index][wafer_type][0][13] = len(wafer_id)
                                        level_schedule[level_row_index][wafer_type][0][14] += bin_row['Z_REST_DIE']
                                    else:
                                        result = [
                                            demandform_row['Z_WORK_ORDER'],    # WP
                                            "1011",                     # WERKS
                                            demandform_row['KUNNR'],     # KUNNR
                                            demandform_row['KONZS'],     # KONZS
                                            demandform_row['Z_ITEM'],     # WP#iten
                                            demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                            total_qty,    # demand
                                            1,                        # die level
                                            bin_row['Z_DEV_TYP'],    # wafer device
                                            bin_row['RTNO'],              # batch id
                                            bin_row['LOTNO']  ,        # wafer lot
                                            id,                         # wafer id
                                            bin_row['ZBIN'],             #bin
                                            1,                # wafer qty
                                            bin_row['Z_REST_DIE'],      # each level die qty
                                            die_count,              # total level die qty
                                            demandform_row['Z_DEMAND_GRP']
                                        ]
                                        level_schedule[level_row_index][wafer_type].append(result)
                        if not_found:
                            text = {
                                'full' : [],
                                'not' : [],
                                'secnot' : [],
                                'BIN' : bin_row['ZBIN'],
                                'RTNO' : bin_row['RTNO']
                            }
                            level_schedule.append(text)

                            id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])

                            result = [
                                demandform_row['Z_WORK_ORDER'],    # WP
                                "1011",                     # WERKS
                                demandform_row['KUNNR'],     # KUNNR
                                demandform_row['KONZS'],     # KONZS
                                demandform_row['Z_ITEM'],     # WP#iten
                                demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                total_qty,    # demand
                                1,                        # die level
                                bin_row['Z_DEV_TYP'],    # wafer device
                                bin_row['RTNO'],                          # batch id
                                bin_row['LOTNO']  ,        # wafer lot
                                id,                      # wafer id
                                bin_row['ZBIN'],             #bin
                                1,                # wafer qty
                                bin_row['Z_REST_DIE'],      # each level die qty
                                die_count,              # total level die qty
                                demandform_row['Z_DEMAND_GRP']
                            ]
                            level_row_index = len(level_schedule)-1
                            if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                    level_schedule[level_row_index]['not'].append(result)
                                else:
                                    level_schedule[level_row_index]['secnot'].append(result)
                            else:
                                level_schedule[level_row_index]['full'].append(result)
                        # 到這邊為走完一個bin的流程，統計分配進level_schedule中的總die量
                        die_count += bin_row['Z_REST_DIE']

                    # 如果PC_split_flag被設定，代表分配完這一個wafer了，這邊要將level_schedule整理輸出結果
                    if PC_split_flag:
                        PC_split_flag = False
                        die_seq_num = 0
                        for level_row_index in range(len(level_schedule)):
                            if len(level_schedule[level_row_index]['not'])!=0:
                                level_schedule[level_row_index]['not'][0][7] = 10000 + die_seq_num
                                level_schedule[level_row_index]['not'][0][15] = die_count
                                all_schedule.append(level_schedule[level_row_index]['not'][0])
                                die_seq_num += 1
                            if len(level_schedule[level_row_index]['full'])!=0:
                                level_schedule[level_row_index]['full'][0][7] = 10000 + die_seq_num
                                level_schedule[level_row_index]['full'][0][15] = die_count
                                all_schedule.append(level_schedule[level_row_index]['full'][0])
                                die_seq_num += 1
                            if len(level_schedule[level_row_index]['secnot'])!=0:
                                level_schedule[level_row_index]['secnot'][0][7] = 10000 + die_seq_num
                                level_schedule[level_row_index]['secnot'][0][15] = die_count
                                all_schedule.append(level_schedule[level_row_index]['secnot'][0])
                                die_seq_num += 1

                        die_count = 0
                        pc_count = 0
                        wafer_id = []
                        level_schedule = []
                        sequence_index += 1
                        continue
                    # 到這邊為走完一片wafer的流程，統計分配進level_schedule中的總片量
                    pc_count += 1 
        # 分配產品為mcm的部分
        else:
            print('{} mcm {} {}'.format(split_type,by_PCEA,demandform_row["Z_ASSY_DEV_TYPE"]))

            die_device_index = 0       # 用來決定要取的PID資料(有些產品有重複使用的device setting，可以不用每個都抓)
            die_level_list = []        # 用來記錄該產品使用到的PID index(經過核對相同原料後，有重複使用的原料可以不用再跑一遍)
            split_sequence = []
            multi_use = False
            # 依據是否有使用相同的device setting調整要取的die sequence
            for ratio_index in range(len(demandform_row['device combine']['device_ratio'])):
                if ratio_index!=0:
                    die_device_index += demandform_row['device combine']['device_ratio'][ratio_index-1]
                if demandform_row['device combine']['device_ratio'][ratio_index]!=1:
                    multi_use = True
                if len(demandform_row['die device'])==1:
                    die_level = 1                     
                    die_level_list.append(die_level)
                else:
                    die_level = (demandform_row['die device'][(die_device_index)]['DIE_SEQ'])
                    die_level_list.append(die_level)
            
            # 如果沒有同一個產品裡使用相同原料
            if not multi_use:
                temp_demand = copy.deepcopy(demandform_row)   # 用來站存本次分配產品的資訊
                demand_num_list = []
                EAPC_str = "count" if by_PCEA=='EA' else "pc"

                # 走訪全部device setting的batch資訊，找出一個合適的拆分數列
                while len(temp_demand['demand batch'][str(die_level_list[0])])!=0:
                    demand_num = 1000000
                    # 找出使用的batch中最小的分配量
                    for die_level_index in range(len(die_level_list)):
                        if die_level_index==0:
                            if temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str] < demand_num:
                                demand_num = temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str]
                        else:
                            # 如果剩餘batch數小餘combine數，調整combine數
                            if len(temp_demand['demand batch'][str(die_level_list[die_level_index])]) < combine_num:
                                combine_num = len(temp_demand['demand batch'][str(die_level_list[die_level_index])])

                            combine_sum = 0
                            # 累加可以combine的batch分配量
                            for combine_index in range(combine_num):
                                combine_sum += temp_demand['demand batch'][str(die_level_list[die_level_index])][combine_index][EAPC_str]
                            if combine_sum < demand_num:
                                demand_num = combine_sum

                    # 將最小分配量從全部batch中扣除
                    for die_level_index in range(len(die_level_list)):
                        temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str] -= demand_num
                        if temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str]==0:
                            temp_demand['demand batch'][str(die_level_list[die_level_index])].pop(0)
                        elif temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str]<0:
                            while temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str]<0:
                                left_num = temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str] * (-1)
                                temp_demand['demand batch'][str(die_level_list[die_level_index])].pop(0)

                                temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str] -= left_num 
                            if temp_demand['demand batch'][str(die_level_list[die_level_index])][0][EAPC_str]==0:
                                temp_demand['demand batch'][str(die_level_list[die_level_index])].pop(0)

                    # 將分配量加入list中
                    demand_num_list.append(demand_num)
                
                # 依據split rule的種類來切分拆分後的數列
                if split_type == "avg":
                    for batch_qty in demand_num_list:
                        ratio = batch_qty // std_size if batch_qty % std_size==0 else batch_qty // std_size + 1
                        avg_schedule = batch_qty // ratio if batch_qty%ratio==0 else batch_qty // ratio + 1

                        while batch_qty!=0:
                            if batch_qty < avg_schedule:
                                avg_schedule = batch_qty
                            split_sequence.append(avg_schedule)
                            batch_qty -= avg_schedule
                elif split_type == "last":
                    for batch_qty in demand_num_list:
                        while batch_qty!=0:
                            # 檢查目前的batch量扣掉std_size後會不會小於min_size
                            left_qty = batch_qty - std_size
                            # 如果沒有小於，則直接紀錄std_size
                            if left_qty>=min_size:
                                split_sequence.append(std_size)
                                batch_qty = left_qty
                            # 如果小於，則記錄目前的batch量(代表尾批會小於min，要合併)
                            else:
                                split_sequence.append(batch_qty)
                                batch_qty = 0
                else:
                    for batch_qty in demand_num_list:
                        while batch_qty!=0:
                            left_qty = batch_qty - std_size
                            # 如果batch剩餘量不到std_size，則記錄剩餘量
                            if batch_qty - std_size<=0:
                                split_sequence.append(batch_qty)
                                batch_qty = 0
                            else:
                                split_sequence.append(std_size)
                                batch_qty = left_qty

                # 開始分配結果(基本上與single的一樣分法)
                all_result = []
                # 走訪所有需要被分配的device setting
                for die_level_index in range(len(die_level_list)):
                    level_schedule = []
                    result_list = []
                    wafer_id = []
                    die_count = 0
                    pc_count = 0
                    sequence_index = 0
                    PC_split_flag = False
                    EA_split_flag = False
                    # 針對單一device setting依照切分後的數列依序分配schedule
                    # die_level_list[die_level_index]代表的是當前要分配的device setting
                    for demand_wafer_row in demandform_row['demand wafer'][str(die_level_list[die_level_index])]:
                        for wafer_row in demand_wafer_row:
                            for bin_row in wafer_row:
                                if by_PCEA=='EA':
                                    qty_type_count = die_count
                                    ratio = bin_row['Z_REST_DIE']
                                else:
                                    qty_type_count = pc_count
                                    ratio = 1

                                while qty_type_count + ratio>=split_sequence[sequence_index]:
                                    if qty_type_count + ratio>=split_sequence[sequence_index]:
                                        if by_PCEA=='EA':
                                            left_die = bin_row['Z_REST_DIE'] - (split_sequence[sequence_index] - die_count)
                                            die_count += bin_row['Z_REST_DIE'] - left_die
                                        else:
                                            left_die = 0
                                            die_count += bin_row['Z_REST_DIE']

                                        not_found = True
                                        for level_row_index in range(len(level_schedule)):
                                            if level_schedule[level_row_index]['RTNO']==bin_row['RTNO']:
                                                if level_schedule[level_row_index]['BIN']==bin_row['ZBIN']:
                                                    not_found = False
                                                    if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE'] or left_die!=0:  #進場量!=餘量
                                                        if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                                            wafer_type = "not"
                                                        else:
                                                            wafer_type = "secnot"
                                                    else:
                                                        wafer_type = "full"

                                                    id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])
                                                    
                                                    if len(level_schedule[level_row_index][wafer_type])!=0:
                                                        wafer_id = level_schedule[level_row_index][wafer_type][0][11].split(',')
                                                        wafer_id.append(id)
                                                        wafer_id.sort()
                                                        level_schedule[level_row_index][wafer_type][0][11] = ','.join(wafer_id)
                                                        level_schedule[level_row_index][wafer_type][0][13] = len(wafer_id)
                                                        level_schedule[level_row_index][wafer_type][0][14] += bin_row['Z_REST_DIE'] - left_die
                                                    else:
                                                        result = [
                                                            demandform_row['Z_WORK_ORDER'],    # WP
                                                            "1011",                     # WERKS
                                                            demandform_row['KUNNR'],     # KUNNR
                                                            demandform_row['KONZS'],     # KONZS
                                                            demandform_row['Z_ITEM'],     # WP#iten
                                                            demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                                            total_qty,    # demand
                                                            1,                        # die level
                                                            bin_row['Z_DEV_TYP'],    # wafer device
                                                            bin_row['RTNO'],              # batch id
                                                            bin_row['LOTNO']  ,        # wafer lot
                                                            id,                         # wafer id
                                                            bin_row['ZBIN'],             #bin
                                                            1,                # wafer qty
                                                            bin_row['Z_REST_DIE']-left_die,      # each level die qty
                                                            die_count,              # total level die qty
                                                            demandform_row['Z_DEMAND_GRP']
                                                        ]
                                                        level_schedule[level_row_index][wafer_type].append(result)
                                        if not_found:
                                            text = {
                                                'full' : [],
                                                'not' : [],
                                                'secnot' : [],
                                                'BIN' : bin_row['ZBIN'],
                                                'RTNO' : bin_row['RTNO']
                                            }
                                            level_schedule.append(text)

                                            id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])

                                            result = [
                                                demandform_row['Z_WORK_ORDER'],    # WP
                                                "1011",                     # WERKS
                                                demandform_row['KUNNR'],     # KUNNR
                                                demandform_row['KONZS'],     # KONZS
                                                demandform_row['Z_ITEM'],     # WP#iten
                                                demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                                total_qty,    # demand
                                                1,                        # die level
                                                bin_row['Z_DEV_TYP'],    # wafer device
                                                bin_row['RTNO'],                          # batch id
                                                bin_row['LOTNO']  ,        # wafer lot
                                                id,                      # wafer id
                                                bin_row['ZBIN'],             #bin
                                                1,                # wafer qty
                                                bin_row['Z_REST_DIE']-left_die,      # each level die qty
                                                die_count,              # total level die qty
                                                demandform_row['Z_DEMAND_GRP']
                                            ]
                                            level_row_index = len(level_schedule)-1
                                            if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                                if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                                    level_schedule[level_row_index]['not'].append(result)
                                                else:
                                                    level_schedule[level_row_index]['secnot'].append(result)
                                            else:
                                                level_schedule[level_row_index]['full'].append(result)
                                        
                                        if by_PCEA=='EA':
                                            die_seq_num = 0
                                            # mcm整理結果時，是先將單一device setting的結果存到result_list中
                                            # 因為schedule需要10000,20000,30000,...這樣交叉紀錄
                                            # 但是現在存的方式是10000,10000,10000,...同一個device setting連續紀錄
                                            # 因此會到後面在做結果的整理
                                            for level_row_index in range(len(level_schedule)):
                                                if len(level_schedule[level_row_index]['not'])!=0:
                                                    level_schedule[level_row_index]['not'][0][7] = die_level_list[die_level_index]*10000 + die_seq_num
                                                    level_schedule[level_row_index]['not'][0][15] = die_count
                                                    result_list.append(level_schedule[level_row_index]['not'][0])
                                                    die_seq_num += 1
                                                if len(level_schedule[level_row_index]['full'])!=0:
                                                    level_schedule[level_row_index]['full'][0][7] = die_level_list[die_level_index]*10000 + die_seq_num
                                                    level_schedule[level_row_index]['full'][0][15] = die_count
                                                    result_list.append(level_schedule[level_row_index]['full'][0])
                                                    die_seq_num += 1
                                                if len(level_schedule[level_row_index]['secnot'])!=0:
                                                    level_schedule[level_row_index]['secnot'][0][7] = die_level_list[die_level_index]*10000 + die_seq_num
                                                    level_schedule[level_row_index]['secnot'][0][15] = die_count
                                                    result_list.append(level_schedule[level_row_index]['secnot'][0])
                                                    die_seq_num += 1

                                            bin_row['Z_REST_DIE'] = left_die
                                            die_count = 0
                                            pc_count = 0
                                            wafer_id = []
                                            level_schedule = []
                                            sequence_index += 1
                                            if left_die==0:
                                                EA_split_flag = True
                                                break
                                        else:
                                            PC_split_flag = True
                                            break
                                    
                                    if by_PCEA=='EA':
                                        qty_type_count = die_count
                                        ratio = bin_row['Z_REST_DIE']
                                    else:
                                        qty_type_count = pc_count
                                        ratio = 1

                                if EA_split_flag or PC_split_flag:
                                    EA_split_flag = False
                                    continue

                                not_found = True
                                for level_row_index in range(len(level_schedule)):
                                    if level_schedule[level_row_index]['RTNO']==bin_row['RTNO']:
                                        if level_schedule[level_row_index]['BIN']==bin_row['ZBIN']:
                                            not_found = False
                                            if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                                if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                                    wafer_type = "not"
                                                else:
                                                    wafer_type = "secnot"
                                            else:
                                                wafer_type = "full"

                                            id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])
                                                
                                            if len(level_schedule[level_row_index][wafer_type])!=0:
                                                wafer_id = level_schedule[level_row_index][wafer_type][0][11].split(',')
                                                wafer_id.append(id)
                                                wafer_id.sort()
                                                level_schedule[level_row_index][wafer_type][0][11] = ','.join(wafer_id)
                                                level_schedule[level_row_index][wafer_type][0][13] = len(wafer_id)
                                                level_schedule[level_row_index][wafer_type][0][14] += bin_row['Z_REST_DIE']
                                            else:
                                                result = [
                                                    demandform_row['Z_WORK_ORDER'],    # WP
                                                    "1011",                     # WERKS
                                                    demandform_row['KUNNR'],     # KUNNR
                                                    demandform_row['KONZS'],     # KONZS
                                                    demandform_row['Z_ITEM'],     # WP#iten
                                                    demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                                    total_qty,    # demand
                                                    1,                        # die level
                                                    bin_row['Z_DEV_TYP'],    # wafer device
                                                    bin_row['RTNO'],              # batch id
                                                    bin_row['LOTNO']  ,        # wafer lot
                                                    id,                         # wafer id
                                                    bin_row['ZBIN'],             #bin
                                                    1,                # wafer qty
                                                    bin_row['Z_REST_DIE'],      # each level die qty
                                                    die_count,              # total level die qty
                                                    demandform_row['Z_DEMAND_GRP']
                                                ]
                                                level_schedule[level_row_index][wafer_type].append(result)
                                if not_found:
                                    text = {
                                        'full' : [],
                                        'not' : [],
                                        'secnot' : [],
                                        'BIN' : bin_row['ZBIN'],
                                        'RTNO' : bin_row['RTNO']
                                    }
                                    level_schedule.append(text)

                                    id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])

                                    result = [
                                        demandform_row['Z_WORK_ORDER'],    # WP
                                        "1011",                     # WERKS
                                        demandform_row['KUNNR'],     # KUNNR
                                        demandform_row['KONZS'],     # KONZS
                                        demandform_row['Z_ITEM'],     # WP#iten
                                        demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                        total_qty,    # demand
                                        1,                        # die level
                                        bin_row['Z_DEV_TYP'],    # wafer device
                                        bin_row['RTNO'],                          # batch id
                                        bin_row['LOTNO']  ,        # wafer lot
                                        id,                      # wafer id
                                        bin_row['ZBIN'],             #bin
                                        1,                # wafer qty
                                        bin_row['Z_REST_DIE'],      # each level die qty
                                        die_count,              # total level die qty
                                        demandform_row['Z_DEMAND_GRP']
                                    ]
                                    level_row_index = len(level_schedule)-1
                                    if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                        if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                            level_schedule[level_row_index]['not'].append(result)
                                        else:
                                            level_schedule[level_row_index]['secnot'].append(result)
                                    else:
                                        level_schedule[level_row_index]['full'].append(result)
                                die_count += bin_row['Z_REST_DIE']

                            if PC_split_flag:
                                PC_split_flag = False
                                die_seq_num = 0
                                for level_row_index in range(len(level_schedule)):
                                    if len(level_schedule[level_row_index]['not'])!=0:
                                        level_schedule[level_row_index]['not'][0][7] = die_level_list[die_level_index]*10000 + die_seq_num
                                        level_schedule[level_row_index]['not'][0][15] = die_count
                                        result_list.append(level_schedule[level_row_index]['not'][0])
                                        die_seq_num += 1
                                    if len(level_schedule[level_row_index]['full'])!=0:
                                        level_schedule[level_row_index]['full'][0][7] = die_level_list[die_level_index]*10000 + die_seq_num
                                        level_schedule[level_row_index]['full'][0][15] = die_count
                                        result_list.append(level_schedule[level_row_index]['full'][0])
                                        die_seq_num += 1
                                    if len(level_schedule[level_row_index]['secnot'])!=0:
                                        level_schedule[level_row_index]['secnot'][0][7] = die_level_list[die_level_index]*10000 + die_seq_num
                                        level_schedule[level_row_index]['secnot'][0][15] = die_count
                                        result_list.append(level_schedule[level_row_index]['secnot'][0])
                                        die_seq_num += 1

                                die_count = 0
                                pc_count = 0
                                wafer_id = []
                                level_schedule = []
                                sequence_index += 1
                                continue
                            pc_count += 1 

                    # 將當前device setting的結果戰存到all_result中，稍後在整理到全部結果中
                    all_result.append(result_list)

                # 將結果依序整理到最終結果中
                while True:
                    # 走訪全部的mcm切分schedule(這個for迴圈代表的是不同device setting)
                    for result_index in range(len(all_result)):
                        first = True
                        # 這個迴圈代表的是同一個device setting中，不同筆的schedule
                        while True:
                            #將第一個die level為10000的加入最終結果
                            if first:
                                all_schedule.append(all_result[result_index][0])
                                all_result[result_index].pop(0)
                                first = False
                                continue
                            # 如果出現第二筆的10000則終止(代表到了下一個schedule)
                            if len(all_result[result_index])==0 or all_result[result_index][0][7]%((result_index+1)*10000)==0:
                                break
                            # 依序加入後續的10001, 10002,...
                            else:
                                all_schedule.append(all_result[result_index][0])
                                all_result[result_index].pop(0)

                    if len(all_result)==0:
                        break
                    if len(all_result[result_index])==0:
                        break
            # 如果有同一個產品裡使用相同原料，則需要另外做處理
            else:
                batch_sequence = copy.deepcopy(demandform_row['demand batch'])    # 用來站存分配的batch資訊
                wafer_sequence = copy.deepcopy(demandform_row['demand wafer'])   # 用來站存分配的wafer資訊

                EAPC_str = "count" if by_PCEA=='EA' else "pc"
                total = demandform_row['current qty']

                
                qty = 0                       # 用來記錄拆分數量是否已經達到demand
                all_result = []
                # 先依據現有的batch資料進行切分
                # 拆分的宗旨為：找出當前所有device setting中，能夠拆分並且消耗完batch的最小數量，重複流程值到分配完全部batch
                while qty<total:
                    # 用來儲存每個device setting的batch組合方式
                    # 例：若有三個batch，index為0 1 2
                    # conbine 1時：(0) (1) (2)
                    # conbine 2時：(0,1) (0,2) (1,2)
                    # conbine 3時：(0,1,2)
                    all_batch_arrange = []    
                    all_batch_qty = []    # 用來儲存每一個組合的總量
                    for die_level_index in range(len(die_level_list)):
                        batch_pty = []
                        # 紀錄每個batch的總量
                        for batch_row in batch_sequence[str(die_level_list[die_level_index])]:
                            batch_pty.append(batch_row[EAPC_str])

                        arrange = []
                        combine_index = ""
                        # 紀錄目前還有哪些batch可以用，如果被分配完batch餘量會是0，要排除掉
                        for batch_index in range(len(batch_sequence[str(die_level_list[die_level_index])])):
                            if batch_sequence[str(die_level_list[die_level_index])][batch_index][EAPC_str]!=0:
                                combine_index += str(batch_index)
                        # 根據main或sec調整combine量，並且進行排列組合
                        if die_level_index==0:
                            arrange = list(combinations(combine_index,1))
                        else:
                            arrange_num = len(combine_index) if len(combine_index)<combine_num else combine_num
                            arrange = list(combinations(combine_index,arrange_num))
                        # 記錄組合後的結果
                        all_batch_arrange.append(arrange)

                        arrange_qty = []
                        # 根據組合後的結果，統計對應的batch總量，並儲存起來
                        for arrange_row in arrange:
                            sum = 0
                            for arrange_index in arrange_row:
                                sum += batch_pty[int(arrange_index)]
                            arrange_qty.append(sum)
                        all_batch_qty.append(arrange_qty)
                    
                    minimal_list_qty = []       # 紀錄不同device setting中，能接受的最小拆分量
                    minimal_list_index = []     # 紀錄不同device setting中，能接受最小拆分量的index位置(該位置可以對應回組合方式)
                    for die_level_index in range(len(die_level_list)):
                        count = 0
                        # 統計當前device setting中，組合量不是0的數量
                        for num in all_batch_qty[die_level_index]:
                            if num != 0:
                                count += 1
                        
                        minimal = 0
                        device_ratio = demandform_row['device combine']['device_ratio'][die_level_index]     # 紀錄該device setting會重複被使用X次
                        # 如果剩餘組合量的數量不到device_ratio，則需要針對每個組合量判斷
                        if count < device_ratio:
                            batch_list_qty = []    # 用來站存當前device setting中每個組合量能接受的最小拆分量
                            for num in all_batch_qty[die_level_index]:
                                temp_minimal = 1000000
                                # 走訪每一個device_ratio
                                for each_ratio in range(1,device_ratio+1):
                                    # 如果可以整除，則保留最小的拆分量
                                    if num%each_ratio==0 and num/each_ratio<temp_minimal:
                                        temp_minimal = num/each_ratio
                                batch_list_qty.append(temp_minimal)
                            
                            # 從全部batch的拆分量中找出最小的
                            minimal = min(batch_list_qty)
                            num_index = batch_list_qty.index(minimal)
                        # 如果有，則直接從全部組合量中找出最小的即可(代表可以從每個組合量中扣除這個最小組合量)
                        else:
                            minimal = min(all_batch_qty[die_level_index])      #紀錄當前device setting最小可接受拆分量
                            num_index = all_batch_qty[die_level_index].index(minimal)   #紀錄當前device setting最小可接受拆分量的index位置(該位置對應的為組合方式)

                        minimal_list_index.append(num_index)      #紀錄全部device setting最小可接受拆分量的index位置
                        minimal_list_qty.append(minimal)          #紀錄全部device setting最小可接受拆分量

                    target_num = min(minimal_list_qty)            # 找出全部device setting中，最小可接受拆分量，做為這次的拆分數量
                    target_die_level = minimal_list_qty.index(target_num)   # 紀錄該拆分量對應的是哪一個device setting
                    target_index = minimal_list_index[target_die_level]     # 紀錄該拆分量對應的是哪一個device setting中的哪一個batch組合方式
                    target_flag = True
                    result_list = []
                    # 找到合適的拆分數量後，要從全部的batch中扣去數量
                    for die_level_index in range(len(die_level_list)):
                        device_ratio = demandform_row['device combine']['device_ratio'][die_level_index]  # 紀錄該device setting會重複被使用X次
                        die_level_num = die_level_list[die_level_index]      # 用來記錄計算die level用的

                        # 如果還沒有被分配完，持續分配
                        while device_ratio!=0:
                            # 如果目標拆分數量使用的device setting與當前要扣除的相同，則要調整要扣除的batch index
                            if target_die_level==die_level_index and target_flag:
                                target_flag = False
                                maximal = target_num
                                max_index = target_index
                                arrange_index_list = all_batch_arrange[die_level_index][target_index]
                            # 如果不是，則找出當前devic setting中餘量最大的組合量
                            else:
                                maximal = max(all_batch_qty[die_level_index])
                                max_index = all_batch_qty[die_level_index].index(maximal)
                                arrange_index_list = all_batch_arrange[die_level_index][ max_index ]
                            
                            # maximal為記錄當前組合量的餘量
                            # max_index為寄錄當前組合量對應的組合index
                            # arrange_index_list為記錄組合index對應回使用到batch的index
                            # 如果還沒有被分配完，持續分配 或 如果當前組合量的餘量不夠分配，則要跳下一個組合量
                            while maximal - target_num>=0 and device_ratio!=0:
                                left_qty = 0
                                # 走訪該組合量對應回的batch index
                                for batch_index in range(len(arrange_index_list)):
                                    # 如果是combine的batch且上一個batch有餘量但不夠分配拆分數量，則會有left_qty
                                    if left_qty!=0:
                                        # 從當前batch扣除還缺的量
                                        batch_sequence[str(die_level_list[die_level_index])][int(arrange_index_list[batch_index])][EAPC_str] -= left_qty
                                        # 從上一個batch中扣除所有量
                                        batch_sequence[str(die_level_list[die_level_index])][int(arrange_index_list[batch_index-1])][EAPC_str] = 0

                                        # 根據拆分數量，進行wafer的分配
                                        temp_result = returnSplitWafer(demandform_row, 
                                            by_PCEA, 
                                            total_qty,
                                            str(die_level_list[die_level_index]),    # 此為目標的device setting
                                            [ int(arrange_index_list[batch_index-1]), int(arrange_index_list[batch_index]) ],   # 此為目標device setting中使用的batch index
                                            target_num,            # 此為該次的拆分數量
                                            die_level_num)         # 此為該次的die levei
                                        result_list += temp_result  # 將回傳結果記錄起來

                                        # 從組合量扣除拆分數量
                                        maximal -= target_num
                                        all_batch_qty[die_level_index][max_index] -= target_num
                                        left_qty = 0
                                        # 扣除一次分配次數
                                        device_ratio -= 1
                                        # 增加die level的等級
                                        die_level_num += 1

                                    # batch的餘量是否可以應付拆分數量且還沒有被分配完，則從batch中扣除
                                    while batch_sequence[str(die_level_list[die_level_index])][int(arrange_index_list[batch_index])][EAPC_str]-target_num>=0 and device_ratio!=0:
                                        batch_sequence[str(die_level_list[die_level_index])][int(arrange_index_list[batch_index])][EAPC_str] -= target_num

                                        temp_result = returnSplitWafer(demandform_row, 
                                            by_PCEA, 
                                            total_qty,
                                            str(die_level_list[die_level_index]), 
                                            [ int(arrange_index_list[batch_index]) ],
                                            target_num,
                                            die_level_num)
                                        result_list += temp_result

                                        maximal -= target_num
                                        all_batch_qty[die_level_index][max_index] -= target_num
                                        device_ratio -= 1
                                        die_level_num += 1

                                    # 如果分配完了，則直接中斷
                                    if device_ratio==0:
                                        break
                                    # 如果該batch還有剩，但不夠拆分數量，紀錄該batch餘量
                                    left_qty = target_num - batch_sequence[str(die_level_list[die_level_index])][int(arrange_index_list[batch_index])][EAPC_str]
                    
                    # 紀錄該次拆分數量的結果
                    all_result += result_list
                    # 統計全部的拆分數量
                    qty += target_num
                
                # 將全部拆分數量的結果加到最終結果中
                all_schedule += all_result
        
        device_log = {
            demandform_row['Z_ASSY_DEV_TYPE']:log_list2,
            demandform_row['Z_ITEM']:""
        }
        log2['split schedule'].append(device_log)

    end_time = time.time()
    exe_time = round(end_time - start_time, 2)
    start_list = [time.strftime("%Y%m%d", time.localtime(start_time)),time.strftime("%H%M%S", time.localtime(start_time))]
    end_list = [time.strftime("%Y%m%d", time.localtime(end_time)),time.strftime("%H%M%S", time.localtime(end_time))]
    log2["exe time"].append([start_list, end_list, exe_time])

    all_log_2.append(log2)

    return demandform, all_schedule, all_log_2

def returnSplitWafer(demandform_row, by_PCEA, total_qty, die_level, arrange_index_list, target_num, die_level_num):
    level_schedule = []
    result_list = []
    wafer_id = []
    die_count = 0
    pc_count = 0
    PC_split_flag = False
    EA_split_flag = False
    # 針對單一device setting依照切分後的數列依序分配schedule
    # 根據傳入的batch index進行分配
    # 其餘部分與single的一樣
    for arrange_index in arrange_index_list:
        demand_wafer_row = demandform_row['demand wafer'][die_level][arrange_index]
        for wafer_row in demand_wafer_row:
            for bin_row in wafer_row:
                if bin_row['Z_REST_DIE']==0:
                    continue
                if by_PCEA=='EA':
                    qty_type_count = die_count
                    ratio = bin_row['Z_REST_DIE']
                else:
                    qty_type_count = pc_count
                    ratio = 1

                while qty_type_count + ratio>=target_num:
                    if qty_type_count + ratio>=target_num:
                        if by_PCEA=='EA':
                            left_die = bin_row['Z_REST_DIE'] - (target_num - die_count)
                            die_count += bin_row['Z_REST_DIE'] - left_die
                        else:
                            left_die = 0
                            die_count += bin_row['Z_REST_DIE']

                        not_found = True
                        for level_row_index in range(len(level_schedule)):
                            if level_schedule[level_row_index]['RTNO']==bin_row['RTNO']:
                                if level_schedule[level_row_index]['BIN']==bin_row['ZBIN']:
                                    not_found = False
                                    if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE'] or left_die!=0:  #進場量!=餘量
                                        if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                            wafer_type = "not"
                                        else:
                                            wafer_type = "secnot"
                                    else:
                                        wafer_type = "full"

                                    id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])
                                    
                                    if len(level_schedule[level_row_index][wafer_type])!=0:
                                        wafer_id = level_schedule[level_row_index][wafer_type][0][11].split(',')
                                        wafer_id.append(id)
                                        wafer_id.sort()
                                        level_schedule[level_row_index][wafer_type][0][11] = ','.join(wafer_id)
                                        level_schedule[level_row_index][wafer_type][0][13] = len(wafer_id)
                                        level_schedule[level_row_index][wafer_type][0][14] += bin_row['Z_REST_DIE'] - left_die
                                    else:
                                        result = [
                                            demandform_row['Z_WORK_ORDER'],    # WP
                                            "1011",                     # WERKS
                                            demandform_row['KUNNR'],     # KUNNR
                                            demandform_row['KONZS'],     # KONZS
                                            demandform_row['Z_ITEM'],     # WP#iten
                                            demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                            total_qty,    # demand
                                            1,                        # die level
                                            bin_row['Z_DEV_TYP'],    # wafer device
                                            bin_row['RTNO'],              # batch id
                                            bin_row['LOTNO']  ,        # wafer lot
                                            id,                         # wafer id
                                            bin_row['ZBIN'],             #bin
                                            1,                # wafer qty
                                            bin_row['Z_REST_DIE']-left_die,      # each level die qty
                                            die_count,              # total level die qty
                                            demandform_row['Z_DEMAND_GRP']
                                        ]
                                        level_schedule[level_row_index][wafer_type].append(result)
                        if not_found:
                            text = {
                                'full' : [],
                                'not' : [],
                                'secnot' : [],
                                'BIN' : bin_row['ZBIN'],
                                'RTNO' : bin_row['RTNO']
                            }
                            level_schedule.append(text)

                            id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])

                            result = [
                                demandform_row['Z_WORK_ORDER'],    # WP
                                "1011",                     # WERKS
                                demandform_row['KUNNR'],     # KUNNR
                                demandform_row['KONZS'],     # KONZS
                                demandform_row['Z_ITEM'],     # WP#iten
                                demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                total_qty,    # demand
                                1,                        # die level
                                bin_row['Z_DEV_TYP'],    # wafer device
                                bin_row['RTNO'],                          # batch id
                                bin_row['LOTNO']  ,        # wafer lot
                                id,                      # wafer id
                                bin_row['ZBIN'],             #bin
                                1,                # wafer qty
                                bin_row['Z_REST_DIE']-left_die,      # each level die qty
                                die_count,              # total level die qty
                                demandform_row['Z_DEMAND_GRP']
                            ]
                            level_row_index = len(level_schedule)-1
                            if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                    level_schedule[level_row_index]['not'].append(result)
                                else:
                                    level_schedule[level_row_index]['secnot'].append(result)
                            else:
                                level_schedule[level_row_index]['full'].append(result)
                        
                        bin_row['Z_REST_DIE'] = left_die
                        
                        if by_PCEA=='EA':
                            die_seq_num = 0
                            for level_row_index in range(len(level_schedule)):
                                if len(level_schedule[level_row_index]['not'])!=0:
                                    level_schedule[level_row_index]['not'][0][7] = die_level_num*10000 + die_seq_num
                                    level_schedule[level_row_index]['not'][0][15] = die_count
                                    result_list.append(level_schedule[level_row_index]['not'][0])
                                    die_seq_num += 1
                                if len(level_schedule[level_row_index]['full'])!=0:
                                    level_schedule[level_row_index]['full'][0][7] = die_level_num*10000 + die_seq_num
                                    level_schedule[level_row_index]['full'][0][15] = die_count
                                    result_list.append(level_schedule[level_row_index]['full'][0])
                                    die_seq_num += 1
                                if len(level_schedule[level_row_index]['secnot'])!=0:
                                    level_schedule[level_row_index]['secnot'][0][7] = die_level_num*10000 + die_seq_num
                                    level_schedule[level_row_index]['secnot'][0][15] = die_count
                                    result_list.append(level_schedule[level_row_index]['secnot'][0])
                                    die_seq_num += 1

                            bin_row['Z_REST_DIE'] = left_die
                            die_count = 0
                            pc_count = 0
                            wafer_id = []
                            level_schedule = []

                            # 如果拆分完畢的話則直接回傳結果，不然程式會繼續用剩下的wafer分配，這樣會出錯
                            return result_list
                        else:
                            PC_split_flag = True
                            break
                    
                    if by_PCEA=='EA':
                        qty_type_count = die_count
                        ratio = bin_row['Z_REST_DIE']
                    else:
                        qty_type_count = pc_count
                        ratio = 1

                if EA_split_flag or PC_split_flag:
                    EA_split_flag = False
                    continue

                not_found = True
                for level_row_index in range(len(level_schedule)):
                    if level_schedule[level_row_index]['RTNO']==bin_row['RTNO']:
                        if level_schedule[level_row_index]['BIN']==bin_row['ZBIN']:
                            not_found = False
                            if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                                if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                                    wafer_type = "not"
                                else:
                                    wafer_type = "secnot"
                            else:
                                wafer_type = "full"

                            id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])
                                
                            if len(level_schedule[level_row_index][wafer_type])!=0:
                                wafer_id = level_schedule[level_row_index][wafer_type][0][11].split(',')
                                wafer_id.append(id)
                                wafer_id.sort()
                                level_schedule[level_row_index][wafer_type][0][11] = ','.join(wafer_id)
                                level_schedule[level_row_index][wafer_type][0][13] = len(wafer_id)
                                level_schedule[level_row_index][wafer_type][0][14] += bin_row['Z_REST_DIE']
                            else:
                                result = [
                                    demandform_row['Z_WORK_ORDER'],    # WP
                                    "1011",                     # WERKS
                                    demandform_row['KUNNR'],     # KUNNR
                                    demandform_row['KONZS'],     # KONZS
                                    demandform_row['Z_ITEM'],     # WP#iten
                                    demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                                    total_qty,    # demand
                                    1,                        # die level
                                    bin_row['Z_DEV_TYP'],    # wafer device
                                    bin_row['RTNO'],              # batch id
                                    bin_row['LOTNO']  ,        # wafer lot
                                    id,                         # wafer id
                                    bin_row['ZBIN'],             #bin
                                    1,                # wafer qty
                                    bin_row['Z_REST_DIE'],      # each level die qty
                                    die_count,              # total level die qty
                                    demandform_row['Z_DEMAND_GRP']
                                ]
                                level_schedule[level_row_index][wafer_type].append(result)
                if not_found:
                    text = {
                        'full' : [],
                        'not' : [],
                        'secnot' : [],
                        'BIN' : bin_row['ZBIN'],
                        'RTNO' : bin_row['RTNO']
                    }
                    level_schedule.append(text)

                    id = "0" + str(bin_row['ZWAFER_ID']) if len(str(bin_row['ZWAFER_ID']))==1 else str(bin_row['ZWAFER_ID'])

                    result = [
                        demandform_row['Z_WORK_ORDER'],    # WP
                        "1011",                     # WERKS
                        demandform_row['KUNNR'],     # KUNNR
                        demandform_row['KONZS'],     # KONZS
                        demandform_row['Z_ITEM'],     # WP#iten
                        demandform_row['Z_ASSY_DEV_TYPE'],   # assy device
                        total_qty,    # demand
                        1,                        # die level
                        bin_row['Z_DEV_TYP'],    # wafer device
                        bin_row['RTNO'],                          # batch id
                        bin_row['LOTNO']  ,        # wafer lot
                        id,                      # wafer id
                        bin_row['ZBIN'],             #bin
                        1,                # wafer qty
                        bin_row['Z_REST_DIE'],      # each level die qty
                        die_count,              # total level die qty
                        demandform_row['Z_DEMAND_GRP']
                    ]
                    level_row_index = len(level_schedule)-1
                    if bin_row['RECV_DIE1']!=bin_row['Z_REST_DIE']:  #進場量!=餘量
                        if len(level_schedule[level_row_index]['full'])==0 and len(level_schedule[level_row_index]['not'])==0:
                            level_schedule[level_row_index]['not'].append(result)
                        else:
                            level_schedule[level_row_index]['secnot'].append(result)
                    else:
                        level_schedule[level_row_index]['full'].append(result)
                die_count += bin_row['Z_REST_DIE']
                bin_row['Z_REST_DIE'] = 0

            if PC_split_flag:
                PC_split_flag = False
                die_seq_num = 0
                for level_row_index in range(len(level_schedule)):
                    if len(level_schedule[level_row_index]['not'])!=0:
                        level_schedule[level_row_index]['not'][0][7] = die_level_num*10000 + die_seq_num
                        level_schedule[level_row_index]['not'][0][15] = die_count
                        result_list.append(level_schedule[level_row_index]['not'][0])
                        die_seq_num += 1
                    if len(level_schedule[level_row_index]['full'])!=0:
                        level_schedule[level_row_index]['full'][0][7] = die_level_num*10000 + die_seq_num
                        level_schedule[level_row_index]['full'][0][15] = die_count
                        result_list.append(level_schedule[level_row_index]['full'][0])
                        die_seq_num += 1
                    if len(level_schedule[level_row_index]['secnot'])!=0:
                        level_schedule[level_row_index]['secnot'][0][7] = die_level_num*10000 + die_seq_num
                        level_schedule[level_row_index]['secnot'][0][15] = die_count
                        result_list.append(level_schedule[level_row_index]['secnot'][0])
                        die_seq_num += 1

                die_count = 0
                pc_count = 0
                wafer_id = []
                level_schedule = []

                # 如果拆分完畢的話則直接回傳結果，不然程式會繼續用剩下的wafer分配，這樣會出錯
                return result_list
            pc_count += 1 

    return result_list
