 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json

import RWcsv
import orcl_connect

config = pd.read_csv('./config.csv')

# 設定最終分配的message
def setMessage(demandform, all_schedule, result_folder, customer):
    msg = [
        "success",                                 # msg = 0
        "WO creation fail: PID",                   # msg = 1
        "WO creation fail: device setting",        # msg = 2
        "WO creation fail: PID!=device setting",   # msg = 3
        "group shortage",                          # msg = 4
        "priority restriction",                    # msg = 5
        "shortage",                                # msg = 6
        "庫存餘量",                                 # msg = 7
        "MATNR在PID中沒有找到，詳看log確認RTNO"      # msg = 8
    ]

    # 走訪全部分配結果
    for index in range(len(all_schedule)):
        num = -1
        demandform_index = 0
        # 從demand form中找出對應的產品與其對應的msg
        for demandform_index in range(len(demandform)):
            if demandform[demandform_index]['Z_ITEM'] == all_schedule[index][4] and\
                demandform[demandform_index]['Z_ASSY_DEV_TYPE'] == all_schedule[index][5]:
                num = demandform[demandform_index]['msg']
                break
        
        # 如果mag是shortage，要計算shortage量
        if num==6:
            if pd.isnull(demandform[demandform_index]['Z_DIE_QTY']):
                shortage_num = demandform[demandform_index]['Z_WAFER_QTY'] - demandform[demandform_index]['current qty']
            else:
                shortage_num = demandform[demandform_index]['Z_DIE_QTY'] - demandform[demandform_index]['current qty']
            all_schedule[index].append("{}".format(msg[num]))
            all_schedule[index].append("{}".format(shortage_num))
        # 如果msg是庫存餘量，須尋找分配結果
        elif num==7:
            die_device_index = 0
            die_level_list = []
            for ratio_index in range(len(demandform[demandform_index]['device combine']['device_ratio'])):
                if ratio_index!=0:
                    die_device_index += demandform[demandform_index]['device combine']['device_ratio'][ratio_index-1]
                if len(demandform[demandform_index]['die device'])==1:
                    die_level = 1                     
                    die_level_list.append(die_level)
                else:
                    die_level = (demandform[demandform_index]['die device'][(die_device_index)]['DIE_SEQ'])
                    die_level_list.append(die_level)

            die_level = all_schedule[index][7] // 10000
            while die_level not in die_level_list:
                die_level -= 1

            if pd.isnull(demandform[demandform_index]['Z_DIE_QTY']):
                qty_type = "pc"
            else:
                qty_type = "count"
            left_num = 0
            if len(demandform[demandform_index]['demand batch'][str(die_level)])!=0:
                for batch_row in demandform[demandform_index]['demand batch'][str(die_level)]:
                    left_num += batch_row[qty_type]

            all_schedule[index].append("{} {}".format(msg[num], left_num))
        # 如果msg為其他，則直接輸出
        else:
            all_schedule[index].append(msg[num])
            all_schedule[index].append("")
        

    # 輸出最終結果
    resultColumnName = ["Z_WORK_ORDER", "WERKS", "KUNNR", "KONZS", "Z_ITEM", "assy device", "demand", "die level","wafer device", "batch id", "wafer lot", "wafer id", "BIN", "wafer qty","each level die qty", "total level die qty", "group", "msg","SHORTAGE_QTY"]
    resultDF = pd.DataFrame(all_schedule, columns=resultColumnName)

    # 寫入資料的方式 (匯出Excel檔或寫入資料庫中)
    if config['RUN_WAY'][0] == 'EXCEL':
        RWcsv.writeCSV(resultDF, "DeviceResult{}.csv".format(customer), result_folder, "Big5")
    elif config['RUN_WAY'][0] == 'ORACLE':
        orcl_connect.setResult(resultDF)
        RWcsv.writeCSV(resultDF, "DeviceResult{}.csv".format(customer), result_folder, "Big5")

    return all_schedule
