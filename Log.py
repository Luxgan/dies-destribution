 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json

import RWcsv
import orcl_connect

config = pd.read_csv('./config.csv')

# 將json的log格式化成csv
def setLogFile(data_folder, result_folder, data_filename, result_log, customer, input_parameter):
    # 讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        original_bp = RWcsv.readCSV(data_filename, data_folder)
    elif config['RUN_WAY'][0] == 'ORACLE':
        original_bp = orcl_connect.getBuildPlan( orcl_connect.getOrclConnectCur(), input_parameter )

    WP = original_bp.loc[0,'Z_WORK_ORDER']
    WERKS = "1011"
    if len(input_parameter)!=1:
        instr_KUNNR = input_parameter[2]
    else:
        instr_KUNNR = original_bp.loc[0,'KUNNR']
    all_result = []
    temp = []

    # 0 program start
    temp = result_log[0]["program start"]
    all_result.append([WERKS,instr_KUNNR, WP, "", "0", "creation log","start", "", temp[0], temp[1]])

    # 1 demand 的部份
    temp = result_log[1]['demand sorted']
    instr_time = result_log[1]['exe time'][0]
    all_result.append([WERKS,instr_KUNNR, WP, "","1", "demand sorted","start", "", instr_time[0][0], instr_time[0][1]])

    for item in temp:
        
        header = "demand sorted"

        str = item[0].split(',')
        key = str[0]
        
        Z_WORK_ORDER = str[1]
        assy = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)]
        index = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)].index[0]
        KUNNR = assy.loc[index,'KUNNR']
        WP = assy.loc[index,'Z_WORK_ORDER']

        str.pop(0)
        str.pop(0)
        value = ""
        for seg in str:
            value += seg + " "    

        all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,1,header,key,value,item[1],item[2]])
    all_result.append([WERKS,instr_KUNNR, WP, "","1", "demand sorted","end", "", instr_time[1][0], instr_time[1][1], instr_time[2]])

    # 2 die device
    instr_time = result_log[2]['exe time'][0]
    all_result.append([WERKS,instr_KUNNR, WP, "","2", "get die device","start", "", instr_time[0][0], instr_time[0][1]])
    for item in result_log[2]['die device']:
        key = list(item.keys())[0]
        value = list(item.values())[0]

        Z_WORK_ORDER = list(item.keys())[1]
        assy = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)]
        index = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)].index[0]
        KUNNR = assy.loc[index,'KUNNR']
        WP = assy.loc[index,'Z_WORK_ORDER']

        exe_time = list(item.values())[2]
        if len(value)==0 or value[0]=="no pid":
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,2,"get die device","{}".format(key),"no pid",exe_time[0],exe_time[1]])
            continue

        exe_time = list(item.values())[2]
        #value.pop(0)
        for row in value:
            header = "get die device"
            str = "{}".format(key)
            str2 = "{}".format(row)
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,2,header,str,str2,exe_time[0],exe_time[1]])
    all_result.append([WERKS,instr_KUNNR, WP, "","2", "get die device","end", "", instr_time[1][0], instr_time[1][1], instr_time[2]])

    # 3 device setting
    instr_time = result_log[3]['exe time'][0]
    all_result.append([WERKS,instr_KUNNR, WP, "","3", "get device setting","start", "", instr_time[0][0], instr_time[0][1]])
    for item in result_log[3]['device setting']:
        key = list(item.keys())[0]
        value = list(item.values())[0]

        Z_WORK_ORDER = list(item.keys())[1]
        assy = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)]
        index = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)].index[0]
        KUNNR = assy.loc[index,'KUNNR']
        WP = assy.loc[index,'Z_WORK_ORDER']

        exe_time = list(item.values())[2]
        for row in value:
            header = "get device setting"
            str = "{}".format(key)
            str2 = "{}".format(row)
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,3,header,str,str2,exe_time[0],exe_time[1]])
        if len(value)==0:
            header = "get device setting"
            str = "{}".format(key)
            str2 = "{}".format("no device setting")
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,3,header,str,str2,exe_time[0],exe_time[1]])
    all_result.append([WERKS,instr_KUNNR, WP, "","3", "get device setting","end", "", instr_time[1][0], instr_time[1][1], instr_time[2]])

    # 4 die release rule
    instr_time = result_log[4]['exe time'][0]
    all_result.append([WERKS,instr_KUNNR, WP, "","4", "get die release rule","start", "", instr_time[0][0], instr_time[0][1]])
    for item in result_log[4]['die release rule']:
        key = list(item.keys())[0]
        value = list(item.values())[0]

        Z_WORK_ORDER = list(item.keys())[1]
        assy = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)]
        index = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)].index[0]
        KUNNR = assy.loc[index,'KUNNR']
        WP = assy.loc[index,'Z_WORK_ORDER']

        exe_time = list(item.values())[2]
        for row in value:
            header = "get die release rule"
            str = "{}".format(key)
            str2 = "{}".format(row)
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,4,header,str,str2,exe_time[0],exe_time[1]])
    all_result.append([WERKS,instr_KUNNR, WP, "","4", "get die release rule","end", "", instr_time[1][0], instr_time[1][1], instr_time[2]])

    # 5 distribution
    instr_time = result_log[5]['exe time'][0]
    all_result.append([WERKS,instr_KUNNR, WP, "","5", "distribution wafer","start", "", instr_time[0][0], instr_time[0][1]])
    for item in result_log[5]['distribution']:
        key = list(item.keys())[0]
        value = list(item.values())[0]

        Z_WORK_ORDER = list(item.keys())[1]
        assy = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)]
        index = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)].index[0]
        KUNNR = assy.loc[index,'KUNNR']
        WP = assy.loc[index,'Z_WORK_ORDER']

        for row in value:
            header = "distribution wafer"
            str = "{}".format(key)
            str2 = "{}".format(row[0])
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,5,header,str,str2,row[1],row[2]])
    all_result.append([WERKS,instr_KUNNR, WP, "","5", "distribution wafer","end", "", instr_time[1][0], instr_time[1][1], instr_time[2]])

    # 6 split schedule
    instr_time = result_log[6]['exe time'][0]
    all_result.append([WERKS,instr_KUNNR, WP, "","6", "split schedule","start", "", instr_time[0][0], instr_time[0][1]])
    for item in result_log[6]['split schedule']:
        key = list(item.keys())[0]
        value = list(item.values())[0]

        Z_WORK_ORDER = list(item.keys())[1]
        assy = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)]
        index = original_bp[original_bp['Z_ITEM']==int(Z_WORK_ORDER)].index[0]
        KUNNR = assy.loc[index,'KUNNR']
        WP = assy.loc[index,'Z_WORK_ORDER']

        for row in value:
            header = "split schedule"
            str = "{}".format(key)
            str2 = "{}".format(row[0])
            all_result.append([WERKS,KUNNR,WP,Z_WORK_ORDER,6,header,str,str2,row[1],row[2]])
    all_result.append([WERKS,instr_KUNNR, WP, "","6", "split schedule","end", "", instr_time[1][0], instr_time[1][1], instr_time[2]])


    # 7 program end
    temp = result_log[7]["program end"]
    all_result.append([WERKS,instr_KUNNR, WP, "", "7", "creation log","end", "", temp[0], temp[1], result_log[7]["exe time"]])

    resultColumnName = ["WERKS", "KUNNR", "WP#", "WP#item", "Z_STEP_NO","instruction","assy device","description","ERDAT","ERTIME","Z_EXE_TIME"]
    resultDF = pd.DataFrame(all_result, columns=resultColumnName)

    # 寫入資料的方式 (匯出Excel檔或寫入資料庫中)
    if config['RUN_WAY'][0] == 'EXCEL':
        RWcsv.writeCSV(resultDF, "log_{}_2.csv".format(customer), result_folder, "Big5")
    elif config['RUN_WAY'][0] == 'ORACLE':
        orcl_connect.setLog( resultDF ) 
        RWcsv.writeCSV(resultDF, "log_{}_2.csv".format(customer), result_folder, "Big5")
