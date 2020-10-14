 #-*- coding=utf-8 -*-
import os
import sys
import pandas as pd
import numpy as np
import csv
import json

import RWcsv
import orcl_connect

config = pd.read_csv('./config.csv')

# Ken 提供的切wafer id工具
def F_DETERMINE_YORN(I_COL_NAME, I_CONDITION, I_DATA_VALUE):    
    I_RESULT = ""
    I_CHAR = ""
    I_INT = 0
    I_LEN1 = 0
    I_LEN2 = 0
    I_SEC_CHAR = ""
    I_DATA_VALUE2 = ""

    I_RESULT = "N"
    if I_CONDITION=="!=":
        if I_DATA_VALUE.find("%")==(-1):
            if I_COL_NAME!=I_DATA_VALUE:
                I_RESULT = "Y"
        else:
            I_DATA_VALUE2 = I_DATA_VALUE
            I_INT = I_DATA_VALUE2.find("%")+1
            I_LEN1 = len(I_DATA_VALUE)
            I_SEC_CHAR = I_COL_NAME
            if I_INT == I_LEN1:
                I_CHAR = I_DATA_VALUE2[0:int(I_INT-1)]
                I_LEN2 = len(I_CHAR)
                I_SEC_CHAR = I_SEC_CHAR[0:int(I_LEN2)]
                if I_CHAR == I_SEC_CHAR:
                    I_RESULT = "N"
                else:
                    I_RESULT = "Y"
            else:
                while I_INT < I_LEN1:
                    if I_INT > 0:
                        I_CHAR = I_DATA_VALUE2[0:int(I_INT-1)]
                    else:
                        I_CHAR = I_DATA_VALUE2
                    
                    if I_SEC_CHAR.find(I_CHAR) >= 0 and\
                        I_SEC_CHAR[ int(I_SEC_CHAR.find(I_CHAR)+1-len(I_CHAR)-1) : int(I_SEC_CHAR.find(I_CHAR)+1-len(I_CHAR)-1+I_SEC_CHAR.find(I_CHAR)) ]>=0:
                        I_RESULT = "N"
                    else:
                        I_RESULT = "Y"
                    
                    if I_CHAR != "":
                        I_SEC_CHAR = I_SEC_CHAR[ int(I_SEC_CHAR.find(I_CHAR)+1+len(I_CHAR)-1) : int(I_SEC_CHAR.find(I_CHAR)+1+len(I_CHAR)-1+len(I_SEC_CHAR))]
                    I_DATA_VALUE2 = I_DATA_VALUE2[int(I_INT) : int(I_INT+len(I_DATA_VALUE2))]
                    if I_DATA_VALUE2 =="" or I_INT == 0:
                        break
                    else:
                        I_INT = I_DATA_VALUE2.find("%")+1
    elif I_CONDITION=="=":
        if I_DATA_VALUE.find("%") == (-1):
            if I_COL_NAME == I_DATA_VALUE:
                I_RESULT = "Y"
        else:
            if len(I_DATA_VALUE) == 1:
                I_RESULT = "Y"
            else:
                I_DATA_VALUE2 = I_DATA_VALUE
                I_INT = I_DATA_VALUE2.find("%")+1
                I_LEN1 = len(I_DATA_VALUE)
                I_SEC_CHAR = I_COL_NAME
                if I_INT == I_LEN1:
                    I_CHAR = I_DATA_VALUE2[0:int(I_INT-1)]
                    I_LEN2 = len(I_CHAR)
                    I_SEC_CHAR = I_SEC_CHAR[0:int(I_LEN2)]
                    if I_CHAR == I_SEC_CHAR:
                        I_RESULT = "N"
                    else:
                        I_RESULT = "Y"
                else:
                    while I_INT < I_LEN1:
                        if I_INT > 0:
                            I_CHAR = I_DATA_VALUE2[0:int(I_INT-1)]
                        else:
                            I_CHAR = I_DATA_VALUE2
                        
                        if I_SEC_CHAR.find(I_CHAR) >= 0 and\
                            I_SEC_CHAR[ int(I_SEC_CHAR.find(I_CHAR)+1-len(I_CHAR)-1) : int(I_SEC_CHAR.find(I_CHAR)+1-len(I_CHAR)-1+I_SEC_CHAR.find(I_CHAR)) ]>=0:
                            I_RESULT = "Y"
                        else:
                            I_RESULT = "N"
                        
                        if I_CHAR != "":
                            I_SEC_CHAR = I_SEC_CHAR[int(I_SEC_CHAR.find(I_CHAR)+1+len(I_CHAR)-1) : int(I_SEC_CHAR.find(I_CHAR)+1+len(I_CHAR)-1+len(I_SEC_CHAR))]
                        I_DATA_VALUE2 = I_DATA_VALUE2[int(I_INT) : int(I_INT+len(I_DATA_VALUE2))]
                        if I_DATA_VALUE2 =="" or I_INT == 0:
                            break
                        else:
                            I_INT = I_DATA_VALUE2.find("%")+1
    elif I_CONDITION=="{}":
        if I_COL_NAME.find(I_DATA_VALUE) >= 0:
            I_RESULT = "Y"
    elif I_CONDITION=="!{}":
        if I_COL_NAME.find(I_DATA_VALUE) >= 0:
            I_RESULT = "N"
        else:
            I_RESULT = "Y"
    return I_RESULT

# Ken 提供的切wafer id工具
def F_WKPLN_CUT_WAFERID(I_CUST3,I_WAFERID,I_WAFERLOT,I_BATCH,I_WAFERDEV, input_parameter):
    O_WAFERID = ""
    D_WAFERID = ""
    POSITION = ""
    POSITION1 = ""
    I_SUB_LOT = ""
    POSITION_DASH = ""   # 判斷"."位置
    POSITION_DEC = ""    # 判斷"-"位置

    D_WAFERID =  I_WAFERID
    # Wafer id = 01~25, 不進程式判斷
    if len(D_WAFERID) == 2 and\
        (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
        (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
        (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
        O_WAFERID = D_WAFERID
    else:
        # 有無設定 Split Wafer id
        D_WAFERID = F_WKPLN_SPILT_WID_SETUP(I_CUST3,I_WAFERID,I_BATCH,I_WAFERDEV,I_WAFERLOT, input_parameter)

        # 取出一碼補0
        if len(D_WAFERID) == 1:
            D_WAFERID = "0" + D_WAFERID
        # 解析結果是否為01~25
        if len(D_WAFERID) == 2 and\
            (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
            (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
            (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
            O_WAFERID = D_WAFERID
        else:
            if I_CUST3=="ITP":   # By 客戶 Hard-Code
                # 內容有.(小數點)
                POSITION_DEC = I_WAFERID.find(".")+1
                # 內容有-(DASH)
                POSITION_DASH = I_WAFERID.find("-")+1
                # 不包含"." and 不包含"-",取前2碼
                if POSITION_DEC == 0 and POSITION_DASH == 0:
                    D_WAFERID = I_WAFERID[0:2]
                else:
                    D_WAFERID = I_WAFERID
            elif I_CUST3=="LDG":
                POSITION_DEC = I_WAFERID.find(".")+1
                POSITION_DASH = I_WAFERID.find("-")+1
                if POSITION_DEC == 0 and POSITION_DASH == 0 and I_WAFERLOT[0:1]!="A" and I_WAFERLOT[0:2]!="EL":
                    D_WAFERID = I_WAFERID[len(I_WAFERID)-2:len(I_WAFERID)]
                else:
                    D_WAFERID = I_WAFERID
            elif I_CUST3=="FUJ":
                POSITION_DEC = I_WAFERID.find(".")+1
                POSITION_DASH = I_WAFERID.find("-")+1
                # 不包含"." and 不包含"-",從右取3.4碼
                if POSITION_DEC == 0 and POSITION_DASH == 0 and I_WAFERLOT[0:1]!="8":
                    D_WAFERID = I_WAFERID[len(I_WAFERID)-4:len(I_WAFERID)-2]
                else:
                    D_WAFERID = I_WAFERID
            else:
                D_WAFERID = I_WAFERID
        
        # 取出一碼補0
        if len(D_WAFERID) == 1:
            D_WAFERID = "0" + D_WAFERID

        if len(D_WAFERID) == 2 and\
            (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
            (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
            (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
            O_WAFERID = D_WAFERID
        else:
            # 內容有.(小數點)
            POSITION_DEC = I_WAFERID.find(".")+1
            if POSITION_DEC == 0:
                # Wafer id 回傳全碼
                D_WAFERID = I_WAFERID
            else:
                # 取.(小數點)後2碼
                D_WAFERID = I_WAFERID[POSITION_DEC:POSITION_DEC+2]
                if len(D_WAFERID) == 1:
                    D_WAFERID = "0" + D_WAFERID
                # 解析結果是否為01~25
                if len(D_WAFERID) == 2 and\
                    (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
                    (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
                    (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
                    O_WAFERID = D_WAFERID
                else:
                    # Wafer id 回傳全碼
                    D_WAFERID = I_WAFERID
            
            if len(D_WAFERID) == 2 and\
                (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
                (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
                (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
                O_WAFERID = D_WAFERID
            else:
                # 內容有-(DASH)
                POSITION_DASH = I_WAFERID.find("-")+1
                if POSITION_DEC == 0:
                    # Wafer id 回傳全碼
                    D_WAFERID = I_WAFERID
                else:
                    # 取-(dash)後2碼
                    D_WAFERID = I_WAFERID[POSITION_DASH:POSITION_DASH+2]
                    if len(D_WAFERID) == 1:
                        D_WAFERID = "0" + D_WAFERID
                    # 解析結果是否為01~25
                    if len(D_WAFERID) == 2 and\
                        (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
                        (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
                        (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
                        O_WAFERID = D_WAFERID
                    else:
                        # Wafer id 回傳全碼
                        D_WAFERID = I_WAFERID

                if len(D_WAFERID) == 2 and\
                    (ord(D_WAFERID[0])>=48 and ord(D_WAFERID[0])<=57) and\
                    (ord(D_WAFERID[1])>=48 and ord(D_WAFERID[1])<=57)  and\
                    (int(D_WAFERID)>=1 and int(D_WAFERID)<=25 ):
                    O_WAFERID = D_WAFERID
                else:
                    # Wafer id 回傳全碼
                    O_WAFERID = I_WAFERID
    return O_WAFERID

# Ken 提供的切wafer id工具        
def F_WKPLN_SPILT_WID_SETUP(I_KONZS ,I_WAFERID,I_BATCH,I_WAFERDEV,I_WAFER_LOT, input_parameter):
    I_PRIORITY = ""      # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_PRIORITY 格式;
    I_COL_NAME = ""      # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_COL_NAME 格式;
    I_CONDITION = ""     # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_CONDITION格式;
    I_DATA_VALUE = ""    # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_DATA_VALUE格式;
    I_RULE_TYPE = ""     # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_RULE_TYPE格式;
    I_MAP_VALUE = ""     # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_MAP_VALUE格式;
    I_POSITION = ""      # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_POSITION格式;
    I_FROM = ""          # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_FROM 格式;
    I_TO = ""            # 參照 GRP_PP_T_WKPLN_SPLIT_WID.Z_TO 格式;
    O_WID = ""           # Z_W_SERIAL_NO(WaferID的意思);
    I_RESULT = ""
    I_NUM1 = ""
    I_NUM2 = ""
    I_VAR_CHAR = ""
    I_INT = ""
    I_TRUE = ""

    FROMPOS = 1
    PARTLEN = 0
    SRCSTR = ""
    RES = ""

    DATA_FOLDER = "../csvData/" + "QCM0623" + "/"

    # 先抓取 Split WaferID Table (Excel:Split WaferId) 的設定
    split_wafer_id = pd.DataFrame()


    #讀取資料的方式 (讀Excel檔或從資料庫取資料)
    if config['RUN_WAY'][0] == 'EXCEL':
        split_wafer_id = RWcsv.readCSV("Split WaferId.csv", DATA_FOLDER)
    elif config['RUN_WAY'][0] == 'ORACLE':
        split_wafer_id = orcl_connect.getSplitWaferID( orcl_connect.getOrclConnectCur(), input_parameter )        

    c0 = split_wafer_id[split_wafer_id['KONZS']==I_KONZS].sort_values(by=['Z_PRIORITY'])

    if len(c0)!=0:
        for index, c0_row in c0.iterrows():
            I_PRIORITY = c0_row['Z_PRIORITY']
            I_COL_NAME = c0_row['Z_COLUMN']
            I_CONDITION = c0_row['Z_CONDITION']
            I_DATA_VALUE = c0_row['Z_VALUE']
            I_RULE_TYPE = c0_row['Z_RULE_TYPE']
            I_MAP_VALUE = c0_row['Z_MAP_VALUE']
            I_POSITION = c0_row['Z_POSITION']
            I_FROM = c0_row['Z_FROM']
            I_TO = c0_row['Z_TO']

            if I_COL_NAME=="CHARG":
                I_RESULT = F_DETERMINE_YORN(I_BATCH,I_CONDITION,I_DATA_VALUE)
            elif I_COL_NAME=="Z_WAFER_DEVICE":
                I_RESULT = F_DETERMINE_YORN(I_WAFERDEV,I_CONDITION,I_DATA_VALUE)
            elif I_COL_NAME=="Z_WAFER_ID":
                I_RESULT = F_DETERMINE_YORN(I_WAFERID,I_CONDITION,I_DATA_VALUE)
            elif I_COL_NAME=="Z_WAFER_LOT":
                I_RESULT = F_DETERMINE_YORN(I_WAFER_LOT,I_CONDITION,I_DATA_VALUE)
            
            if I_RESULT=="Y":
                I_VAR_CHAR =  I_WAFERID
                if I_RULE_TYPE=="SPC":
                    if I_WAFERID.find(I_MAP_VALUE)>=0 :
                        SRCSTR = I_WAFERID + I_MAP_VALUE

                        # 區塊判斷
                        if I_POSITION == 1:
                            I_TRUE = "Y"
                            # 拆解字串起始
                            FROMPOS = 1
                            # 擷取長度
                            PARTLEN = 0
                            for pos in range(int(I_POSITION)):
                                PARTLEN = SRCSTR.find(I_MAP_VALUE,1+PARTLEN)
                            PARTLEN = PARTLEN - 1
                        elif I_POSITION < 1:
                            I_TRUE = "Y"
                            FROMPOS = 1
                            PARTLEN = 0
                        else:
                            I_TRUE = "Y"
                            FROMPOS = 0
                            for pos in range(int(I_POSITION-1)):
                                FROMPOS = SRCSTR.find(I_MAP_VALUE,1+FROMPOS)
                            FROMPOS = FROMPOS + len(I_MAP_VALUE)
                            temp1 = 0
                            for pos in range(int(I_POSITION)):
                                temp1 = SRCSTR.find(I_MAP_VALUE,1+temp1)
                            temp2 = 0
                            for pos in range(int(I_POSITION-1)):
                                temp2 = SRCSTR.find(I_MAP_VALUE,1+temp2)
                            PARTLEN = temp1 - temp2 - len(I_MAP_VALUE)
                        # RES-區塊內全碼
                        RES = SRCSTR[FROMPOS:FROMPOS+PARTLEN]
                        if I_TRUE == "Y":
                            if not pd.isnull(I_FROM) and I_FROM>0 and not pd.isnull(I_TO) and I_TO>0:
                                I_VAR_CHAR = RES[int(I_FROM-1) : int(I_FROM-1+I_TO)]
                            elif not pd.isnull(I_FROM) and I_FROM<0 and not pd.isnull(I_TO) and I_TO>0:
                                # I_FROM -1 表示從右邊往左取
                                I_VAR_CHAR = RES[int(len(RES)+I_FROM-1) : int(len(RES)+I_FROM-1+I_TO) : -1]
                elif I_RULE_TYPE=="SPI":
                    I_NUM1 = 0
                    I_INT = 0
                    while I_INT < I_POSITION:
                        I_NUM1 = I_VAR_CHAR.find(I_MAP_VALUE)+1
                        I_NUM2 = len(I_VAR_CHAR)
                        I_INT = I_INT + 1
                        if I_VAR_CHAR.find(I_MAP_VALUE) >= 0:
                            if I_INT == I_POSITION:
                                if not pd.isnull(I_FROM) and I_FROM!=0:
                                    if I_FROM.find("-") >= 0:
                                        I_VAR_CHAR = I_VAR_CHAR[int(I_NUM1) : int(I_NUM1+I_TO)]
                                    else:
                                        I_VAR_CHAR = I_VAR_CHAR[int(I_NUM1-I_TO-1) : int(I_NUM1-1)]
                            else:
                                I_VAR_CHAR = I_VAR_CHAR[int(I_NUM1) : int(I_NUM2)]
                        if (I_VAR_CHAR.find(I_MAP_VALUE) == (-1) and  I_INT < I_POSITION) or I_VAR_CHAR is np.nan:
                            I_VAR_CHAR = I_WAFERID
                elif I_RULE_TYPE=="RSN":
                    if len(I_VAR_CHAR) >= I_TO:
                        I_INT = I_TO-I_FROM+1
                        I_NUM1 = len(I_VAR_CHAR)-I_FROM
                        I_VAR_CHAR = I_VAR_CHAR[int(I_NUM1-1) : int(I_NUM1-1+I_INT)]
                elif I_RULE_TYPE=="LSN":
                    if len(I_VAR_CHAR) >= I_TO:
                        I_INT = I_TO-I_FROM+1
                        I_VAR_CHAR = I_VAR_CHAR[int(I_FROM-1) : int(I_FROM-1+I_INT)]
                        
                if len(I_VAR_CHAR) == 1 and\
                    (ord(I_VAR_CHAR[0])>=48 and ord(I_VAR_CHAR[0])<=57):
                    I_VAR_CHAR = "0" + I_VAR_CHAR

            if len(I_VAR_CHAR) == 2 and\
                (ord(I_VAR_CHAR[0])>=48 and ord(I_VAR_CHAR[0])<=57) and\
                (ord(I_VAR_CHAR[1])>=48 and ord(I_VAR_CHAR[1])<=57)  and\
                (int(I_VAR_CHAR)>=1 and int(I_VAR_CHAR)<=25 ):
                break 

    O_WID = I_VAR_CHAR
    return O_WID

# 切分完wafer id後所需的自訂排序
def sortkey(element):
    return element[0]['ZWAFER_ID']

# 切分wafer id
def getSplitWaferID(temp_all_wafer, input_parameter, *indexs):
    re_merge_all = []

    if len(indexs)==0:
        # 資料格式為
        # [
        #   batch[
        #       wafer[
        #           bin[
        #           ],
        #           bin[
        #           ],...
        #       ],
        #       wafer[
        #       ],...
        #   ],
        #   batch[
        #   ],...
        # ]
        for batch_row in temp_all_wafer:
            re_merge_wafer = []
            for wafer_row in batch_row:
                merge_bin = []
                # 對wafer做id切分即可，不用對bin
                wafer_id = F_WKPLN_CUT_WAFERID(wafer_row[0]['CUST_GRP'],str(wafer_row[0]['ZWAFER_ID']),wafer_row[0]['LOTNO'],wafer_row[0]['RTNO'],wafer_row[0]['Z_DEV_TYP'], input_parameter)
                for bin_row in wafer_row:
                    bin_row['ZWAFER_ID'] = wafer_id
                    merge_bin.append(bin_row)

                re_merge_wafer.append(merge_bin)
            re_merge_wafer.sort(key=sortkey)
            re_merge_all.append(re_merge_wafer)
    else:
        # 資料格式為
        # [
        #   batch[
        #       wafer[
        #           bin[
        #           ],
        #           bin[
        #           ],...
        #       ],
        #       wafer[
        #       ],...
        #   ],
        #   batch[
        #   ],...
        # ]
        if indexs[0]!=-1:
            for batch_index in range(len(temp_all_wafer)):
                if batch_index not in indexs:
                    continue
                re_merge_wafer = []
                for wafer_row in temp_all_wafer[batch_index]:
                    merge_bin = []
                    wafer_id = F_WKPLN_CUT_WAFERID(wafer_row[0]['CUST_GRP'],str(wafer_row[0]['ZWAFER_ID']),wafer_row[0]['LOTNO'],wafer_row[0]['RTNO'],wafer_row[0]['Z_DEV_TYP'], input_parameter)
                    for bin_row in wafer_row:
                        bin_row['ZWAFER_ID'] = wafer_id
                        merge_bin.append(bin_row)

                    re_merge_wafer.append(merge_bin)
                    
                re_merge_wafer.sort(key=sortkey)
                re_merge_all = re_merge_wafer
        
        # 資料格式為
        # [
        #   dataframe
        # ]
        else:
            for batch_row in temp_all_wafer:
                for temp_index, wafer_row in batch_row.iterrows():
                    wafer_id = F_WKPLN_CUT_WAFERID(wafer_row['CUST_GRP'],str(wafer_row['ZWAFER_ID']),wafer_row['LOTNO'],wafer_row['RTNO'],wafer_row['Z_DEV_TYP'], input_parameter)
                    batch_row.loc[temp_index,'ZWAFER_ID'] = wafer_id

                re_merge_all = batch_row

    return re_merge_all
