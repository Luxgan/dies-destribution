import cx_Oracle
import pandas as pd

def getOrclConnectCur():
    # connect to oracle data base
    # DB account: sys
    # paseword: ASEftp_9031
    conn = cx_Oracle.connect("sys", "ASEftp_9031", "localhost:1521/orcl", mode=cx_Oracle.SYSDBA)
    #conn = cx_Oracle.connect("ppot/smooth@DC3")

    # get the cursor of the target db
    cur = conn.cursor()
    return cur

def getOrclConnectDB():
    # connect to oracle data base
    # DB account: sys
    # paseword: ASEftp_9031
    conn = cx_Oracle.connect("sys", "ASEftp_9031", "localhost:1521/orcl", mode=cx_Oracle.SYSDBA)
    #conn = cx_Oracle.connect("ppot/smooth@DC3")

    # get the cursor of the target db
    cur = conn.cursor()
    return conn, cur

def getAssignedLot(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_PLAN_NO",
        "Z_BP_NO",
        "Z_WAFER_LOT",
        "Z_CHARG"]

    # define the table name
    table_name = "GRP_PP_T_WP_ASSIGNED_LOT"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name )
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3] )
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()
    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getBuildPlan(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["Z_WORK_ORDER",
        "Z_ITEM",
        "KUNNR",        
        "KONZS",
        "Z_DEMAND_GRP",
        "Z_PRIORITY",
        "Z_OVER",
        "Z_WAFER_DEV_TYPE",
        "Z_BIN",
        "Z_ASSY_DEV_TYPE",
        "Z_WAFER_QTY",
        "Z_DIE_QTY",
        "Z_PROCESS_CODE",
        "Z_PLAN_NO",
        "Z_AUFNR",
        "Z_WEEK_CODE",
        "Z_PRODUCT_GRP",
        "Z_FG_DEVICE",
        "Z_PKG_CODE",
        "Z_SITE_CODE",
        "Z_KUNAG",
        "Z_PACKING",
        "Z_ISSUE_DATE"]

    # define the table name
    table_name = "GRP_PP_T_WP_BUILD_PLAN"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE Z_WORK_ORDER=\'{}\' and KUNNR=\'{}\'".format( ",".join(item), table_name, input_parameter[1], input_parameter[2])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getCombineRule(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KUNNR",
        "Z_ASSY_DEV_TYPE",
        "Z_MAIN_SEC",
        "Z_RULE",
        "Z_NUMOF_LOT",
        "Z_TABLE",
        "Z_COLUMN",
        "Z_SET_RULE",
        "Z_SET_FROM",
        "Z_SET_TO",
        "Z_MAP_VAL",
        "Z_REP_VAL"]

    # define the table name
    table_name = "GRP_PP_T_WP_COMBINE_RULE"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KUNNR=\'{}\'".format( ",".join(item), table_name, input_parameter[2])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()
    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getDeviceSetting(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_ASSY_DEV_TYPE",
        "Z_LOT_POSNR",        
        "Z_WAFER_DEV_TYPE",
        "Z_DIE_DEV_TYPE",
        "Z_MAIN_SEC",
        "Z_BIN",
        "Z_PRIORITY",
        "Z_BUMP_FLAG",
        "Z_DPW",
        "Z_Table",
        "Z_Column",
        "Z_Value"]

    # define the table name
    table_name = "GRP_PP_T_WP_DEVICE_SETTING"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getDieReleasingRule(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KUNNR",
        "Z_ASSY_DEV_TYPE",
        "Z_CONDITION",        
        "Z_RULE",
        "Z_VALUE"]

    # define the table name
    table_name = "GRP_PP_T_WP_DIE_RELEASING_RULE"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KUNNR=\'{}\'".format( ",".join(item), table_name, input_parameter[2])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getInventory(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["MATERIAL",
        "COMPANY ",
        "RTNO",
        "CUST_GRP",
        "KUNNR",
        "Z_DEV_TYP",
        "LOTNO",
        "Z_REST_DIE_MM",  
        "ZTARLOT",
        "Z_WAFER_INCH",
        "ZFROM",
        "ZBUMPING",
        "ZGRDATE",
        "ZGRTIME",
        "Z_ENGINNER_FLAG",
        "MATNR",
        "FLAG",
        "zase_wp_z_c_spcod1",
        "zase_wp_z_c_spcod2",
        "zase_wp_z_c_spcod3",
        "zase_wp_z_c_spcod4",
        "zase_wp_z_c_spcod5",
        "zase_wp_z_c_spcod6",
        "zase_wp_z_c_spcod7",
        "zase_wp_z_c_spcod8",
        "zase_wp_z_c_spcod9",
        "zase_wp_z_c_spcod10",
        "SLOC",
        "EXPIRE_DATE",
        "MTRL_TYPE",
        "GRIND_THINK",
        "RECV_WFR",
        "RECV_DIE",
        "Z_REST_DIE",
        "ZWAFER_ID",
        "ZBIN",
        "RECV_DIE1",  
        "Z_SCHEDULE",
        "Z_YEAR",
        "CHARG",
        "Z_W_SERIAL_NO",
        "zot50_KUNNR",  
        "zot50_z_c_spcod1",
        "zot50_z_c_spcod2",
        "zot50_z_c_spcod3",
        "zot50_z_c_spcod4",
        "zot50_z_c_spcod5",
        "zot50_z_c_spcod6",
        "zot50_z_c_spcod7",
        "zot50_z_c_spcod8",
        "zot50_z_c_spcod9",
        "zot50_z_c_spcod10",
        "zot50_z_c_spcod13",
        "zot50_z_c_spcod14",
        "zot50_z_c_spcod15",
        "zot50_z_c_spcod16",
        "zot50_z_c_spcod17"]

    # define the table name
    table_name = "GRP_PP_T_WP_INVENTORY"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE CUST_GRP=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getLotLimit(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_WAFER_DEV_TYPE",
        "Z_SPEC_NO ",
        "Z_GRP_NO"]

    # define the table name
    table_name = "GRP_PP_T_WP_LOT_LIMIT"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getLotSelection(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_WORK_ORDER",
        "Z_TYPE",        
        "Z_PRIORITY",
        "Z_WAFER_DEV_TYPE",
        "Z_CHARG",
        "Z_WAFER_LOT",
        "Z_W_SERIAL_NO",
        "Z_ASSY_DEV_TYPE",
        "Z_TABLE",
        "Z_COLUMN",
        "Z_VALUE",
        "Z_REMARK"]

    # define the table name
    table_name = "GRP_PP_T_WP_LOT_SELECTION"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getPID(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["WERKS",
        "KONZS",
        "Z_MFGID",
        "DIE_SEQ",        
        "Z_WAFER_INCH",
        "GRIND_THI_ASS_QTY",
        "GRIND_THI_MAX_QTY",
        "GRIND_THI_MIN_QTY",
        "DIE_SIZE_X_QTY",
        "DIE_SIZE_Y_QTY",
        "Z_DPW",
        "Z_DIE_DEV_TYPE",
        "RATIO",
        "DIE_LEVEL",
        "FUNCTION_DEVICE",
        "EC_MC_ID",
        "OPR_NUM",
        "CMP_ITM_ID",
        "CMP_ITM_NAME",
        "MFG_DEV_NAME",
        "Z_PKG_NAME",
        "Z_PKG_CODE",
        "Z_LEAD_COUNT",
        "ITM_UOM_TYPE",
        "Z_FLAG",
        "CUST_INST_NO",
        "CUST_INST_REV",
        "CUST_MARK_NO"]

    # define the table name
    table_name = "GRP_PP_T_WP_PID"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()
        
    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getSplitRule(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KUNNR",
        "Z_ASSY_DEV_TYPE",
        "Z_PKG_CODE",        
        "Z_PKG_NAME",
        "Z_D_MEINS",
        "Z_STD_QTY",
        "Z_MIN_QTY",
        "Z_MAX_QTY",
        "Z_AVG_FLAG",
        "Z_MRG_LASTLOT",
        "Z_LEAD_COUNT"]

    # define the table name
    table_name = "GRP_PP_T_WP_SPLIT_RULE"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KUNNR=\'{}\'".format( ",".join(item), table_name, input_parameter[2])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getSplitSchedule(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_GRP_NO",
        "Z_WAFER_LOT",        
        "Z_ASSY_DEV_TYPE",
        "Z_WAFER_DEV_TYPE",
        "Z_W_SERIAL_NO",
        "Z_BIN",
        "Z_REMARK"]

    # define the table name
    table_name = "GRP_PP_T_WP_SPLIT_SCH"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getSplitWaferID(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_SEQ",
        "Z_PRIORITY",        
        "Z_COLUMN",
        "Z_CONDITION",
        "Z_VALUE",
        "Z_RULE_TYPE",
        "Z_MAP_VALUE",
        "Z_POSITION",
        "Z_FROM",
        "Z_TO",
        "Z_TRIM_CHAR"]

    # define the table name
    table_name = "GRP_PP_T_WP_SPLIT_WAFERID"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()

    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def getWaferDeviceRef(cur, input_parameter):
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["KONZS",
        "Z_RULE",
        "Z_SLOC",        
        "Z_WAFER_DEV_TYPE",
        "Z_EXCLUDE_SLOC",
        "Z_MATNR",
        "Z_CONDITION_SEQ"]

    # define the table name
    table_name = "GRP_PP_T_WP_WAFER_DEVICE_REF"

    # execute the sql command to the target table name
    if len(input_parameter)==1:
        sql = "SELECT {} FROM {}".format( ",".join(item), table_name)
    else:
        sql = "SELECT {} FROM {} WHERE KONZS=\'{}\'".format( ",".join(item), table_name, input_parameter[3])
    cur.execute(sql)

    # get all result with the cursor
    rows = cur.fetchall()
    
    # change the different column name to original excel column name
    item[2] = "SLOC"
    
    # combine the result with the culomn to get a pd dataframe
    df = pd.DataFrame(rows, columns=item)

    return df

def setResult(result):
    db, cur = getOrclConnectDB()
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["Z_WORK_ORDER",
        "WERKS",
        "KUNNR",
        "KONZS",
        "Z_ITEM",     
        "Z_ASSY_DEV_TYPE",
        "PLAN_QTY",
        "Z_LOT_POSNR",        
        "Z_WAFER_DEV",
        "Z_CHARG",
        "Z_WAFER_LOT",
        "Z_W_SERIAL_NO",
        "Z_BIN",
        "Z_WAFER_QTY",
        "Z_DIE_QTY",
        "Z_DIE_TOTAL_QTY",
        "Z_DEMAND_GRP",
        "Z_REMARK",
        "SHORTAGE_QTY"]
    
    # define the table name
    table_name = "GRP_PP_T_WP_RESULT"
    
    for result_index, result_row in result.iterrows():
        re_str = ""
        for num in range(len(result_row)):
            re_str += "\'{}\'".format(result_row[num])
            if len(result_row)>=2 and num<=len(result_row)-2:
                re_str += ","

        sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, ",".join(item), re_str )
        
        cur.execute(sql)
    
    db.commit()

    return True

def setLog(log):
    db, cur = getOrclConnectDB()
    # define which culomn in table showld be taken out
    # the same with the column in excel file 
    item = ["WERKS",
        "KUNNR",
        "Z_WORK_ORDER",
        "Z_ITEM",
        "Z_STEP_NO",
        "Z_STEP",
        "Z_ASSY_DEV_TYPE",
        "Z_REMARK",
        "ERDAT",
        "ERTIME",
        "Z_EXE_TIME"]
    
    table_name = "GRP_PP_T_WP_LOG"
    
    for log_index, log_row in log.iterrows():
        re_str = ""
        for num in range(len(log_row)):
            re_str += "\'{}\'".format(log_row[num])
            if len(log_row)>=2 and num<=len(log_row)-2:
                re_str += ","

        sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, ",".join(item), re_str )
        
        cur.execute(sql)
    
    db.commit()

    return True

if __name__ == "__main__":

    # call the get(Table) function to show the test of preview result
    pd_temp = getPID(getOrclConnectCur())

    print(pd_temp)