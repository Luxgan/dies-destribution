 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json


# set the same group
def getSameGroup(demandform):
    no_gropu_index = []
    fit_gropu_index = []
    fit_group = []
    no_group = []

    # first check whether there is set group originally
    for index in range(len(demandform)):
        if not pd.isnull(demandform[index]['Z_DEMAND_GRP']) or demandform[index]['msg']!=0:
            fit_gropu_index.append(index)
        else:
            no_gropu_index.append(index)
    
    fit_group = [demandform[i] for i in fit_gropu_index]
    no_group = [demandform[i] for i in no_gropu_index]

    # check the different demand to search the same device
    group = 1
    defive_wafer = []
    for bp_index in range(len(no_group)):
        if not pd.isnull(no_group[bp_index]['Z_DEMAND_GRP']):
            continue
        defive_wafer = []
        no_group[bp_index]['Z_DEMAND_GRP'] = "a{}".format(group)
        # append the target device setting of assy device 
        if len(no_group[bp_index]['device setting'])==1:
            for level_index in range(len(no_group[bp_index]['device setting'])):
                for device in no_group[bp_index]['device setting'][(level_index+1)]:
                    if not device['Z_WAFER_DEV_TYPE'] in defive_wafer:
                        defive_wafer.append(device['Z_WAFER_DEV_TYPE'])
        else:
            for level_row in no_group[bp_index]['die device']:
                for device in no_group[bp_index]['device setting'][(level_row['DIE_SEQ'])]:
                    if not device['Z_WAFER_DEV_TYPE'] in defive_wafer:
                        defive_wafer.append(device['Z_WAFER_DEV_TYPE'])
        # check whether other assy device use the same device setting
        for next_index in range(bp_index+1,len(no_group)):
            if pd.isnull(no_group[next_index]['Z_DEMAND_GRP']):

                if len(no_group[next_index]['device setting'])==1:
                    same = False
                    for level_index in range(len(no_group[next_index]['device setting'])):
                        for device in no_group[next_index]['device setting'][(level_index+1)]:
                            if device['Z_WAFER_DEV_TYPE'] in defive_wafer:
                                same = True
                                break
                        if same:
                            break
                    if same:
                        no_group[next_index]['Z_DEMAND_GRP'] = "a{}".format(group)
                else:
                    same = False
                    for level_row in no_group[next_index]['die device']:
                        for device in no_group[next_index]['device setting'][(level_row['DIE_SEQ'])]:
                            if device['Z_WAFER_DEV_TYPE'] in defive_wafer:
                                same = True
                                break
                        if same:
                            break
                    if same:
                        no_group[next_index]['Z_DEMAND_GRP'] = "a{}".format(group)
        
        group += 1

    # concat all demand
    group = 1
    while len(no_group)!=0:
        index = 0
        while index<len(no_group):
            if no_group[index]['Z_DEMAND_GRP']=="a{}".format(group):
                temp = no_group.pop(index)
                fit_group.append(temp)
            else:
                index += 1
        group += 1
    
    return fit_group
