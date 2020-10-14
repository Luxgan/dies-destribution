 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json

# set eng flag (but current not use)
def setNotEng(inventory):    
    cannot_use_tag = "X"
    inventory_not_eng = inventory[inventory['Z_ENGINNER_FLAG'] != cannot_use_tag]
    inventory_not_eng = inventory_not_eng.reset_index(drop=True)
    print('set eng success')
    return inventory_not_eng
