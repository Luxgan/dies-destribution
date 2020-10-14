 #-*- coding=utf-8 -*-
import os
import pandas as pd
import numpy as np
import csv
import json


# 背包排序法
def setKnapsack(n,w,cost,weight):
    result = []
    c = np.zeros(w+1)
    p = []
    for i in range(n):
        p.append([])
        for j in range(w,weight[i]-1,-1):
            if (c[j - weight[i]] + cost[i] > c[j]):
                c[j] = c[j - weight[i]] + cost[i]
                p[i].append(j)
    j = w
    for i in range(n-1,-1,-1):
        if j in p[i]:
            result.append(i)
            j = j - weight[i]

    return result
