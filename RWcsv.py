 #-*- coding=utf-8 -*-
import os
import sys
import pandas as pd
import csv
import logging

# read file from csv data
def readCSV(fileName, path="./", encoding="Big5"):
    if fileName.endswith('.csv'):
        buf = os.path.join(path, fileName)
        try:
            logging.info("[readCSV] Reading file {}".format(buf))
            df = pd.read_csv(buf,encoding=encoding)
            # remove special symbol
            #df.replace({",":""}, inplace=True, regex=True)
            # convert string data to number, ignore all convert error
            '''
            for colName in df.columns:
                if colName=="ZWAFER_ID":
                    continue
                df[colName] = pd.to_numeric(df[colName], downcast='integer', errors='ignore')
            '''
            return df
        except:
            logging.error('[readCSV] File={} {}'.format(buf, sys.exc_info()[0]))
    else:
        logging.error('[readCSV] {}. File format error.'.format(fileName))

# write result to csv file
def writeCSV(dataframe, filename, path="./", encoding='Big5'):
    try:
        dataframe.to_csv(path + filename, index=False, encoding=encoding)
    except:
        dataframe.to_csv(path + "new" + filename, index=False, encoding=encoding)    
