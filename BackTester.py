import quandl as qd
import pandas as pd
import logging
import time
import numpy as np
import os
import math
import datetime

##############################################################
DUMP_FOLDER_PATH = 'C:/BackTester/Currency Dump/'#"C:/BackTester/Dump/"
LOG_PATH = "C:/BackTester/Log/"
ANALYSIS_PATH = 'C:/BackTester/Currency Analysis/'#"C:/BackTester/Analysis/"
PROD_CODE = "CHRIS/CME_CL1"
PROD_NAME = "LightCrude"
CONFIG_PATH = 'C:/BackTester/Config/ProductList_Currency.csv'#"C:/BackTester/Config/ProductList.csv"
TICKER_LIST_PATH = 'C:/BackTester/Config/ProductList_Currency.csv'#"C:/BackTester/Config/ProductList.csv"
duration_tuple = (20,5,20,10,30,5,30,10,60,10,60,20,90,20,90,30,250,20,250,30)
scrips = dict()

# configure logging
fileName = LOG_PATH + str(time.strftime("%x")).replace("/", "-") + ".log"
logging.basicConfig(filename=fileName, format='{%(levelname)s} %(asctime)s : %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)


####################
## methods start ##
#######################################################

#################################################
# use this method for pulling data from quandl  #
#################################################

def Quandl_Anon_Data_Pull(code, dumpPath):
    logging.info("Fetching the data from quandl")
    qd.ApiConfig.api_key = ""
    try:
        data = qd.get(code, order = "desc", collapse = "daily", column = [1,2,3,4,5])
        data.to_csv(dumpPath)
        logging.info("Data fetched for " + str(code))
    except Exception as e:
        logging.error("Data pull failed for " + str(code) + "due to exception: " + str(e))
# end def

################################################################
# This method will analyse ticker for given duration           #
################################################################

def Analyse(duration, trail, path, tick_value):

    global DUMP_FOLDER_PATH
    data = pd.DataFrame()
    data = pd.read_csv(path)
    index = (250 - duration)
    #data = data.sort(columns="Date", axis = 0)
    data = data.sort_values(by="Date", axis=0)
    data = data[index:]
    data = data[np.isfinite(data["Open"])]
    data = data[data["Open"] != 0]
    #print(data)
    # retain columns: Date, Open, High, Low, Close / Settle
    lst = list(data.columns.values)
    matches = [x for x in lst if x == "Settle"]
    if(len(matches) > 0):
        column_list = ['Date', 'Open', 'High', 'Low', 'Settle']
    else:
        column_list = ['Date', 'Open', 'High', 'Low', 'Last']
    data = data.ix[:, column_list]

    try:
        data["Date"] = pd.to_datetime(data["Date"], format='%Y-%m-%d')
    except Exception as e:
        data["Date"] = pd.to_datetime(data["Date"], format='%m/%d/%Y')
        print e.message

    data = data.sort_values(by="Date", axis=0)

    percent_high = 0.985
    percent_low = 1.015

    #print data.shape[0]
    ctr = 0

    ## declare lists
    highThresh = list()
    lowThresh = list()
    stopLong = list()
    stopShort = list()


    for row in data.iterrows():
        if(ctr < duration):
            highThresh.append(np.nan)
            lowThresh.append(np.nan)
            stopLong.append(np.nan)
            stopShort.append(np.nan)
        # elif(ctr < duration):
        #     highThresh.append(np.nan)
        #     lowThresh.append(np.nan)
        #     slice1 = pd.DataFrame()
        #     slice1 = data[ctr - trail: ctr]
        #     print slice1
        #     trail_high = max(slice1.ix[:, 2].tolist())
        #     trail_low = min(slice1.ix[:, 3].tolist())
        #     stopLong.append(trail_low)
        #     stopShort.append(trail_high)
        else:
            # compute thresholds
            slice = pd.DataFrame()
            slice = data[ctr - duration : ctr]

            #print slice
            high = max(slice.ix[:, 2].tolist())
            low = min(slice.ix[:, 3].tolist())
            _price = slice.ix[:, 4].iloc[0]
            curr_low = slice.ix[:, 3].iloc[0]
            curr_high = slice.ix[:, 2].iloc[0]

            channel_width = ((high - low) / _price) * 100

            if (channel_width < 5):
                percent_high = 0.995
                percent_low = 1.005

            high_thresh = percent_high * high
            low_thresh = percent_low * low

            highThresh.append(round(high_thresh,2))
            lowThresh.append(round(low_thresh,2))

            # compute stop losses
            slice1 = pd.DataFrame()
            slice1 = data[ctr - trail : ctr]
            #print slice1
            trail_high = max(slice1.ix[:, 2].tolist())
            trail_low = min(slice1.ix[:, 3].tolist())
            stopLong.append(trail_low)
            stopShort.append(trail_high)
        #end ladder
        ctr = ctr + 1
    #end for
    # append new columns to data frame
    data["LongThresh"] = highThresh
    data["ShortThresh"] = lowThresh
    data["StopLong"] = stopLong
    data["StopShort"] = stopShort
    max_index = data.shape[0] - duration - 1
    data = data.ix[max_index:]
    try:
        #logging.info("Adding trades in file: " + path + " for the combination: " + str(duration) + " : " + str(trail))
        #AddTrades(data, path, duration, trail, tick_value)
        global  ANALYSIS_PATH
        _path = path.replace(DUMP_FOLDER_PATH, ANALYSIS_PATH)
        rep = "_" + str(duration) + "_" + str(trail) + ".csv"
        _path = _path.replace(".csv", rep)
        data.to_csv(_path)
    except Exception as e:
        logging.error("Could not add trade due to exception:" + e.message)
    #end try catch
#end def

#end def
###############################################################################################
## start the script ##
######################

df_config = pd.DataFrame()
df_config = pd.read_csv(CONFIG_PATH)


#Quandl_Anon_Data_Pull("CHRIS/CME_HG1", DUMP_FOLDER_PATH + "LME Copper" + ".csv")

for r in df_config.iterrows():
    row = r[1]
    _prod = row["Product Name"]
    _code = row["Product Code"]
    _tick_size = row["TickSize"]
    print(_prod + " " + _code + " " + str(_tick_size))
    #uncomment this only when you require to download the data
    #Quandl_Anon_Data_Pull(_code, DUMP_FOLDER_PATH + _prod + ".csv")
    tup_len = len(duration_tuple)
    for index in range(0, tup_len - 1, 2):
        logging.info("Analysing " + _prod + " for combination: " +
                     str(duration_tuple[index]) + " " + str(duration_tuple[index + 1]))
        print("Analysing " + _prod + " for combination: " +
                     str(duration_tuple[index]) + " " + str(duration_tuple[index + 1]))
        Analyse(duration_tuple[index], duration_tuple[index + 1], DUMP_FOLDER_PATH  + _prod + ".csv", _tick_size)
    #end for
logging.info("Done!")
#Quandl_Anon_Data_Pull("CHRIS/CME_YM1", DUMP_FOLDER_PATH + "Dow E-Mini" + ".csv")

    # end for


print("Done!")