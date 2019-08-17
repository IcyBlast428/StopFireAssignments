# Task 2.1
# Write an algorithm to find surface temperature (°C), air temperature (°C), relative
# humidity and maximum wind speed.

import datetime
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

fireDataDF = pd.read_csv('./Datasets/FireData.csv')
climateDataDF = pd.read_csv('./Datasets/ClimateData.csv')

fireDataDF.columns = fireDataDF.columns.str.strip()
climateDataDF.columns = climateDataDF.columns.str.strip()

def quickSort_Task_2_1(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_2_1([x for x in data[1:] if datetime.datetime.strptime(x[6], '%d/%m/%y') < datetime.datetime.strptime(data[0][6], '%d/%m/%y')]) \
               + [data[0]] + \
               quickSort_Task_2_1([x for x in data[1:] if datetime.datetime.strptime(x[6], '%d/%m/%y') >= datetime.datetime.strptime(data[0][6], '%d/%m/%y')])

def rangePartition_Task_2_1(data, range_indices, df):
    result = []
    new_data = data
    n_bin = len(range_indices)
    dataIndex = list(df.columns).index('Date')
    for i in range(n_bin):
        s = []
        strpDate2 = datetime.datetime.strptime(range_indices[i], '%d/%m/%y')
        for row in range(len(new_data)):
            strpDate1 = datetime.datetime.strptime(new_data[row][dataIndex], '%d/%m/%y')
            if strpDate1 < strpDate2:
                s.append(new_data[row])
        result.append(s)
        new_data = new_data[len(s):]
    result.append([new_data[row] for row in range(len(new_data)) if datetime.datetime.strptime(new_data[row][dataIndex], '%d/%m/%y') >= datetime.datetime.strptime(range_indices[-1], '%d/%m/%y')])
    return result

def sortMergeJoin_Task_2_1(data1, data2):
    result = [] # the joined table
    i = j = 0
    while True:
        data1Date = datetime.datetime.strptime(data1[i][1], '%d/%m/%y')
        data2Date = datetime.datetime.strptime(data2[j][6], '%d/%m/%y')
        if data1Date < data2Date:
            i += 1
        elif data1Date > data2Date:
            j += 1
        elif data1Date == data2Date:
            tmp = []
            tmp.append(data1[i][1]) # Date
            tmp.append(data2[j][7]) # Surface Temperature
            tmp.append(data1[i][2]) # Air Temperature
            tmp.append(data1[i][3]) # Relative Humidity
            tmp.append(data1[i][5]) # Max Wind Speed
            result.append(tmp)
            j += 1
        if (i == len(data1)) or (j == len(data2)):
            break
    return result

def parellelJoin_Task_2_1(T_climateData, T_fireData, range_indices, n_processor):
    results = []
    pool = mp.Pool(processes = n_processor)
    fireDataList = np.array(T_fireData).tolist()
    climateDataList = np.array(T_climateData).tolist()
    sortedFireData = quickSort_Task_2_1(fireDataList)
    rangedFireData = rangePartition_Task_2_1(sortedFireData, range_indices, T_fireData)
    rangedClimateData = rangePartition_Task_2_1(climateDataList, range_indices, T_climateData)
    for i in range(len(range_indices) + 1):
        result = pool.apply(sortMergeJoin_Task_2_1, [rangedClimateData[i], rangedFireData[i]])
        results.append(result)
    results = [y for x in results for y in x]
    return results

result = parellelJoin_Task_2_1(climateDataDF, fireDataDF, ['1/4/17', '1/7/17', '1/10/17'], 3)
joinedData = pd.DataFrame(result, columns = ['Data', 'Surface Temperature', 'Air Temperature', 'Relative Humidity', 'Max Wind Speed'])
joinedData.to_csv('./ParallelJoinOutput/ParallelJoin_1.csv')
