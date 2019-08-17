# Task 2.2
# Write an algorithm to find datetime, air temperature (°C), surface temperature (°C) and
# confidence when the confidence is between 80 and 100.

import datetime
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

fireDataDF = pd.read_csv('./Datasets/FireData.csv')
climateDataDF = pd.read_csv('./Datasets/ClimateData.csv')

def quickSort_Task_2_2(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_2_2([x for x in data[1:] if datetime.datetime.strptime(x[6], '%d/%m/%y') < datetime.datetime.strptime(data[0][6], '%d/%m/%y')]) \
               + [data[0]] + \
               quickSort_Task_2_2([x for x in data[1:] if datetime.datetime.strptime(x[6], '%d/%m/%y') >= datetime.datetime.strptime(data[0][6], '%d/%m/%y')])

def rangePartition_Task_2_2(data, range_indices):
    result = []
    new_data = data
    n_bin = len(range_indices)
    for i in range(n_bin):
        s = [x for x in new_data if x[5] < range_indices[i]]
        result.append(s)
        new_data = new_data[len(s):]
    result.append([x for x in new_data if x[5] >= range_indices[n_bin - 1]])
    return result

def sortMergeJoin_Task_2_2(data1, data2):
    result = []
    i = j = 0
    while True:
        data1Date = datetime.datetime.strptime(data1[i][1], '%d/%m/%y')
        data2Date = datetime.datetime.strptime(data2[j][6], '%d/%m/%y')
        if data1Date < data2Date:
            i += 1
        elif data1Date > data2Date:
            j += 1
        else:
            tmp = []
            tmp.append(data1[i][1]) # Date
            tmp.append(data1[i][2]) # Air Temperature
            tmp.append(data2[j][7]) # Surface Temperature
            tmp.append(data2[j][5]) # Confidence
            result.append(tmp)
            j += 1
        if (i == len(data1)) or (j == len(data2)):
            break
    return result

def parellelJoin_Task_2_2(T_climateData, T_fireData, rangeIndices, nProcessor):
    results = []
    pool = mp.Pool(processes = nProcessor)
    fireDataList = np.array(T_fireData).tolist()
    climateDataList = np.array(T_climateData).tolist()
    rangedFireData = rangePartition_Task_2_2(fireDataList, rangeIndices)
    sortedFireData = []
    for i in range(len(rangedFireData)):
        sortedFireData.append(pool.apply(quickSort_Task_2_2, [rangedFireData[i]]))
    for i in range(1, len(sortedFireData)):
        results.append(pool.apply(sortMergeJoin_Task_2_2, [climateDataList, sortedFireData[i]]))
    results = [y for x in results for y in x]
    return results

joinedData = parellelJoin_Task_2_2(climateDataDF, fireDataDF, [80, 100], 4)
result = pd.DataFrame(joinedData, columns = ['Date', 'Air Temperature', 'Surface Temperature', 'Confidence'])
result.to_csv('./ParallelJoinOutput/ParallelJoin_2.csv')
