# Task 1
# Write an algorithm to search climate data for the records on 15th December 2017.

import datetime
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

climateDataDF = pd.read_csv('./Datasets/ClimateData.csv', sep = ',')

def rangePartition_Task_1_1(data, range_indices):
    result = []
    new_data = data
    n_bin = len(range_indices)
    for i in range(n_bin):
        s = []
        strpDate2 = datetime.datetime.strptime(range_indices[i], '%d/%m/%y')
        for row in range(len(new_data)):
            strpDate1 = datetime.datetime.strptime(new_data[row][1], '%d/%m/%y')
            if strpDate1 < strpDate2:
                s.append(new_data[row])
        result.append(s)
        new_data = new_data[len(s):]
    result.append([new_data[row] for row in range(len(new_data)) if datetime.datetime.strptime(new_data[row][1], '%d/%m/%y') >= datetime.datetime.strptime(range_indices[-1], '%d/%m/%y')])
    return result

def binarySearch_Task_1_1(data, date):
    key = datetime.datetime.strptime(date, '%d/%m/%y')
    matched_record = None
    lower = 0
    middle = 0
    higher = len(data) - 1
    while lower <= higher:
        middle = int((lower + higher) / 2)
        strpDate = datetime.datetime.strptime(data[middle][1], '%d/%m/%y')
        if strpDate == key:
            matched_record = data[middle]
            break
        elif strpDate > key:
            higher = middle - 1
        else:
            lower = middle + 1
    return matched_record

def parellelSearch_Task_1_1(data, query, range_indices, n_processor):
    results = []
    pool = mp.Pool(processes = n_processor)
    climateDataList = np.array(data).tolist()
    rangedData = rangePartition_Task_1_1(climateDataList, range_indices)
    key = datetime.datetime.strptime(query, '%d/%m/%y')
    index = 0
    for i in range(len(range_indices)):
        strpDate = datetime.datetime.strptime(range_indices[i], '%d/%m/%y')
        if key <= strpDate:
            index = i
        else:
            index = i + 1
    results = pool.apply(binarySearch_Task_1_1, [rangedData[index], query])
    return results

result = parellelSearch_Task_1_1(climateDataDF, '15/12/17',['1/4/17', '1/7/17', '1/10/17'], 4)
print(result)
