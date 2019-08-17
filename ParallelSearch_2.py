# Task 1.2
# Write an algorithm to find the latitude , longitude and confidence when the surface
# temperature (°C) was between 65 °C and 100 °C.

import math
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

fireDataDF = pd.read_csv('./Datasets/FireData.csv')

def quickSort_Task_1_2(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_1_2([x for x in data[1:] if x[7] < data[0][7]]) + [data[0]] + quickSort_Task_1_2([x for x in data[1:] if x[7] >= data[0][7]])
    return data

def rangePartition_Task_1_2(data, range_indices):
    result = []
    new_data = tuple(data[1:])
    new_data = quickSort_Task_1_2(new_data)
    new_data = list(new_data)
    n_bin = len(range_indices)
    for i in range(n_bin):
        s = [x for x in new_data if int(x[7]) < range_indices[i]]
        result.append(s)
        new_data = new_data[len(s):]
    result.append([x for x in new_data if int(x[7]) >= range_indices[n_bin - 1]])
    return result

def binarySearchFindFirst_Task_1_2(data, key):
    lower = 0
    higher = len(data) - 1
    matched_record = None
    position = -1
    while lower <= higher:
        middle = int((lower + higher) / 2)
        if int(data[middle][7]) == key:
            matched_record = data[middle]
            position = middle
            while lower < higher:
                middle = math.floor((lower + higher) / 2)
                if int(data[middle][7]) == int(matched_record[7]):
                    matched_record = data[middle]
                    higher = middle
                if int(data[middle][7]) < int(matched_record[7]):
                    lower = middle + 1
            position = lower
            break
        if int(data[middle][7]) > key:
            higher = middle - 1
        if int(data[middle][7]) < key:
            lower = middle + 1
    return position, matched_record

def binarySearchFindLast_Task_1_2(data, key):
    lower = 0
    higher = len(data) - 1
    matched_record = None
    position = -1
    while lower <= higher:
        middle = int((lower + higher) / 2)
        if int(data[middle][7]) == key:
            matched_record = data[middle]
            position = middle
            lower = middle
            while lower < higher:
                middle = math.ceil((lower + higher) / 2)
                if int(data[middle][7]) == int(matched_record[7]):
                    matched_record = data[middle]
                    lower = middle
                if int(data[middle][7]) > int(matched_record[7]):
                    higher = middle - 1
            position = lower
            break
        if int(data[middle][7]) > key:
            higher = middle - 1
        if int(data[middle][7]) < key:
            lower = middle + 1
    return position, matched_record

def parellelSearch_Task_1_2(data, range_indices, query, n_processor):
    result = []
    dataIndex = []
    searchIndex = []
    pool = mp.Pool(processes = n_processor)
    fireDataList = np.array(data).tolist()
    data = rangePartition_Task_1_2(fireDataList, range_indices)
    index = 0
    for num in query:
        for i in range(len(range_indices)):
            if num < range_indices[i]:
                index = i
                break
            else:
                index = i + 1
        dataIndex.append(index)
    
    start = pool.apply(binarySearchFindFirst_Task_1_2, [data[dataIndex[0]], query[0]])
    end = pool.apply(binarySearchFindLast_Task_1_2, [data[dataIndex[1]], query[1]])
    searchIndex.append(start[0])
    searchIndex.append(end[0])

    n = dataIndex[1] - dataIndex[0]
    if n == 0:
        result.append(data[dataIndex[0]][searchIndex[0]:searchIndex[1]+1])
    elif n == 1:
        result.append(data[dataIndex[0]][searchIndex[0]:])
        result.append(data[dataIndex[1]][:searchIndex[1]+1])
    else:
        result.append(data[dataIndex[0]][searchIndex[0]:])
        i = dataIndex[0] + 1
        while i < dataIndex[1]:
            result.append(data[i])
            i += 1
        result.append(data[dataIndex[1]][:searchIndex[1]+1]) 
    results = []
    for x in result:
        for y in x:
            tmp = []
            tmp.append(y[0])    # Latitude
            tmp.append(y[1])    # Longitude
            tmp.append(y[5])    # Confidence
            tmp.append(y[7])    # Surface Temperature (Celcius)
            results.append(tmp)
    return results

result = parellelSearch_Task_1_2(fireDataDF, [50, 70, 90], [65, 100], 3)
fireData = pd.DataFrame(result, columns = ['Latitude', 'Longitude', 'Confidence', 'Surface Temperature (Celcius)'])
fireData.to_csv('./ParallelSearchOutput/ParallelSearch_2.csv')
