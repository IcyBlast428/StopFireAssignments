# Task 3
# Write an algorithm to sort fire data based on surface temperature in a ascending order.
##########################################################################################
# Round-Robin partition

import sys
import numpy as np
import pandas as pd 
import multiprocessing as mp

fireDataDF = pd.read_csv('./Datasets/FireData.csv')

def rrPartition_Task_3(data, n_processor):
    result = []
    for i in range(n_processor):
        result.append([])
    for i in range(len(data)):
        result[i % n_processor].append(data[i])
    return result

def quickSort_Task_3(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_3([x for x in data[1:] if x[7] < data[0][7]]) + [data[0]] + quickSort_Task_3([x for x in data[1:] if x[7] >= data[0][7]])
    return data

def kWayMergeFindMin_Task_3(records):
    m = records[0]
    index = 0
    for i in range(len(records)):
        if records[i] < m:
            index = i
            m = records[i]
    return index

def kWayMerge_Task_3(record_sets):
    indexes = []
    for x in range(len(record_sets)):
        indexes.append(x - x)     # indexes [0, 0, 0, 0]
    result = []
    while 1:
        tuples = []
        for i in range(len(record_sets)):
            if indexes[i] >= len(record_sets[i]):
                tuples.append(sys.maxsize)
            else:
                tuples.append(record_sets[i][indexes[i]][7])
        smallest = kWayMergeFindMin_Task_3(tuples)
        if tuples[smallest] == sys.maxsize:
            break
        result.append(record_sets[smallest][indexes[smallest]])
        indexes[smallest] += 1
    return result

def parallelSort_Task_3(data, nProcessor):
    results = []
    pool = mp.Pool(processes = nProcessor)
    fireDataList = np.array(data).tolist()
    partedData = rrPartition_Task_3(fireDataList, nProcessor)
    for i in range(len(partedData)):
        partedData[i] = pool.apply(quickSort_Task_3, [partedData[i]])
    results = pool.apply(kWayMerge_Task_3, [partedData])
    return results

result = parallelSort_Task_3(fireDataDF, 4)
fireData = pd.DataFrame(result, columns = fireDataDF.columns)
fireData.to_csv('./ParallelSortOutput/ParallelSort.csv')
