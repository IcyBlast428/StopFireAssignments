# Task 4.2
# Write an algorithm to find the average surface temperature (°C) for each day .

import datetime
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

fireDataDF = pd.read_csv('./Datasets/FireData.csv')

def rrPartition_Task_4_2(data, nProcessor): # return data structure: [[], [], [], [], ...]
    result = []
    for i in range(nProcessor):
        result.append([])
    for i in range(len(data)):
        result[i % nProcessor].append(data[i])
    return result

def groupBy_Task_4_2(data): # return data structure: [[], [], [], ...]
    result = [] # [data, sumTemperature, count,  data, sumTemperature, count, ...]
    for i in range(len(data)):
        d = data[i][6]
        if d not in result:
            result.append(d) # count
            result.append(data[i][7])
            result.append(1)
        else:
            result[result.index(d) + 1] += data[i][7]
            result[result.index(d) + 2] += 1
    results = [] # [[data: sumTemperature, count], [data: sumTemperature, count], ...]
    for i in range(len(result)):
        tmp = []
        if i % 3 == 0:
            tmp.append(result[i])
            tmp.append(result[i + 1])
            tmp.append(result[i + 2])
            results.append(tmp)
    return results

def distribute_Task_4_2(data, rangeIndices): # return data structure: [[[], [], [], []], [[], [], [], []], [[], [], [], []], [[], [], [], []]]
    result = []
    new_data = data
    n_bin = len(rangeIndices)
    rangeList = []

    for i in range(n_bin):
        rangeList.append(datetime.datetime.strptime(rangeIndices[i], '%d/%m/%y'))
    
    for i in range(n_bin + 1):
        result.append([])
    
    for item in new_data:
        strpDate = datetime.datetime.strptime(item[0], '%d/%m/%y')
        #index = 0 # item would go to which processor
        for i in range(n_bin):
            if strpDate < rangeList[i]:
                result[i].append(item)
                break
            if strpDate >= rangeList[n_bin - 1]:
                result[n_bin].append(item)
                break
    return result

def aggregation_Task_4_2(data): # return data structure: [[], [], [], []]
    result = []
    for i in range(len(data)):
        for item in data[i]:
            d = []
            for x in result:
                d.append(x[0])
            if item[0] in d:
                index = d.index(item[0])
                result[index][1] += item[1] # sum temperature
                result[index][2] += item[2] # sum count
            else:
                result.append(item)
    return result

def quickSort_Task_4_2(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_4_2([x for x in data[1:] if datetime.datetime.strptime(x[0], '%d/%m/%y') < datetime.datetime.strptime(data[0][0], '%d/%m/%y')]) \
               + [data[0]] + \
               quickSort_Task_4_2([x for x in data[1:] if datetime.datetime.strptime(x[0], '%d/%m/%y') >= datetime.datetime.strptime(data[0][0], '%d/%m/%y')])

def parallelGroupBy_Task_4_2(data, nProcessor):
    results = []
    fireDataList = np.array(data).tolist()
    groupedData = []
    distributedData = []
    pool = mp.Pool(processes = nProcessor)
    partedData = rrPartition_Task_4_2(fireDataList, nProcessor)
    for i in range(nProcessor):
        groupedData.append(pool.apply(groupBy_Task_4_2, [partedData[i]]))
        distributedData.append(pool.apply(distribute_Task_4_2, [groupedData[i], ['1/4/17', '1/7/17', '1/10/17']]))

    aggregateDate = []
    
    for i in range(len(distributedData[0])):
        aggregateDate.append([])
    for i in range(len(distributedData)):
        for j in range(len(distributedData[i])):
            aggregateDate[j].append(distributedData[i][j])
    
    for i in range(len(aggregateDate)):
        aggregateDate[i] = pool.apply(aggregation_Task_4_2, [aggregateDate[i]])
        results.extend((aggregateDate[i]))
    
    for i in range(len(results)):
        results[i][1] = '%.3f'%(results[i][1] / results[i][2])
        del results[i][2]
    
    results = quickSort_Task_4_2(results)
    return results

result = parallelGroupBy_Task_4_2(fireDataDF, 4)
fireData = pd.DataFrame(result, columns = ['Date', 'Average Temperature'])
fireData.to_csv('./ParallelGroupByOutput/ParallelGroupBy_2.csv')
