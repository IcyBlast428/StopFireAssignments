# Task 4.1
# Write an algorithm to get the number of fire in each day.

# Two phase method
import datetime
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

fireDataDF = pd.read_csv('./Datasets/FireData.csv')

def rrPartition_Task_4_1(data, nProcessor): # [[], [], [], []]
    result = []
    for i in range(nProcessor):
        result.append([])
    for i in range(len(data)):
        result[i % nProcessor].append(data[i])
    return result

def groupBy_Task_4_1(data): # {}
    result = {}
    for i in range(len(data)):
        date = data[i][6] # Date
        if date not in result:
            result[date] = 1
        else:
            result[date] += 1
    return result

def distribute_Task_4_1(data, rangeIndices):    # [{}, {}, {}, {}]
    result = []
    new_data = data
    n_bin = len(rangeIndices)
    rangeList = []

    for i in range(n_bin):
        rangeList.append(datetime.datetime.strptime(rangeIndices[i], '%d/%m/%y'))
    
    for i in range(n_bin + 1):
        result.append({})
    
    for item in new_data.items():
        strpDate = datetime.datetime.strptime(item[0], '%d/%m/%y')
        index = 0      # item would go to which processor
        while index < n_bin:
            if strpDate < rangeList[index]:
                result[index].update({item[0]:item[1]})
                break
            else:
                index += 1
        result[index].update({item[0]:item[1]})
    
    return result

def aggregation_Task_4_1(data):
    result = {}
    for i in range(len(data)):
        for item in data[i].items():
            date = item[0]
            if date not in result:
                result[date] = item[1]
            else:
                result[date] += item[1]
    return result

def quickSort_Task_4_1(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_4_1([x for x in data[1:] if datetime.datetime.strptime(x[0], '%d/%m/%y') < datetime.datetime.strptime(data[0][0], '%d/%m/%y')]) \
               + [data[0]] + \
               quickSort_Task_4_1([x for x in data[1:] if datetime.datetime.strptime(x[0], '%d/%m/%y') >= datetime.datetime.strptime(data[0][0], '%d/%m/%y')])

def parallelGroupBy_Task_4_1(data, nProcessor):
    results = {}
    fireDataList = np.array(data).tolist()
    groupedData = []
    distributedData = []
    pool = mp.Pool(processes = nProcessor)
    partedData = rrPartition_Task_4_1(fireDataList, nProcessor)
    for i in range(nProcessor):
        groupedData.append(pool.apply(groupBy_Task_4_1, [partedData[i]]))
        distributedData.append(pool.apply(distribute_Task_4_1, [groupedData[i], ['1/4/17', '1/7/17', '1/10/17']]))
    
    aggregateDate = []
    for i in range(len(distributedData[0])):
        aggregateDate.append([])
    for i in range(len(distributedData)):
        for j in range(len(distributedData[i])):
            aggregateDate[j].append(distributedData[i][j])
    
    for i in range(len(aggregateDate)):
        aggregateDate[i] = pool.apply(aggregation_Task_4_1, [aggregateDate[i]])
        results.update(aggregateDate[i])
    result = []
    for i in results.items():
        result.append(list(i))
    result = quickSort_Task_4_1(result)
    return result

result = parallelGroupBy_Task_4_1(fireDataDF, 4)
fireData = pd.DataFrame(result, columns = ['Date', 'Count'])
fireData.to_csv('./ParallelGroupByOutput/ParallelGroupBy_1.csv')
