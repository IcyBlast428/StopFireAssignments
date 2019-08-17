#Task 5
# Write an algorithm to find the average surface temperature (°C) for each weather station.

import datetime
import numpy as np 
import pandas as pd 
import multiprocessing as mp 

fireDataDF = pd.read_csv('./Datasets/FireData.csv')
climateDataDF = pd.read_csv('./Datasets/ClimateData.csv')

fireDataDF.columns = fireDataDF.columns.str.strip()
climateDataDF.columns = climateDataDF.columns.str.strip()

#fireDataList = np.array(fireDataDF).tolist()
#climateDataList = np.array(climateDataDF).tolist()

def quickSort_Task_5(data):
    if len(data) <= 1:
        return data
    else:
        return quickSort_Task_5([x for x in data[1:] if datetime.datetime.strptime(x[6], '%d/%m/%y') < datetime.datetime.strptime(data[0][6], '%d/%m/%y')]) \
               + [data[0]] + \
               quickSort_Task_5([x for x in data[1:] if datetime.datetime.strptime(x[6], '%d/%m/%y') >= datetime.datetime.strptime(data[0][6], '%d/%m/%y')])

#sortedFireData = quickSort_Task_5(fireDataList)
#sortedClimateData = climateDataList
#print(sortedFireData)

def rangePartition_Task_5(data, range_indices, df):
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

#rangedFireData = rangePartition_Task_5(sortedFireData, ['1/4/17', '1/7/17', '1/10/17'], fireDataDF)
#rangedClimateData = rangePartition_Task_5(sortedClimateData, ['1/4/17', '1/7/17', '1/10/17'], climateDataDF)
#print(rangedClimateData[0])
#print(rangedFireData[0])

def sortMergeJoin_Task_5(data1, data2):
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
            tmp.append(data1[i][0]) # Station
            tmp.append(data1[i][1]) # date
            tmp.append(data2[j][7]) # Surface Temperature
            result.append(tmp)
            j += 1
        if (i == len(data1)) or (j == len(data2)):
            break
    return result

#joinedData = sortMergeJoin_Task_5(rangedClimateData[0], rangedFireData[0])
#print(joinedData)
#print(len(joinedData))

def groupBy_Task_5(data): # return data structure: [[], [], [], ...]
    result = [] # [data, sumTemperature, count,  data, sumTemperature, count, ...]
    for i in range(len(data)):  # scan the list
        stationNumber = data[i][0]
        if stationNumber not in result:
            result.append(stationNumber) # station
            result.append(data[i][2]) # Surface Temperature
            result.append(1) # count
        else:   # if station number exist in the result list
            result[result.index(stationNumber) + 1] += data[i][2] # surfaceTemperature += newSurfaceTemperature
            result[result.index(stationNumber) + 2] += 1 # stationNumber++
    results = [] # [[data, sumTemperature, count], [data, sumTemperature, count], ...]
    for i in range(len(result)):
        tmp = []
        if i % 3 == 0:
            tmp.append(result[i])
            tmp.append(result[i + 1])
            tmp.append(result[i + 2])
            results.append(tmp)
    return results

#groupedData = groupBy_Task_5(joinedData)
#print(groupedData)

def aggregation_Task_5(data): # return data structure: [[], [], [], []]
    result = []
    for i in range(len(data)):
        for item in data[i]:
            station = []
            for x in result:
                station.append(x[0])
            if item[0] in station:
                index = station.index(item[0])
                result[index][1] += item[1]
                result[index][2] += item[2]
            else:
                result.append(item)
    return result

def parallelGroupBy_Task_5(climateDataDF, fireDataDF, nProcessor):
    results = []
    climateDataList = np.array(climateDataDF).tolist()
    fireDataList = np.array(fireDataDF).tolist()
    groupedData = []
    rangeIndices = ['1/4/17', '1/7/17', '1/10/17']
    pool = mp.Pool(processes = nProcessor)
    sortedFireData = quickSort_Task_5(fireDataList)
    sortedClimateData = climateDataList

    rangedFireData = rangePartition_Task_5(sortedFireData, rangeIndices, fireDataDF)
    rangedClimateData = rangePartition_Task_5(sortedClimateData, rangeIndices, climateDataDF)

    joinedData = []
    for i in range(nProcessor):
        joinedData.append(pool.apply(sortMergeJoin_Task_5, [rangedClimateData[i], rangedFireData[i]]))
        groupedData.append(pool.apply(groupBy_Task_5, [joinedData[i]]))

    results.extend(pool.apply(aggregation_Task_5, [groupedData]))
    
    for i in range(len(results)):
        results[i][1] = '%.3f'%(results[i][1] / results[i][2])
        del results[i][2]

    return results

result = parallelGroupBy_Task_5(climateDataDF, fireDataDF, 4)
print(result)
