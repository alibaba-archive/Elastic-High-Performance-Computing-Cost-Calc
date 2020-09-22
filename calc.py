# -*- coding: utf-8 -*-
import csv
import codecs
import os, sys
from datetime import datetime
import time
import tempfile
import zipfile
import logging
import copy
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
import configparser


logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
FLOAT_FORMAT = '%.2f'
TMP_FILE_PATH = './'
CLEAN_TMP_FILE = True
ORVERWRITE_FILE = True

PRODUCT_CODE_INVOLVED = ('ecs')
SUBSCRIPTION_TYPE_INVOLVED = ('PayAsYouGo', 'Subscription')
DURATIONUNIT_INVOLVED = ('Year', 'Month', 'Week', 'Second')
BILLINGITE_INVOLVED =('OsDisk', 'Cloudserverconfiguration')

BILL_HEADER_TRANSLATE_MAP = {
    u'产品Code': 'ProductCode',
    'Product Code': 'ProductCode',
    u'消费类型': 'SubscriptionType',
    'Subscription Type': 'SubscriptionType',
    u'账单开始时间': 'UsageStartTime',
    'Usage Start Time': 'UsageStartTime',
    u'账单结束时间': 'UsageEndTime',
    'Usage End Time': 'UsageEndTime',
    u'服务时长': 'ServiceDuration',
    'Service Duration': 'ServiceDuration',
    u'实例ID': 'InstanceId',
    'Instance ID': 'InstanceId',
    u'计费项': 'BillingItem',
    'Billing Item': 'BillingItem',
    u'单价': 'ListPrice',
    'List Price': 'ListPrice',
    u'用量': 'Usage',
    'Usage': 'Usage',
    u'原价': 'PreTaxGrossAmount',
    'Pretax Gross Amount': 'PreTaxGrossAmount',
    u'优惠金额': 'InvoiceDiscount',
    'Invoice Discount': 'InvoiceDiscount',
    u'优惠券抵扣': 'DeductedByCoupons',
    'Deducted By Coupons': 'DeductedByCoupons',
    u'应付金额': 'PreTaxAmount',
    'Pretax Amount': 'PreTaxAmount',
    u'时长单位': 'DurationUnit',
    'Duration Unit': 'DurationUnit',
    u'优惠名称':'DiscountName',
    'Discount Name':'DiscountName'
}
BILL_VALUE_SUBSCRIPTIONTYPE_MAP = {
    u'预付费': 'Subscription',
    'Subscription': 'Subscription',
    u'后付费': 'PayAsYouGo',
    'Pay-As-You-Go': 'PayAsYouGo'
}
BILL_VALUE_DISCOUNTNAME_MAP = {
    u'预设优惠_spotIntanc': 'spotIntanc'
}
BILL_VALUE_DURATIONUNIT_MAP = {
    u'月': 'Month',
    'Month': 'Month',
    u'秒': 'Second',
    'Second': 'Second',
    u'年': 'Year',
    'Year': 'Year',
    u'周': 'Week',
    'Week': 'week',
    u'日': 'Day',
    'Day': 'Day'
}

BILL_VALUE_BILLINGITEM_MAP = { #need to extend
    u'系统盘':'OsDisk',
    'Os Disk':'OsDisk',
    u'云服务器配置': 'Cloudserverconfiguration',
    'Cloud server configuration': 'Cloudserverconfiguration',

    'systemdisk':'systemdisk',
    'instance_type':'instance_type',
    u'带宽':'Bandwidth',
    'Bandwidth':'Bandwidth',
    u'流出流量':'Outgoingtraffic',
    'Outgoingtraffic':'Outgoing traffic'
}

JOBDETAIL_HEADER_MAP = {
    'jobId': 'jobId',
    'taskId': 'taskId',
    'jobName': 'jobName',
    'user': 'user',
    'queue': 'queue',
    'hostname': 'hostname',
    'usedCPU': 'usedCPU',
    'usedMem': 'usedMem',
    'startTime': 'startTime',
    'endTime': 'endTime',
    'exitStatus': 'exitStatus',
    'instanceId': 'instanceId',
    'instanceCores': 'instanceCores'
}

OUTPUT_HEADER_MAP = {
    'jobId': u'jobId',
    'taskId': u'taskId',
    'jobName': u'jobName',
    'user': u'user',
    'queue': u'queue',
    'hostname': u'hostname',
    'usedCPU': u'usedCPU',
    'usedMem': u'usedMem',
    'startTime': u'startTime',
    'endTime': u'endTime',
    'exitStatus': u'exitStatus',
    'instanceId': u'instanceId',
    'instanceCores': u'instanceCores',
    'PreTaxGrossAmount': u'原价',
    'PreTaxAmount': u'应付金额'
}

def buildColumnIndex(header, translateMap):
    index = {}
    for i, field in enumerate(header):
        if field in translateMap:
            index[translateMap[field]] = i
    # check if header miss field
    fieldNeedsSet = set(translateMap.values())
    indexSet = set(index.keys())
    diff = fieldNeedsSet - indexSet
    if len(diff) > 0:
        raise Exception('missing fields: ' + ','.join(diff))
    else:
        return index

def buildHeaderFromColumnIndex(columnIndex):
    headerLen = max(columnIndex.values()) + 1
    header = [None] * headerLen
    for key, idx in columnIndex.items():   
        header[idx] = key
    return header

def getValueByColumnIndex(row, key, columnIndex):
    '''Get value from row by column index'''
    index = columnIndex.get(key, None)
    if index is None:
        raise Exception('key %s not exists in column index' % key)
    return row[index]

def modifyValueByColumnIndex(row, key, columnIndex, to_add):
    """Modify value (+ to_add) from row by column index"""
    index = columnIndex.get(key, None)
    if index is None:
        raise Exception('key %s not exists in column index' % key)
    if key == "hostname":
        row[index] = row[index] + " " + to_add
    else:
        row[index] = float(row[index]) + to_add


class FilterClass:
    """Filter base class. Filter rows by filter(rowArray),
    return True if row accepted,
    format value if needed,
    set meta if needed"""
    def __init__(self, columnIndex):
        self.columnIndex = columnIndex

    def getFieldIndex(self, headerKey):
        return self.columnIndex[headerKey]

    def getValue(self, row, headerKey):
        return getValueByColumnIndex(row, headerKey, self.columnIndex)

    def filter(self, rowArray, meta):
        raise Exception('use derived class')

'''filter by ProductCode = ecs'''
class ProductCodeFilter(FilterClass):
    """Filter rows by ProductCode"""
    def filter(self, rowArray, meta):
        value = self.getValue(rowArray, 'ProductCode')
        return value in PRODUCT_CODE_INVOLVED

class SubscriptionTypeFilter(FilterClass):
    '''Filter rows by SubscriptionType'''
    def filter(self, rowArray, meta):
        indexSubType = self.getFieldIndex('SubscriptionType')
        valueStr = rowArray[indexSubType]
        value = BILL_VALUE_SUBSCRIPTIONTYPE_MAP.get(valueStr, None)
        if not value:
            raise Exception('unknown subscription type: %s' % valueStr)
        rowArray[indexSubType] = value
        meta['subscriptionType'] = value   
        ## TODO: support 预付费
        return value in SUBSCRIPTION_TYPE_INVOLVED

class TimeFilter(FilterClass):
    '''Filter rows by startTime & endTime'''
    def __init__(self, columnIndex, startTime, endTime):
        self.columnIndex = columnIndex
        self.startTime = startTime
        self.endTime = endTime

    def filter(self, rowArray, meta):
        indexStartTime = self.getFieldIndex('UsageStartTime')
        indexEndTime = self.getFieldIndex('UsageEndTime')
        vStart = parseLocalTimeToTimestamp(rowArray[indexStartTime])
        vEnd = parseLocalTimeToTimestamp(rowArray[indexEndTime])
        rowArray[indexStartTime] = vStart
        rowArray[indexEndTime] = vEnd
        meta['startTime'] = int(vStart // 3600 * 3600)
        meta['endTime'] = int((vEnd + 3599) // 3600 * 3600)
        if self.startTime and self.endTime:
            return meta['endTime'] > self.startTime and meta['startTime'] < self.endTime
            '''startTime <= [] <= endTime'''
        else:
            return True


class DurationFilter(FilterClass):
    '''Filter rows by ServiceDuration & DurationUnit'''
    def filter(self, rowArray, meta):
        indexDurationUnit = self.getFieldIndex('DurationUnit')
        indexServiceDuration = self.getFieldIndex('ServiceDuration')
        unitStr = rowArray[indexDurationUnit]
        durationStr = rowArray[indexServiceDuration]
        if not durationStr.isdigit():
            raise Exception('duration not a digit: %s' % durationStr)

        unit = BILL_VALUE_DURATIONUNIT_MAP.get(unitStr, None)
        if not unit:
            raise Exception('unknown duration unit: %s' % unitStr)
        if unit == 'Month':
            duration = int(durationStr) * 2592000
        elif unit == 'Week':
            duration = int(durationStr) * 604800
        elif unit == 'Year':
            duration = int(durationStr) * 365 * 86400
        elif unit == 'Day':
            duration = int(durationStr) * 86400
        else:
            duration = int(durationStr)

        rowArray[indexDurationUnit] = 'Second'
        rowArray[indexServiceDuration] = duration
        ## TODO: support 月，年，周
        return duration > 0 and unit in DURATIONUNIT_INVOLVED

class AmountFormatter(FilterClass):
    '''Format PreTaxGrossAmount & PreTaxAmount in row'''
    def filter(self, rowArray, meta):
        indexGrossAmount = self.getFieldIndex('PreTaxGrossAmount')
        grossAmountStr = rowArray[indexGrossAmount]
        try:
            grossAmount = float(grossAmountStr)
        except ValueError:
            raise Exception('PreTaxGrossAmount not a float number: %s' % grossAmountStr)
        rowArray[indexGrossAmount] = grossAmount

        indexAmount = self.getFieldIndex('PreTaxAmount')
        amountStr = rowArray[indexAmount]
        try:
            amount = float(amountStr)
        except ValueError:
            raise Exception('PreTaxAmount not a float number: %s' % amountStr)
        rowArray[indexAmount] = amount

        return True

class BillingItemFilter(FilterClass):
    '''Filter rows by BillingItemFilter'''
    def filter(self, rowArray, meta):
        indexBillingItem = self.getFieldIndex('BillingItem')
        valueStr = rowArray[indexBillingItem]
        value = BILL_VALUE_BILLINGITEM_MAP.get(valueStr, None)
        rowArray[indexBillingItem] = value
        if not value:
            rowArray[indexBillingItem] = valueStr
        return value in BILLINGITE_INVOLVED

class DiscountNameFilter(FilterClass):
    '''Filter rows by SubscriptionType'''
    def filter(self, rowArray, meta):
        indexDiscountName = self.getFieldIndex('DiscountName')
        valueStr = rowArray[indexDiscountName]

        value = BILL_VALUE_DISCOUNTNAME_MAP.get(valueStr, '')
        rowArray[indexDiscountName] = value
        return True


'''parse Time to TimeStamp'''
def parseTime(timeStr, timeFormat = TIME_FORMAT):
    return datetime.strptime(timeStr, timeFormat)

def parseLocalTimeToTimestamp(timeStr, timeFormat = TIME_FORMAT):
    _t = parseTime(timeStr, timeFormat)
    return int(time.mktime(_t.timetuple()))

def loadBillingDetailToDict(billingDetailFileName, billingDetailDict = {}, startTime = None, endTime = None):
    '''Load billing detail file, return columnIndex & data dict'''
    filters = []
    retDict = billingDetailDict
    columnIndex = {}
    with open(billingDetailFileName, encoding='UTF-8') as fd1:
        logging.info('Start loading %s ...' % billingDetailFileName)
        detailReader = csv.reader(fd1)

        header = [field.strip() for field in next(detailReader)]

        columnIndex = buildColumnIndex(header, BILL_HEADER_TRANSLATE_MAP)

        productFilter = ProductCodeFilter(columnIndex)
        filters.append(productFilter)

        '''subTypeFilter optional? No!,translate cn to en'''
        subTypeFilter = SubscriptionTypeFilter(columnIndex)
        filters.append(subTypeFilter)

        timeFilter = TimeFilter(columnIndex, startTime, endTime)
        filters.append(timeFilter)

        durationFilter = DurationFilter(columnIndex)
        filters.append(durationFilter)

        amountFormatter = AmountFormatter(columnIndex)
        filters.append(amountFormatter)

        billingitemFilter = BillingItemFilter(columnIndex)
        filters.append(billingitemFilter)

        discountFilter = DiscountNameFilter(columnIndex)
        filters.append(discountFilter)

        lineCount = 0
        for row in detailReader:
            items = [item.strip() for item in row]
            useful = True
            meta = {}
            for f in filters:
                if not f.filter(items, meta):
                    useful = False
                    break
            lineCount += 1
            if lineCount % 10000 == 0:
                logging.info('Load %d rows' % lineCount)
            if useful:
                subscription = items[columnIndex['SubscriptionType']]
                instanceId = items[columnIndex['InstanceId']]
                billingitem = items[columnIndex['BillingItem']]
                if instanceId not in retDict:
                    retDict[instanceId] = [{}, {}]
                if billingitem == 'Cloudserverconfiguration':
                    index = 0
                else:
                    index = 1

                '''retDict[instanceId][0]: PayAsYouGo items'''
                '''retDict[instanceId][1]: Subscription items'''
                '''retDict[instanceId][*][(startTime, endTime)][0]: Cloudserverconfiguration fee'''
                '''retDict[instanceId][*][(startTime, endTime)][1]: OsDisk fee'''

                if subscription == 'PayAsYouGo':
                    startTime = meta['startTime']
                    endTime = meta['endTime']
                    if (startTime, endTime) in retDict[instanceId][0]:
                        if len(retDict[instanceId][0][(startTime, endTime)][index]) == 0:
                            retDict[instanceId][0][(startTime, endTime)][index] = items
                        else:
                            temp = retDict[instanceId][0][(startTime, endTime)][index]
                            temp = MergeTwoRow(temp, items, columnIndex)
                            if temp[columnIndex['ServiceDuration']] > 3600:
                                raise Exception('unsupport PayAsYouGo Service duration > 3600') #need to modify
                            retDict[instanceId][0][(startTime, endTime)][index] = temp
                    else:
                        retDict[instanceId][0][(startTime, endTime)] = [[],[]]
                        retDict[instanceId][0][(startTime, endTime)][index] = items

                    '''预付费'''
                else:
                    startTime = items[columnIndex['UsageStartTime']]
                    endTime = items[columnIndex['UsageEndTime']]
                    if (startTime, endTime) in retDict[instanceId][1]:
                        retDict[instanceId][1][(startTime, endTime)][index] = items
                    else:
                        retDict[instanceId][1][(startTime, endTime)] = [[], []]
                        retDict[instanceId][1][(startTime, endTime)][index] = items
        logging.info('Finish loading %s, %d rows' % (billingDetailFileName, lineCount))
    return columnIndex, retDict

def splitDurationByHours(startTime, endTime):
    '''Split duration (timestamp) into list of one hour short periods'''
    startSharp = startTime // 3600 * 3600
    _t = startSharp
    ret = []
    while _t + 3600 < endTime:
        if _t < startTime:
            ret.append((startTime, _t + 3600))
        else:
            ret.append((_t, _t + 3600))
        _t += 3600
    if _t < startTime:
        ret.append((startTime, endTime))
    else:
        ret.append((_t, endTime))
    logging.debug('split (%d, %d) by hours %s' % (startTime, endTime, str(ret)))
    return ret

def getRowFromBillingDetailDict(billingDetailDict, instanceId, timeTuple):
    '''Get row from billing detail dict by instanceId, (startTime, endTime)'''
    start, end = timeTuple
    startSharp = int(start) // 3600 * 3600
    endSharp = (int(end) + 3599) // 3600 * 3600
    buckets = billingDetailDict.get(instanceId, None)
    if not buckets:
        raise Exception('Instance not found in billing detail: %s' % instanceId)
    ## TODO: support 预付费
    bucketPayAsYouGo = buckets[0]
    bucketsubscription = buckets[1]
    row = copy.deepcopy(bucketPayAsYouGo.get((startSharp, endSharp), None))

    if len(bucketsubscription) > 0:
        for st, ed in bucketsubscription.keys():
            if start >= st and end <= ed:
                if row is not None:
                    subscriptionList = bucketsubscription.get((st, ed), None)
                    if subscriptionList is not None:    #[[],[]]
                        for sub in subscriptionList:
                            if len(sub) != 0:
                                row.append(sub)
                else:
                    row = bucketsubscription.get((st, ed), None)
    if not row:
        start_t = datetime.fromtimestamp(startSharp).strftime(TIME_FORMAT)
        end_t = datetime.fromtimestamp(endSharp).strftime(TIME_FORMAT)
        logging.warning('Missing row of (%s, %s) for instance %s' % (start_t, end_t, instanceId))
    return row



def loadJobDetailAndMergeBillingDetail(jobDetailFileList, billingColumnIndex, billingDetailDict, filter_startTime, filter_endTime):
    '''Load job detail file, fill rows with billing amount, return columnIndex & rows'''
    retRows = []
    columnIndex = {}
    spotDict = {}
    detailcolumnIndex = {}
    detailRows = []
    detailrow = []

    num_file = 1
    total_fee = {}
    instance_set = set()
    for jobDetailFileName in jobDetailFileList:
        with open(jobDetailFileName, encoding='UTF-8') as fd1:
            logging.info('Start loading %s ...' % jobDetailFileName)
            detailReader = csv.reader(fd1)

            # header = next(detailReader)[0].strip().split(",")
            header = [field.strip() for field in next(detailReader)]
            if num_file == 1:
                global_header = copy.deepcopy(header)
                columnIndex = buildColumnIndex(header, JOBDETAIL_HEADER_MAP)
                columnIndex['PreTaxGrossAmount'] = len(header)
                columnIndex['PreTaxAmount'] = len(header) + 1
                columnIndex['CpuTime'] = len(header) + 2
                columnIndex['CoreHours'] = len(header) + 3

                detailcolumnIndex = copy.deepcopy(columnIndex)
                detailcolumnIndex['ItemType'] =  len(header) + 2    #计费项：系统盘 or 云服务器配置
                detailcolumnIndex['Subscription'] = len(header) + 3 #付费类型   预付费 or 后付费
                detailcolumnIndex['InstanceType'] = len(header) + 4 #实例类型   按时按量 or 包年包月 or 抢占式实例
                detailRows = []
            else:
                if header != global_header:
                    raise Exception("file header is differnet")


            taskIdIndex = columnIndex.get('taskId', None)
            userIndex = columnIndex.get('user', None)
            queueIndex = columnIndex.get('queue', None)
            hostnameIndex = columnIndex.get('hostname', None)

            lineCount = 0
            i = 0
            for row in detailReader:
                '''Error Type : {0: no error, 1: }'''
                items = [item.strip() for item in row]
                lineCount += 1

                taskId = items[taskIdIndex]
                if not taskId.isdigit():
                    items[taskIdIndex] = taskId.lower()

                items[userIndex] = items[userIndex].lower()
                # items[queueIndex] = items[queueIndex].lower()
                items[hostnameIndex] = items[hostnameIndex].lower()

                if lineCount % 1000 == 0:
                    logging.info('Load %d rows' % lineCount)

                jobStartTime = int(getValueByColumnIndex(items, 'startTime', columnIndex))
                jobEndTime = int(getValueByColumnIndex(items, 'endTime', columnIndex))
                if filter_startTime and filter_endTime:
                    if not (filter_startTime <= jobStartTime and jobEndTime <= filter_endTime):
                        continue

                jobId = getValueByColumnIndex(items, 'jobId', columnIndex)
                queue = getValueByColumnIndex(items, 'queue', columnIndex)
                instanceId = getValueByColumnIndex(items, 'instanceId', columnIndex)
                if not instanceId:
                    raise Exception('Missing instanceId of job detail, jobId: %s' % jobId)
                jobUsedCpu = int(getValueByColumnIndex(items, 'usedCPU', columnIndex))
                instanceCpu = int(getValueByColumnIndex(items, 'instanceCores', columnIndex))

                grossAmount_sum = 0.0
                amount_sum = 0.0
                portion = float(jobUsedCpu) / instanceCpu
                periods = splitDurationByHours(jobStartTime, jobEndTime)

                if instanceId not in instance_set:
                    instance_set.add(instanceId)
                    buckets = billingDetailDict.get(instanceId, None)
                    total = 0
                    for key, value in buckets[0].items():
                        if filter_startTime and filter_endTime:
                            if key[1] > filter_startTime and key[0] < filter_endTime:
                                fee = getValueByColumnIndex(value[0], 'PreTaxAmount', billingColumnIndex)
                        else:
                            print(len(value[0]))
                            if len(value[0]):
                                fee = getValueByColumnIndex(value[0], 'PreTaxAmount', billingColumnIndex)
                            else:
                                fee = 0
                        total += float(fee)
                    if queue not in total_fee:
                        total_fee[queue] = total
                    else:
                        total_fee[queue] += total

                for p in periods:
                    bills = getRowFromBillingDetailDict(billingDetailDict, instanceId, p)
                    if bills is None:
                        continue
                    for bill in bills:
                        if len(bill) != 0:
                            logging.debug('match row (%s, %d, %d): %s' % (instanceId, jobStartTime, jobEndTime, str(bill)))

                            billGrossAmount = getValueByColumnIndex(bill, 'PreTaxGrossAmount', billingColumnIndex)
                            billAmount = getValueByColumnIndex(bill, 'PreTaxAmount', billingColumnIndex)
                            billStartTime = getValueByColumnIndex(bill, 'UsageStartTime', billingColumnIndex)
                            billEndTime = getValueByColumnIndex(bill, 'UsageEndTime', billingColumnIndex)
                            billListPrice = float(getValueByColumnIndex(bill, 'ListPrice', billingColumnIndex))
                            billItem = getValueByColumnIndex(bill, 'BillingItem', billingColumnIndex)
                            billSubscriptionType = getValueByColumnIndex(bill, 'SubscriptionType', billingColumnIndex)
                            billDiscountName = getValueByColumnIndex(bill, 'DiscountName', billingColumnIndex)
                            jobUsedTimePart = p[1] - p[0]
                            temp_grossAmount = billGrossAmount * portion * jobUsedTimePart / (billEndTime - billStartTime)
                            temp_amount = billAmount * portion * jobUsedTimePart / (billEndTime - billStartTime)
                            grossAmount_sum += temp_grossAmount
                            amount_sum += temp_amount

                            detailrow = copy.deepcopy(items)
                            billStartTimeIndex = detailcolumnIndex.get('startTime', None)
                            billEndTimeIndex = detailcolumnIndex.get('endTime', None)
                            detailrow[billStartTimeIndex] = p[0]
                            detailrow[billEndTimeIndex] = p[1]

                            detailrow.append(FLOAT_FORMAT % temp_grossAmount)
                            detailrow.append(FLOAT_FORMAT % temp_amount)
                            detailrow.append(billItem)
                            detailrow.append(billSubscriptionType)
                            if billSubscriptionType == 'Subscription':
                                detailrow.append('MonthlyOrAnnualPayment')
                            else:
                                if billDiscountName == 'spotIntanc':
                                    detailrow.append('Spot')
                                    tempDict = {}

                                    if instanceId in spotDict:
                                        if billItem in spotDict[instanceId]:
                                            spotDict[instanceId][billItem][(billStartTime, billEndTime)] = billListPrice
                                        else:
                                            temp = {(billStartTime, billEndTime): billListPrice}
                                            spotDict[instanceId][billItem] = temp
                                    else:
                                        temp = {(billStartTime, billEndTime): billListPrice}
                                        spotDict[instanceId] = {billItem: temp}
                                else:
                                    detailrow.append('PayByQuantity')


                            detailRows.append(detailrow)
                    i = i + 1
                if grossAmount_sum <= 0:
                    logging.warning('Got 0.0 fee for job %s' % jobId)
                items.append(FLOAT_FORMAT % grossAmount_sum)
                items.append(FLOAT_FORMAT % amount_sum)
                cputime = jobEndTime - jobStartTime
                items.append(cputime)
                CoreHours = cputime / 3600 * jobUsedCpu
                items.append(CoreHours)
                retRows.append(items)
            logging.info('Finish loading %s, %d rows' % (jobDetailFileName, lineCount))
            num_file = num_file + 1
    return columnIndex, retRows, detailcolumnIndex, detailRows, spotDict, total_fee

def MergeTwoRow(firstrow, lastrow, columnIndex):
    temprow = copy.deepcopy(firstrow)
    floatTypeKey = ['PreTaxGrossAmount', 'PreTaxAmount', 'CoreHours']
    intTypeKey = ['usedCPU', 'usedMem', 'instanceCores', 'ServiceDuration', 'CpuTime']

    for key, value in columnIndex.items():
        if key in floatTypeKey:
            temprow[value] = float(firstrow[value]) + float(lastrow[value])
        elif key in intTypeKey:
            temprow[value] = int(firstrow[value]) + int(lastrow[value])
        elif key == 'UsageStartTime':
            if firstrow[value] < lastrow[value]:
                temprow[value] = firstrow[value]
            else:
                temprow[value] = lastrow[value]
        elif key == 'UsageEndTime':
            if firstrow[value] > lastrow[value]:
                temprow[value] = firstrow[value]
            else:
                temprow[value] = lastrow[value]
        else:
            if firstrow[value] != lastrow[value]:
                temprow[value] = firstrow[value] + ";" + lastrow[value]
    return temprow

def extendHostnameAndInstanceId(row, columnIndex):
    hostnameIndex = columnIndex.get("hostname", None)
    if hostnameIndex is None:
        raise Exception('key hostname not exists in column index')
    usedCPUIndex = columnIndex.get("usedCPU", None)
    if usedCPUIndex is None:
        raise Exception('key usedCPU not exists in column index')
    instanceIdIndex = columnIndex.get("instanceId", None)
    row[hostnameIndex] = row[hostnameIndex] + "/" + row[usedCPUIndex]
    return row


def MergeJobDetailByJobId(columnIndex, rows):
    temprows = copy.deepcopy(rows)
    retRows = []
    JobIdList = []
    JobDetailDict = {}
    for row in temprows:
        jobId = getValueByColumnIndex(row, 'jobId', columnIndex)
        if jobId in JobIdList:
            temprow = JobDetailDict[jobId]
            JobDetailDict[jobId] = MergeTwoRow(temprow, row, columnIndex)

        else:
            JobDetailDict[jobId] = row
            JobIdList.append(jobId)
    retRows = JobDetailDict.values()

    return retRows

def analysisJobQueue(Rows, columnIndex, total_fee):
    queue_dict = {}
    new_Rows = []

    i = 1
    for row in Rows:
        queue = getValueByColumnIndex(row, 'queue', columnIndex)
        if queue not in queue_dict:
            queue_dict[queue] = i
            i = i + 1
    temp_row = [''] * (len(queue_dict) + 1)
    temp_row[0] = 'JobId'
    instance_row = [0] * (len(queue_dict) + 1)
    instance_row[0] = 'instance fee'
    for key, value in queue_dict.items():
        temp_row[value] = key + 'fee'
        instance_row[value] = total_fee[key]
    new_Rows.append(temp_row)

    total_row = [0] * (len(queue_dict) + 1)
    total_row[0] = u'任务总计花费'
    for row in Rows:
        temp_row = [0] * (len(queue_dict) + 1)
        queue = getValueByColumnIndex(row, 'queue', columnIndex)
        amount_sum = float(getValueByColumnIndex(row, 'PreTaxAmount', columnIndex))
        temp_row[0] = getValueByColumnIndex(row, 'jobId', columnIndex)
        temp_row[queue_dict[queue]] = amount_sum
        total_row[queue_dict[queue]] += amount_sum
        new_Rows.append(temp_row)

    new_Rows.append(total_row)
    new_Rows.append(instance_row)
    return new_Rows

def MergeJobDetailByUser(columnIndex, rows):
    temprows = copy.deepcopy(rows)
    retRows = []
    UserList = []
    JobDetailDict = {}
    for row in temprows:
        User = getValueByColumnIndex(row, 'user', columnIndex)
        if User in UserList:
            temprow = JobDetailDict[User]
            JobDetailDict[User] = MergeTwoRow(temprow, row, columnIndex)
        else:
            JobDetailDict[User] = row
            UserList.append(User)
    retRows = JobDetailDict.values()
    return retRows

def MergeJobDetailByQueue(columnIndex, rows):
    temprows = copy.deepcopy(rows)
    retRows = []
    QueueList = []
    JobDetailDict = {}
    for row in temprows:
        Queue = getValueByColumnIndex(row, 'queue', columnIndex)
        if Queue in QueueList:
            temprow = JobDetailDict[Queue]
            JobDetailDict[Queue] = MergeTwoRow(temprow, row, columnIndex)

        else:
            JobDetailDict[Queue] = row
            QueueList.append(Queue)
    retRows = JobDetailDict.values()
    return retRows

def writeJobDetail(outputFileName, columnIndex, rows):
    '''Write job detail to file with billing amount'''
    if os.path.exists(outputFileName):
        if ORVERWRITE_FILE:
            logging.warning('output file already exists, overwrite %s' % outputFileName)
        else:
            raise Exception('output file already exists %s' % outputFileName)
    with open(outputFileName, 'w', newline="", encoding="utf-8") as fd:  
        writer = csv.writer(fd)
        logging.info('Start writing output %s' % outputFileName)

        header = buildHeaderFromColumnIndex(columnIndex)
        writer.writerow(header)

        writer.writerows(rows)
        logging.info('Finish writing output %s, %d rows' % (outputFileName, len(rows)))

def testFileExt(inputFileName, ext):
    '''used to split the path name into a pair root and ext. '''
    _, fileExt = os.path.splitext(inputFileName)
    return fileExt.lower() == ext

def extractFile(inputFileName):
    '''Test if CSV/ZIP file, extract ZIP file if needed, and return file list'''
    isZip = False
    if testFileExt(inputFileName, '.csv'):
        return isZip, [inputFileName]
    elif testFileExt(inputFileName, '.zip'):
        extractDir = tempfile.mkdtemp(dir=TMP_FILE_PATH)
        with zipfile.ZipFile(inputFileName, 'r') as fd:
            fd.extractall(extractDir)
        files = os.listdir(extractDir)
        fullFilePaths = [os.path.join(extractDir, f) for f in files]
        isZip = True
        return isZip, fullFilePaths
    else:
        raise Exception('unknown file type, accept csv/zip: %s' % inputFileName)

def deleteFiles(files):
    for f in files:
        os.remove(f)
    if len(files) > 0:
        os.rmdir(os.path.dirname(files[0]))

def MergeDict(dict_a, dict_b):
    for key, value in dict_b.items():
        if key in dict_a:
            dict_a[key] += value
        else:
            dict_a[key] = value

def drawJobIdBar(DetailDict):
    sns.set_palette(sns.color_palette('bright'))
    plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
    plt.rcParams['axes.unicode_minus'] = False
    df = pd.DataFrame(DetailDict).T[['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']]
    if len(DetailDict) <=2:
        df.plot(kind = 'bar', stacked = True, width = 0.1, alpha = 0.5)
    else:
        df.plot(kind = 'bar', stacked = True, alpha = 0.5)
    plt.title('各个Job花费概览')
    plt.xticks(fontsize=10, rotation=0)  # 设置x和y轴刻度值的字体大小;rotation规定水平排列刻度文字。
    plt.yticks(fontsize=10)
    plt.legend(fontsize=10)
    plt.savefig('./JobBar.svg', format='svg', dpi=500)
    plt.clf()
    plt.close()

def drawUserBar(DetailDict):
    sns.set_palette(sns.color_palette('bright'))
    plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
    plt.rcParams['axes.unicode_minus'] = False
    df = pd.DataFrame(DetailDict).T[['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']]
    df.plot(kind = 'bar', stacked = True, alpha = 0.5)
    plt.title('各个用户花费概览')
    plt.xticks(fontsize=10, rotation=0)  # 设置x和y轴刻度值的字体大小;rotation规定水平排列刻度文字。
    plt.yticks(fontsize=10)
    plt.legend(fontsize=10)
    plt.savefig('./UserBar.svg', format='svg', dpi=500)
    plt.clf()
    plt.close()

def drawQueueIdBar(DetailDict):
    sns.set_palette(sns.color_palette('bright'))
    plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
    plt.rcParams['axes.unicode_minus'] = False
    df = pd.DataFrame(DetailDict).T[['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']]
    if len(DetailDict) <=2:
        df.plot(kind = 'bar', stacked = True, width = 0.1, alpha = 0.5)
    else:
        df.plot(kind = 'bar', stacked = True, alpha = 0.5)
    plt.title('各个队列花费概览')
    plt.xticks(fontsize=10, rotation=0)  # 设置x和y轴刻度值的字体大小;rotation规定水平排列刻度文字。
    plt.yticks(fontsize=10)
    plt.legend(fontsize=10, loc = 'upper right')
    plt.savefig('./QueueBar.svg', format='svg', dpi=300)
    plt.clf()
    plt.close()

def drawPie(DetailDict):
    sns.set_palette(sns.color_palette('bright'))
    plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
    plt.rcParams['axes.unicode_minus'] = False
    ItemList = ['Cloudserverconfiguration', 'OsDisk']
    BillTypeList = ['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']
    Itemlabel = []
    Itemvalue = []
    BillTypelabel =[]
    BillTypevalue = []
    for key, value in DetailDict.items():
        if key in ItemList and value > 0:
            Itemlabel.append(key)
            Itemvalue.append(value)
            continue
        if key in BillTypeList and value > 0:
            BillTypelabel.append(key)
            BillTypevalue.append(value)
    plt.figure(figsize=(6, 6))
    plt.pie(Itemvalue, labels=Itemlabel,autopct='%.1f%%')
    '''
    ItemCost ={}
    for item in ItemList:
        if item in DetailDict:
            if DetailDict[item] != 0.0:
                ItemCost[item] = DetailDict[item]
    ItemDF = pd.Series(ItemCost)
    ItemDF.plot(kind = 'pie', autopct = '%.1f%%', title = '产品花费分布', figsize = (6,6))
    '''
    plt.legend()
    plt.savefig('./ItemPie.svg', format = 'svg', dpi = 500)
    plt.clf()

    plt.pie(BillTypevalue, labels=BillTypelabel,autopct='%.1f%%')
    plt.legend()
    plt.savefig('./BillTypePie.svg', format = 'svg', dpi = 500)
    plt.clf()
    plt.close()

def drawSpot(spotDict, filename):
    sns.set_palette(sns.color_palette('bright'))
    plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', 500)
    for key, value in spotDict.items():
        titlename = '抢占式实例 ' + key + ' 价格趋势'
        new_index = []
        df = pd.DataFrame(value)
        for l in list(df.index):
            tem = ((l[0] + l[1]) // 2)
            temp = time.strftime('%m-%d %H:%M', time.localtime(int(tem)))
            new_index.append(temp)
        df.index = new_index
        df.sort_index().plot(kind = 'line')
        plt.xticks(rotation=30, fontsize=7)
        plt.title(titlename)
        plt.savefig(filename, format = 'svg', dpi = 700)

def draw(_JobCostDetail, _UserCostDetail, _QueueCostDetail, _DetailCostDict, detail_file):
    plt.figure(figsize=(12, 8))
    sns.set_palette(sns.color_palette('bright'))
    plt.rcParams['font.sans-serif'] = 'Microsoft YaHei'
    plt.rcParams['axes.unicode_minus'] = False
    gs = gridspec.GridSpec(2, 2, wspace = 0.3, hspace= 0.45)
    ax0 = plt.subplot(gs[0, :])

    df = pd.DataFrame(_JobCostDetail).T[['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']]
    color = ['lightskyblue', 'red', 'lime']
    labels = ['包年包月', '按量付费', '抢占式实例']
    length = len(_JobCostDetail)
    if length <= 3:
        df.plot(kind='bar', stacked=True, width=0.1, alpha=0.5, ax = ax0, legend = False)
    else:
        df.plot(kind='bar', stacked=True, alpha=0.5, ax = ax0, legend = False, color = color)
    for k, v1, v2, v3, in zip(range(length), list(df['MonthlyOrAnnualPayment']), list(df['PayByQuantity']), list(df['Spot'])):
        if v1 > 0.1:
            plt.text(k, v1 - 0.1, '%.1f' % v1, ha='center', va='bottom', fontsize=8)
        if v2 > 0.1:
            plt.text(k, v1 + v2 + 0.05, '%.1f' % (v1+v2), ha='center', va='bottom', fontsize=8)
        if v3 > 0.1:
            plt.text(k, v1 + v2 + v3 + 0.05, '%.1f' % (v1+v2+v3), ha='center', va='bottom', fontsize=8)
    plt.title('任务花费概览')

    patches = [mpatches.Patch(color=color[i], label="{:s}".format(labels[i])) for i in range(len(color))]
    ax11 = plt.gca()
    box = ax11.get_position()
    ax11.set_position([box.x0, box.y0, box.width, box.height * 0.8])
    ax11.legend(handles=patches, bbox_to_anchor=(1.00, 1.22), ncol=3)
    plt.xlabel('jobId')
    plt.ylabel('详细花费/元')
    plt.xticks(fontsize=10, rotation=0)  # 设置x和y轴刻度值的字体大小;rotation规定水平排列刻度文字。
    plt.yticks(fontsize=10)


    ax2 = plt.subplot(gs[1, 0])
    df = pd.DataFrame(_UserCostDetail).T[['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']]
    length = len(_UserCostDetail)
    if length <= 3:
        df.plot.bar(stacked = True, width = 0.1, alpha = 0.5, ax = ax2, color = color, legend = False)
    else:
        df.plot(kind = 'bar', stacked = True, alpha = 0.5, ax = ax2)
    for k, v1, v2, v3, in zip(range(length), list(df['MonthlyOrAnnualPayment']), list(df['PayByQuantity']), list(df['Spot'])):
        if v1 > 0.1:
            plt.text(k, v1 - 0.1, '%.1f' % v1, ha='center', va='bottom', fontsize=8)
        if v2 > 0.1:
            plt.text(k, v1 + v2 + 0.05, '%.1f' % (v1+v2), ha='center', va='bottom', fontsize=8)
        if v3 > 0.1:
            plt.text(k, v1 + v2 + v3 + 0.05, '%.1f' % (v1+v2+v3), ha='center', va='bottom', fontsize=8)
    plt.title('用户花费概览')
    plt.xlabel('user')
    plt.ylabel('详细花费/元')
    plt.xticks(fontsize=10, rotation=0)  # 设置x和y轴刻度值的字体大小;rotation规定水平排列刻度文字。
    plt.yticks(fontsize=10)


    ax4 = plt.subplot(gs[1, 1])
    df = pd.DataFrame(_QueueCostDetail).T[['MonthlyOrAnnualPayment', 'PayByQuantity', 'Spot']]
    if len(_QueueCostDetail) <=3:
        df.plot(kind = 'bar', stacked = True, width = 0.1, align = "center", alpha = 0.5, ax = ax4, legend = False, color = color)
    else:
        df.plot(kind = 'bar', stacked = True, alpha = 0.5, ax = ax4)
    for k, v1, v2, v3, in zip(range(length), list(df['MonthlyOrAnnualPayment']), list(df['PayByQuantity']), list(df['Spot'])):
        if v1 > 0.1:
            plt.text(k, v1 - 0.1, '%.1f' % v1, ha='center', va='bottom', fontsize=8)
        if v2 > 0.1:
            plt.text(k, v1 + v2 + 0.05, '%.1f' % (v1+v2), ha='center', va='bottom', fontsize=8)
        if v3 > 0.1:
            plt.text(k, v1 + v2 + v3 + 0.05, '%.1f' % (v1+v2+v3), ha='center', va='bottom', fontsize=8)
    plt.title('队列花费概览')
    plt.xticks(fontsize=10, rotation=0)  # 设置x和y轴刻度值的字体大小;rotation规定水平排列刻度文字。
    plt.yticks(fontsize=10)
    plt.xlabel('queue')
    plt.ylabel('详细花费/元')
    plt.savefig(detail_file, format = 'svg', dpi = 700)

def analysisCost(columnIndex, Rows, arg):
    '''ResModeCostDetail: 消费类型 : ResModeCostDetail[0]: 包年包月; ResModeCostDetail[1]: 按量付费; ResModeCostDetail[2]: 抢占式实例'''
    '''BillItemCostDetail: 计费项: BillItemCostDetail[0]: 云服务器配置; BillItemCostDetail[1]: 系统盘'''
    JobCostDetail = {}
    UserCostDetail = {}
    QueueCostDetail = {}
    DetailCostDict = {'MonthlyOrAnnualPayment': 0.0, 'PayByQuantity': 0.0, 'Spot': 0.0, 'Cloudserverconfiguration': 0.0,
                      'OsDisk': 0.0}
    for row in Rows:
        tempDetailCostDict = {'MonthlyOrAnnualPayment': 0.0, 'PayByQuantity': 0.0, 'Spot': 0.0, 'Cloudserverconfiguration': 0.0, 'OsDisk': 0.0}
        amount_sum = float(getValueByColumnIndex(row, 'PreTaxAmount', columnIndex))

        instanceType = getValueByColumnIndex(row, 'InstanceType', columnIndex)
        billitem = getValueByColumnIndex(row, 'ItemType', columnIndex)

        tempDetailCostDict[instanceType] = amount_sum
        tempDetailCostDict[billitem] = amount_sum

        MergeDict(DetailCostDict, tempDetailCostDict)

        jobId = getValueByColumnIndex(row, 'jobId', columnIndex)
        user = getValueByColumnIndex(row, 'user', columnIndex)
        queue = getValueByColumnIndex(row, 'queue', columnIndex)

        if jobId in JobCostDetail:
            MergeDict(JobCostDetail[jobId], tempDetailCostDict)
        else:
            JobCostDetail[jobId] = copy.deepcopy(tempDetailCostDict)

        if user in UserCostDetail:
            MergeDict(UserCostDetail[user], tempDetailCostDict)
        else:
            UserCostDetail[user] = copy.deepcopy(tempDetailCostDict)

        if queue in QueueCostDetail:
            MergeDict(QueueCostDetail[queue], tempDetailCostDict)
        else:
            QueueCostDetail[queue] = copy.deepcopy(tempDetailCostDict)

    draw(JobCostDetail, UserCostDetail, QueueCostDetail, DetailCostDict, arg['detail_FileName'])

def strTimeRows(Rows, columnIndex):
    startIndex = columnIndex.get('startTime', None)
    endIndex = columnIndex.get('endTime', None)
    for row in Rows:
        startTime = row[startIndex]
        endTime = row[endIndex]
        row[startIndex] = time.strftime(TIME_FORMAT, time.localtime(int(startTime)))
        row[endIndex] = time.strftime(TIME_FORMAT, time.localtime(int(endTime)))

def main(arg):
    billingDetailFiles = []
    billingDetailFileList = arg['billingDetailFileList']
    jobDetailFileList = arg['jobDetailFileList']

    billingDetailDict = {}
    billingColumnIndex = {}
    for billingDetailFileName in billingDetailFileList:
        try:
            isZip = False
            isZip, billingDetailFiles = extractFile(billingDetailFileName)
            for f in billingDetailFiles:
                '''load billing file'''
                billingColumnIndex, billingDetailDict = loadBillingDetailToDict(f, billingDetailDict)
        except Exception as err:
            logging.error(err.message)
        finally:
            if isZip and CLEAN_TMP_FILE:
                deleteFiles(billingDetailFiles)

    try:
        outColumnIndex, outRows, detailColumnIndex, detailRows , spotDict, total_fee = loadJobDetailAndMergeBillingDetail(jobDetailFileList, billingColumnIndex, billingDetailDict, arg['filter_starttime'], arg['filter_endtime'])

        for row in outRows:
            extendHostnameAndInstanceId(row, outColumnIndex)

    except Exception as err:
        logging.error(err.message)

    return outRows, outColumnIndex, detailRows, detailColumnIndex, spotDict, total_fee

def parseArg(config_file):
    arg = {}
    config = configparser.ConfigParser()
    config.read(config_file, encoding='UTF-8')

    ehpc_file = config.get("DEFALUT", "ehpc_file", fallback="")
    if ehpc_file == "":
        logging.error("ephc_file is null")
        sys.exit(1)
    jobDetailFileName = ehpc_file.strip().split()
    jobDetailFileList = []
    for file in jobDetailFileName:
        if not os.path.exists(file):
            logging.warning("file %s is not exist, skip" % file)
            continue
        if os.path.isdir(file):
            for f in os.listdir(file):
                jobDetailFileList.append(f)
        else:
            jobDetailFileList.append(file)
    if len(jobDetailFileList) == 0:
        logging.error("no file")
        sys.exit(1)
    arg['jobDetailFileList'] = jobDetailFileList

    consume_detail_file = config.get("DEFALUT", "consume_detail_file", fallback="")
    if consume_detail_file == "":
        logging.error("consume_detail_file is null")
        sys.exit(1)
    billingDetailFileName = consume_detail_file.strip().split()
    billingDetailFileList = []
    for file in billingDetailFileName:
        if not os.path.exists(file):
            logging.warning("file %s is not exist, skip" % file)
            continue
        if os.path.isdir(file):
            for f in os.listdir(file):
                billingDetailFileList.append(f)
        else:
            billingDetailFileList.append(file)
    if len(billingDetailFileList) == 0:
        logging.error("no file")
        sys.exit(1)
    arg['billingDetailFileList'] = billingDetailFileList
    output_path = config.get("DEFALUT", "output_path")
    if output_path == '':
        output_path = os.path.dirname(__file__)
    if not os.path.isdir(output_path):
        logging.error("path is not a dir" % output_path)
        sys.exit(1)
    timestamp = int(time.time())
    job_file = config.get("DEFALUT", "job_file")
    if job_file == '':
        job_file = 'job-'+str(timestamp)+'.csv'
    jobFileName = os.path.join(output_path, job_file)
    arg['jobFileName'] = jobFileName
    user_file = config.get("DEFALUT", "user_file")
    if user_file == '':
        user_file = 'user-'+str(timestamp)+'.csv'
    userFileName = os.path.join(output_path, user_file)
    arg['userFileName'] = userFileName
    queue_file = config.get("DEFALUT", "queue_file")
    if queue_file == '':
        queue_file = 'queue-'+str(timestamp)+'.csv'
    queueFileName = os.path.join(output_path, queue_file)
    arg['queueFileName'] = queueFileName

    job_queue_file = config.get("DEFALUT", "job_queue_file")
    if job_queue_file == '':
        job_queue_file = 'job-queue-'+str(timestamp)+'.csv'
    job_queue_FileName = os.path.join(output_path, job_queue_file)
    arg['job_queue_FileName'] = job_queue_FileName

    detail_file = config.get("DEFALUT", "detail_file")
    if detail_file == '':
        detail_file = 'detail-file'+str(timestamp)+'.svg'
    detail_FileName = os.path.join(output_path, detail_file)
    arg['detail_FileName'] = detail_FileName

    spot_file = config.get("DEFALUT", "spot_file")
    if spot_file == '':
        spot_file = 'spot_file'+str(timestamp)+'.svg'
    spot_FileName = os.path.join(output_path, spot_file)
    arg['spot_FileName'] = spot_FileName

    summaryType = config.get("DEFALUT", "summaryType")
    if summaryType not in ['0', '1', '2', '3', '4']:
        logging.error("summaryType is not support")
        sys.exit(1)
    arg['summaryType'] = summaryType

    filter_starttime = config.get("DEFALUT", "starttime")
    filter_endtime = config.get("DEFALUT", "endtime")
    if filter_endtime == '' and filter_starttime =='':
        filter_starttime = None
        filter_endtime = None
    elif filter_starttime != '' and filter_endtime != '':
        filter_starttime = parseLocalTimeToTimestamp(filter_starttime)
        filter_endtime = parseLocalTimeToTimestamp(filter_endtime)
    else:
        logging.error("filter time is error")
        sys.exit(1)
    arg['filter_starttime'] = filter_starttime
    arg['filter_endtime'] = filter_endtime

    arg['analy'] = config.getboolean("DEFALUT", "analy", fallback=True)
    arg['spot'] = config.getboolean("DEFALUT", "spot", fallback= True)

    return arg

def summaryByJob(Rows, columnindex, summaryByJobIdFileName, total_fee, job_queue_file):
    JobIdMergeRows = MergeJobDetailByJobId(columnindex, Rows)
    strTimeRows(JobIdMergeRows, columnindex)
    JobcolumnList = ['jobId',
                    'taskId',
                    'jobName',
                    'user',
                    'queue',
                    'hostname',
                    'usedCPU',
                    'usedMem',
                    'startTime',
                    'endTime',
                    'instanceCores',
                    'CoreHours',
                    'PreTaxGrossAmount',
                    'PreTaxAmount']
    Jobcolumnindex = {}
    for i, field in enumerate(JobcolumnList):
        Jobcolumnindex[field] = i

    newRows = []
    for row in JobIdMergeRows:
        newrow = [None] * len(Jobcolumnindex)
        for key, value in Jobcolumnindex.items():
            if key == 'tasknum':
                temp = set(row[columnindex['taskId']].split(';'))
                length = len(temp)
                newrow[value] = length
            else:
                newrow[value] = row[columnindex[key]]
        newRows.append(newrow)

    job_queue_detail = analysisJobQueue(JobIdMergeRows, columnindex, total_fee)
    with open(job_queue_file, 'w', newline="", encoding="utf-8-sig") as fd:  #python3
        writer = csv.writer(fd)
        logging.info('Start writing output %s' % job_queue_file)

        writer.writerows(job_queue_detail)
        logging.info('Finish writing output %s, %d rows' % (job_queue_file, len(job_queue_detail)))


    writeJobDetail(summaryByJobIdFileName, Jobcolumnindex, newRows)

def summaryByUser(Rows, columnindex, summaryByUserFileName):
    UserMergeRows = MergeJobDetailByUser(columnindex, Rows)
    UsercolumnList = ['user',
                    'jobnum',
                    'queuenum',
                    'usedCPU',
                    'usedMem',
                    'instanceCores',
                    'PreTaxGrossAmount',
                    'PreTaxAmount',
                    'CoreHours'
                    ]
    Usercolumnindex = {}
    for i, field in enumerate(UsercolumnList):
        Usercolumnindex[field] = i

    newRows = []
    for row in UserMergeRows:
        newrow = [None] * len(Usercolumnindex)
        for key, value in Usercolumnindex.items():
            if key == 'jobnum':
                temp = set(row[columnindex['jobId']].split(';'))

                length = len(temp)
                newrow[value] = length
            elif key == 'queuenum':
                temp = set(row[columnindex['queue']].split(';'))

                length = len(temp)
                newrow[value] = length
            elif key == 'tasknum':
                temp = set(row[columnindex['taskId']].split(';'))

                length = len(temp)
                newrow[value] = length
            else:
                newrow[value] = row[columnindex[key]]


        newRows.append(newrow)
    writeJobDetail(summaryByUserFileName, Usercolumnindex, newRows)

def summaryByQueue(Rows, columnindex, summaryByQueueFileName):
    QueueMergeRows = MergeJobDetailByQueue(columnindex, Rows)
    QueuecolumnList = ['queue',
                        'usedCPU',
                        'usedMem',
                        'instanceCores',
                        'PreTaxGrossAmount',
                        'PreTaxAmount',
                        'jobnum',
                        'usernum',
                        'CoreHours'
                        ]
    Queuecolumnindex ={}
    for i, field in enumerate(QueuecolumnList):
        Queuecolumnindex[field] = i
    newRows = []
    for row in QueueMergeRows:
        newrow = [None] * len(Queuecolumnindex)
        for key, value in Queuecolumnindex.items():
            if key == 'jobnum':
                temp = set(row[columnindex['jobId']].split(';'))

                length = len(temp)
                newrow[value] = length
            elif key == 'usernum':
                temp = set(row[columnindex['user']].split(';'))

                length = len(temp)
                newrow[value] = length
            elif key == 'tasknum':
                temp = set(row[columnindex['taskId']].split(';'))

                length = len(
                    temp)
                newrow[value] = length
            else:
                newrow[value] = row[columnindex[key]]
        newRows.append(newrow)
    writeJobDetail(summaryByQueueFileName, Queuecolumnindex, newRows)

def summary(Rows, columnindex, arg, total_fee):
    summaryType = arg['summaryType']
    summaryByJobIdFileName = arg['jobFileName']
    summaryByUserFileName = arg['userFileName']
    summaryByQueueFileName = arg['queueFileName']
    if summaryType == '4':
        logging.info('Summary By JobId and User and Queue')
        summaryByJob(Rows, columnindex, summaryByJobIdFileName, total_fee, arg['job_queue_FileName'])
        summaryByUser(Rows, columnindex, summaryByUserFileName)
        summaryByQueue(Rows, columnindex, summaryByQueueFileName)
    elif summaryType == '1':
        logging.info('Summary By JobId')
        summaryByJob(Rows, columnindex, summaryByJobIdFileName, total_fee, arg['job_queue_FileName'])
    elif summaryType == '2':
        logging.info('Summary By User')
        summaryByUser(Rows, columnindex, summaryByUserFileName)
    elif summaryType == '3':
        logging.info('Summary By Queue')
        summaryByQueue(Rows, columnindex, summaryByQueueFileName)

if __name__ == "__main__":
    config_file = sys.argv[1]
    arg = parseArg(config_file)
    values = main(arg)
    summary(values[0], values[1], arg, values[5])
    if arg['analy']:
        analysisCost(values[3], values[2], arg)
    if arg['spot']:
        drawSpot(values[4], arg['spot_FileName'])