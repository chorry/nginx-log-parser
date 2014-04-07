# -*- coding: utf-8 -*-
"""
Сохраняет совпавшие данные в отдельный файл
"""
def aggrResultTaskMap(self, object):
    return object.result


def getAB_Value(self, logObject):
    r = TaskResult()
    if logObject != None:
        r.setType('uid')
        r.setResult(
            logObject['abtype'] + " " + logObject['userid']
        )
    return r

def filterByTypeA(self, object):
    #print object
    if object is None:
        return None

    if object['abtype'] == 'A':
        return object
    else:
        return None

def filterByMainPage(self, object):

    if object is None:
        return None
    if object['request_request_uri'] == '/' or object['request_request_uri'] == '/?/' or object['request_request_uri'] =='/?':
        return object
    else:
        return None

def filterByTypeB(self, object):
    if object['abtype'] == 'B':
        return object
    else:
        return None

compareLogDateFrom = '20140405'
compareLogDateTo   = '20140406'
abKey              = 'B'

sender = FileSender()
sender.setFileName(compareLogDateFrom + '-' + compareLogDateTo + abKey + '.log')
sender.setCompareFile('processedLogs/merged.custom_log.' + compareLogDateTo + '.' + abKey)
sender.setAbKey(abKey)

aggregator.setSender( sender )
aggregator.maxBufferSize = 1000

task1 = Task(getAB_Value)

task1.attachFilter( TaskFilter(filterByTypeB) )

attachedTasks.append(task1)

aggregator.addTask(
    TaskResult(
        funcMap=aggrResultTaskMap,
        #funcReduce=aggrResultTaskReduce
    )
)