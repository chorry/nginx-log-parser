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

sender = FileSender()
sender.setFileName('0203_B.log')
sender.setCompareFile('processedLogs/mergedU.20140403.B')
sender.setAbKey('B')


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