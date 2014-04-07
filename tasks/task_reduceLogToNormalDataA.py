# -*- coding: utf-8 -*-
"""
Сохраняет совпавшие данные в отдельный файл
"""

class DataSaver:

    def setFileName(self, fileName):
        self.fileName = fileName

    def sendData(self, data):
        with open(self.fileName, "a") as myfile:
            for i in data:
                print >>myfile, i
        return

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
    if object is None:
        return None

    if object['abtype'] == 'B':
        return object
    else:
        return None

fileName = 'gt-custom_log.20140406'
saver = DataSaver()
saver.setFileName('logs/reduced.' + fileName + '.A')

aggregator.setSender( saver )
aggregator.maxBufferSize = 1000

task1 = Task(getAB_Value)

task1.attachFilter( TaskFilter(filterByTypeA) )
task1.attachFilter( TaskFilter(filterByMainPage) )

attachedTasks.append(task1)

aggregator.addTask(
    TaskResult(
        funcMap=aggrResultTaskMap,
        #funcReduce=aggrResultTaskReduce
    )
)