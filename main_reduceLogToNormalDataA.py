# -*- coding: utf-8 -*-
from __future__ import division
import multiprocessing
import os

from Sender.TaskSender import TaskSender
from Task import Task, TaskResult, TaskFilter
from aggregator import Aggregator
import sys
import time
from pprint import pprint
import signal

from logParser import LogParser


""" Processes all attached tasks to parsed log object, if applicable
"""


def processLogTasks(object):
    result = []
    for task in getAttachedTaskLists():
        taskResult = task.process(object)
        if taskResult.hasResult():
            result.append(taskResult)

    if len(result) > 0:
        return result
    else:
        return None


def getAttachedTaskLists():
    try:
        return attachedTasks
    except NameError:
        return ()


attachedTasks = []
aggregator = Aggregator()



def signal_handler(signal, frame):
        print('Aborted by user.')
        aggregator.flushBuffer()
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


swapFileName = 'worker.swap'
defaultConfig = {
    'offset': 0,
    'linenum': 0,
    'path': None
}
verboseMode = False

defaultNginxLogFormat = ('$remote_addr $host $remote_user [$time_local] $request '
                         '"$status" $body_bytes_sent "$http_referer" '
                         '"$http_user_agent" "$http_x_forwarded_for" '
                         'upstream{$upstream_addr|$upstream_response_time|$upstream_status} $abCookieValue')

#load tasks
execfile('tasks/task_reduceLogToNormalDataA.py')



class ServerTask:
    def __init__(self):
        self.procAmount = max(1, multiprocessing.cpu_count() - 1)
        self.fileList = None

    def setJob(self, job):
        self.job = job

    def getJob(self):
        return self.job

    def setDataSource(self, source):
        self.dataSource = source
        pass

    def getPData(self):
        return self.dataSource.parseFile( self.fileList )

    def setAggregator(self, object):
        self.aggregator = object

    def getAggregator(self):
        return self.aggregator

    def run(self):
        obj = 0
        for processedObj in self.getPData():
            if processedObj is not None:
                r = processLogTasks(processedObj)
                if r is not None:
                    obj += 1
                    self.getAggregator().aggregate(r)

        self.getAggregator().flushBuffer()

if __name__ == '__main__':
    #restore settings
    configFileName = os.path.basename(__file__) + ".config.json"
    #fileName = 'gt-custom_log.20140404' already defined at [task_reduce]

    nginxLogFormat = defaultNginxLogFormat = ('$remote_addr $host $remote_user [$time_local] $request '
                         '"$status" $body_bytes_sent "$http_referer" '
                         '"$http_user_agent" "$http_x_forwarded_for" '
                         'upstream{$upstream_addr|$upstream_response_time|$upstream_status} abtype userid')
    config = {
        'path'       : 'logs',
        'currentFile': 'logs/' + fileName,
        'resumeOnFile': True,
    }

    for arg in sys.argv:
        params = arg.split("=")

        if len(params) == 2:
            if params[0] == '-path':
                config['path'] = params[1]
            if params[1] == '-nginx':
                config['nginxLogFormat'] = params[1]
        else:
            if params[0] == '-verbose':
                config['verboseMode'] = True

    serverTask = ServerTask()
    serverTask.fileList = [ config['currentFile'] ]

    lParser = LogParser(configFileName=configFileName, configParams=config)
    lParser.updateLogParams( { 'userid': '(?P<userid>uid=[A-Z0-9]+)', 'abtype': '(?P<abtype>[A|B|-])' } )
    lParser.setLogPattern(nginxLogFormat)

    print lParser

    serverTask.setDataSource(lParser)
    serverTask.setAggregator(aggregator)
    serverTask.run()


print "the end"





class ServerBaseJob:
    def __init__(self):
        pass

    def execute(self):
        result = []
        for task in getAttachedTaskLists():
            taskResult = task.process(object)
            if taskResult.hasResult():
                result.append(taskResult)

        if len(result) > 0:
            return result
        else:
            return None