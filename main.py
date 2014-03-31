# -*- coding: utf-8 -*-
from collections import Counter
from Task import Task, TaskResult
from aggregator import Aggregator
import re
import time
import json
import fcntl
import sys
import numpy
import os
from pprint import pprint
import ctypes

import datetime, bisect, collections
from logParser import LogParser

import res


""" Processes all attached tasks to parsed log object, if applicable
"""


def processLogTasks(object):
    result = []
    for task in getAttachedTaskLists():
        taskResult = task.process(object)

        if taskResult is not None:
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

'''
----------- TASKS -----------
'''


def aggrResultTask(self, object):
    print "----"
    print (object)
    print "^^^^^"

def getAB_Value(self, logObject):
    r = TaskResult()
    r.setType('ab')
    r.setResult(logObject['ab_cookie_value'])
    return r


attachedTasks.append( Task(getAB_Value) )

aggregator = Aggregator()

aggregator.addTask( TaskResult(aggrResultTask) )


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
if __name__ == '__main__':
    #restore settings
    configFileName = "worker.config.json"
    nginxLogFormat = defaultNginxLogFormat


    config = { 'path' : 'logs/' }

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

    scriptTimeStart = time.time()

    hitsPerTimeInterval = Counter()
    cachedQuery = Counter()
    upstreamResp = Counter()
    upstreamList = []

    lParser = LogParser(configFileName=configFileName, configParams= config)
    lParser.setNginxPattern( nginxLogFormat )

    resume = False
    for processedObj in lParser.parseFile():
        if processedObj is not None:
            r = processLogTasks(processedObj)
            if r is not None:
                aggregator.aggregate(r)

    aggregator.flushBuffer()


scriptTimeEnd = time.time()
scriptTimeTotal = scriptTimeEnd - scriptTimeStart
print "Running time: %f s" % scriptTimeTotal

