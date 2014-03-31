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


def dict_sub(text, d=res.LOG_PARAMS):
    """ Replace in 'text' non-overlapping occurences of REs whose patterns are keys
    in dictionary 'd' by corresponding values (which must be constant strings: may
    have named backreferences but not numeric ones). The keys must not contain
    anonymous matching-groups.
    Returns the new string.
    Thanks to Alex Martelli @
    http://stackoverflow.com/questions/937697/can-you-pass-a-dictionary-when-replacing-strings-in-python
    """
    if d != res.LOG_SPECIAL_SYMBOLS:
        text = dict_sub(text, res.LOG_SPECIAL_SYMBOLS)


    # Create a regular expression  from the dictionary keys
    regex = re.compile("|".join("(%s)" % k for k in d))
    # Facilitate lookup from group number to value
    lookup = dict((i + 1, v) for i, v in enumerate(d.itervalues()))
    # For each match, find which group matched and expand its value
    result = regex.sub(lambda mo: mo.expand(lookup[mo.lastindex]), text)

    return result


def parseLogLine(compiledReObject, text, displayResult=True):
    result = compiledReObject.finditer(text)
    group_name_by_index = dict([(v, k) for k, v in compiledReObject.groupindex.items()])

    resultObj = {}
    iteratorSize = 0
    for match in result:
        iteratorSize += 1
        for group_index, group in enumerate(match.groups()):
            if group:
                if displayResult:
                    print "%s : %s" % (group_name_by_index[group_index + 1], group)
                resultObj[group_name_by_index[group_index + 1]] = group

    if iteratorSize == 0:
        if displayResult:
            print ("Could not parse [ %s ]" % text)
        return None

    #TODO: do something with this date
    resultObj['t_date_local'] = resultObj['time_local'][:11]
    resultObj['t_time_local'] = resultObj['time_local'][12:20]
    if resultObj['upstream_response_time'] != '-':
        if resultObj['upstream_response_time'].find(':') != -1:
            splitKey = ':'
        else:
            splitKey = ','
        resultObj['upstreams'] = resultObj['upstream_response_time'].split(splitKey)

    return resultObj


def tryUpstreamsRespAvg(v, default=0):
    try:
        ret = float(v)
    except ValueError:
        ret = float(0)

    return ret



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
    pass


def isCachedQuery(self, logObject):
    cacheKey = 'missed' if (logObject['upstream_addr'] != '-') else 'hit'

    r = TaskResult()
    r.setType('cachedQuery')
    r.setResult(
        0 if (logObject['upstream_addr'] != '-') else 1
    )
    r.setTime(logObject['t_time_local'][:8])

    return r


def getUpstreamAvgResp(self, logObject):
    upstreamRespAvg = 0
    r = TaskResult()

    if 'upstreams' in logObject:
        upstreamRespAvg = \
            reduce(
                lambda res, v:
                tryUpstreamsRespAvg(v) + res,
                logObject['upstreams'], 0
            ) / len(logObject['upstreams'])

    r.setType('upstreamAvg')
    r.setResult(upstreamRespAvg)
    r.setTime(logObject['t_time_local'][:8])
    return r


cqLogTask = Task(isCachedQuery)
attachedTasks.append(cqLogTask)

upAvgLogTask = Task(getUpstreamAvgResp)
attachedTasks.append(upAvgLogTask)

aggregator = Aggregator()

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



    #non-blocking trick
    """
    configFile = open(configFileName, "r+")
    fcntl.lockf(configFile, fcntl.LOCK_EX)
    """

    hitsPerTimeInterval = Counter()
    cachedQuery = Counter()
    upstreamResp = Counter()
    upstreamList = []

    lParser = LogParser(configFileName=configFileName, configParams= config)
    lParser.setRePattern(dict_sub(nginxLogFormat))

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

