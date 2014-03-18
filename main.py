# -*- coding: utf-8 -*-
from collections import Counter
import pickle
from logTask import logTask, logTaskResult
from aggregator import Aggregator
import re
import time
import sys
from pprint import pprint
import ctypes

import datetime, bisect, collections

""" These symbols in nginx_log_format may break regex, if are left unattended"""
LOG_SPECIAL_SYMBOLS = {
    '\[': '\\[',
    '\]': '\\]',
    '\"': '\\"',
    '\|': '\\|',
}

""" Recognizable nginx_log_format parameters"""
LOG_PARAMS = {
    '\$body_bytes_sent': '(?P<body_bytes_sent>\d+)',
    '\$bytes_sent': '(?P<bytes_sent>\d+)',  #число байт, переданное клиенту
    '\$connection': 'NOT_IMPLEMENTED',  #порядковый номер соединения
    '\$connection_requests': '(?P<connection_requests>\d+)',  #текущее число запросов в соединении (1.1.18)
    '\$host': '(?P<host>[A-Za-z0-9-\.]+)',
    '\$http_referer': '(?P<http_referer>[\d\D]+|)',  #referer cAN by empty
    '\$http_user_agent': '(?P<http_user_agent>[\d\D]+|)',  #agent can be empty
    #'\$http_x_forwarded_for': '(?P<http_x_forwarded_for>[\d\D.,\-\s]+)',
    '\$http_x_forwarded_for': '(?P<http_x_forwarded_for>[\d\D]+|)',
    '\$msec': '(?P<msec>\d+)',  #время в секундах с точностью до миллисекунд на момент записи в лог
    '\$pipe': '(?P<pipe>[.p])',  #“p” если запрос был pipelined, иначе “.”
    '\$remote_addr': '(?P<remote_addr>\d+.\d+.\d+.\d+)',
    '\$remote_user': '(?P<remote_user>[\D\d]+)',
    '\$request': '(?P<request_request_method>[A-Z]+) (?P<request_request_uri>[\d\D]+) (?P<request_request_http_version>HTTP/[0-9.]+)',
    '\$request_length': '(?P<request_length>\d+)',  #длина запроса (включая строку запроса, заголовок и тело запроса)
    '\$request_time': '(?P<request_time>[\d.]+)',
    #время обработки запроса в секундах с точностью до миллисекунд; время, прошедшее с момента чтения первых байт от клиента до момента записи в лог после отправки последних байт клиенту
    '\$status': '(?P<status>\d+)',  #статус ответа
    '\$time_iso8601': 'NOT_IMPLEMENTED',  #локальное время в формате по стандарту ISO 8601
    '\$time_local': '(?P<time_local>[0-3][0-9]/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-5][0-9]:[0-5][0-9] [-+0-9]+)',
    #локальное время в Common Log Format
    '\$upstream_addr': '(?P<upstream_addr>[\-A-Za-z0-9.:, ]+)',  #ip:port and unix:socket-path
    '\$upstream_response_time': '(?P<upstream_response_time>[-.\d,: ]+)',
    '\$upstream_status': '(?P<upstream_status>[\-0-9,: ]+)',
}


def dict_sub(text, d=LOG_PARAMS):
    """ Replace in 'text' non-overlapping occurences of REs whose patterns are keys
    in dictionary 'd' by corresponding values (which must be constant strings: may
    have named backreferences but not numeric ones). The keys must not contain
    anonymous matching-groups.
    Returns the new string.
    Thanks to Alex Martelli @
    http://stackoverflow.com/questions/937697/can-you-pass-a-dictionary-when-replacing-strings-in-python
    """
    if d != LOG_SPECIAL_SYMBOLS:
        text = dict_sub(text, LOG_SPECIAL_SYMBOLS)


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
        #raise Exception("Could not parse [ %s ]" % text)
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
def isCachedQuery(self, logObject):
    cacheKey = 'missed' if (logObject['upstream_addr'] != '-') else 'hit'

    r = logTaskResult()
    r.setType('cachedQuery')
    r.setResult(
        0 if (logObject['upstream_addr'] != '-') else 1
    )
    r.setTime(logObject['t_time_local'][:8])

    return r

def getUpstreamAvgResp(self, logObject):
    upstreamRespAvg = 0
    r = logTaskResult()

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


cqLogTask = logTask()
cqLogTask.attachTask(isCachedQuery)
attachedTasks.append(cqLogTask)

upAvgLogTask = logTask()
upAvgLogTask.attachTask(getUpstreamAvgResp)
attachedTasks.append(upAvgLogTask)

aggregator = Aggregator()

swapFileName = 'worker.swap'

if __name__ == '__main__':

    print "Start @ " + time.asctime()
    scriptTimeStart = time.time()
    nginxLogFormat = ('$remote_addr $host $remote_user [$time_local] $request '
                      '"$status" $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for" '
                      'upstream{$upstream_addr|$upstream_response_time|$upstream_status}')

    rePattern = dict_sub(nginxLogFormat)

    object = re.compile(rePattern)

    logFileName = 'bigLog.log'

    linenum = 0

    hitsPerTimeInterval = Counter()
    cachedQuery = Counter()
    upstreamResp = Counter()
    upstreamList = []

    a = 1
    with open(logFileName) as f:

        for line in f:
            if line != '':
                linenum += 1
                processedObj = parseLogLine(compiledReObject=object, text=line, displayResult=False)

                r = None
                if processedObj is not None:
                    r = processLogTasks(processedObj)
                if r is not None:
                    aggregator.aggregate(r)

aggregator.flushBuffer()
print "End @ " + time.asctime()

scriptTimeEnd = time.time()
scriptTimeTotal = scriptTimeEnd - scriptTimeStart
print "Running time: %f s" % scriptTimeTotal

