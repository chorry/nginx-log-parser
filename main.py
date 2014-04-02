# -*- coding: utf-8 -*-
from __future__ import division


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
execfile('tasks/37944-test_v1.py')



if __name__ == '__main__':
    #restore settings
    configFileName = "worker.config.json"
    nginxLogFormat = ('abtype userid')

    config = {
        'path'       : 'testset',
        'currentFile': 'testset/set_1.txt',
        'resumeOnFile': True,
        'offset': 0
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


    lParser = LogParser(configFileName=configFileName, configParams=config)
    lParser.updateLogParams( { 'userid': '(?P<userid>\d+)', 'abtype': '(?P<abtype>[A|B])' } )
    lParser.setLogPattern(nginxLogFormat)

    #for processedObj in lParser.parseFile():
    for processedObj in lParser.parseFile( ['testset/set_1.txt'] ):
        if processedObj is not None:
            r = processLogTasks(processedObj)
            if r is not None:
                aggregator.aggregate(r)

    aggregator.flushBuffer()
print "the end"