import sys, types, pprint

'''
Simple logTask class
How to use:

def someTask(self, logDict):
    print self.__class__
    for i in range(len(logDict)):
        print i, logDict[i]

task = logTask()
task.attachTask( someTask )
task.process( [1,-2,3] )
'''


class Task:
    task = ''

    def __init__(self, func):
        self.attachTask(func)

    def process(self, object):
        result = None
        if self.task != '':
            result = self.task(object)
        return result

    """Attaches task function"""

    def attachTask(self, func):
        self.task = types.MethodType(func, self)


class TaskResult:
    result = []
    taskMap = taskReduce = None
    type = None
    time = None

    def __init__(self, func=None, funcMap=None, funcReduce=None):
        self.resetResult()

        if func is not None:
            self.attachTask(func)

        if funcMap is not None:
            self.attachTaskMap(funcMap)

        if funcReduce is not None:
            self.attachTaskReduce(funcReduce)

    """Attaches task function"""

    def attachTask(self, func):
        self.task = types.MethodType(func, self)

    def attachTaskMap(self, func):
        self.taskMap = types.MethodType(func, self)

    def attachTaskReduce(self, func):
        self.taskReduce = types.MethodType(func, self)

    def hasTaskMap(self):
        return self.taskMap != []

    def hasTaskReduce(self):
        return self.taskReduce != []

    def process(self, object):
        if self.task != '':
            result = self.task(object)
        self.result.append(result)

    def processTaskMap(self, object):
        if self.taskMap != '':
            result = self.taskMap(object)
        self.result.append(result)

    def processTaskReduce(self):
        result = None
        if self.taskReduce != '':
            result = self.taskReduce(self)
        self.resetResult()
        self.result = result

    def resetResult(self):
        self.result = []

    def getResult(self):
        return self.result

    def setType(self, type):
        self.type = type

    def setResult(self, result):
        self.result = result

    def setTime(self, time):
        self.time = time


    def __repr__(self):
        text = ("Result:%s, Type:%s, Time:%s \n" %
                ( str(self.result or ''),
                  str(self.type or ''),
                  str(self.time or '') )
        )
        return text