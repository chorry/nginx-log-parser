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
    result = None
    type = None
    time = None

    def setType(self, type):
        self.type = type

    def setResult(self, result):
        self.result = result

    def setTime(self, time):
        self.time = time


    def __repr__(self):
        text =  ("Result:%s, Type:%s, Time:%s \n" %
               ( str(self.result or ''),
                 str(self.type or ''),
                 str(self.time or '') )
        )
        return text