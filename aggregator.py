from Sender import Sender
from Sender.UdpSender import UdpSender
from Sender.StdoutSender import StdoutSender
import time

class Aggregator:
    bufferCount = 0
    buffer = []
    times = 0


    def __init__(self, maxBufferSize = 'unlimited'):
        self.sender = None
        self.tasks = []
        self.maxBufferSize = maxBufferSize

    def addTask(self, taskObj):
        self.tasks.append(taskObj)

    def setSender(self, senderObj):
        self.sender = senderObj

    def aggregate(self, logTaskResultDict):
        for logTaskResult in logTaskResultDict:
            self.bufferCount += 1
            self.buffer.append(logTaskResult)

            if self.maxBufferSize == 'unlimited':
                return
            if len(self.buffer) >= self.maxBufferSize:
                self.flushBuffer()

    def flushBuffer(self):
        self.times += 1

        if self.sender is None:
            raise Exception("Sender object was not initialized")

        for task in self.getTaskList():
            if task.hasTaskMap():
                for bufferEl in self.buffer:
                    task.processTaskMap(bufferEl)
            if task.hasTaskReduce():
                task.processTaskReduce()

            self.sender.sendData(task.result)
            task.resetResult()

        self.bufferCount = 0
        self.buffer = []


    def getTaskList(self):
        return self.tasks