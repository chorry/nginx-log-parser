from Sender import Sender
from Sender.UdpSender import UdpSender
from Sender.StdoutSender import StdoutSender


class Aggregator:
    #max amount of results in buffer
    maxBufferSize = 100
    bufferCount = 0
    buffer = []
    times = 0
    #seconds
    timeInterval = 1


    def __init__(self):
        self.sender = StdoutSender()
        self.tasks = []

    def addTask(self, taskObj):
        self.tasks.append(taskObj)

    def setSender(self, senderObj):
        self.sender = senderObj

    def aggregate(self, logTaskResultDict):
        for logTaskResult in logTaskResultDict:
            if len(self.buffer) >= self.maxBufferSize:
                self.flushBuffer()

            self.bufferCount += 1
            self.buffer.append(logTaskResult)

    def flushBuffer(self):
        self.times += 1

        tmp =0
        for task in self.getTaskList():
            for bufferEl in self.buffer:
                task.process(bufferEl)
                tmp += 1
                print tmp
            print '::result::' , task.result
            exit(0)

            self.sender.sendData(task.result)

        #processedData = "upstreamAvg=%f, cachedQ=%d, upstreamQ=%d" % (upstreamAvg, cachedQueries, upstreamQueries)
        #self.sender.sendData( processedData )

        #print "upstreamAvg=%f, cachedQ=%d, upstreamQ=%d" % (upstreamAvg, cachedQueries, upstreamQueries)
        #print "---buffer has flushed %d times!---\n" % self.times

        self.bufferCount = 0
        self.buffer = []

    def getTaskList(self):
        return self.tasks