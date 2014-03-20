class Aggregator:
    #max amount of results in buffer
    maxBufferSize = 100
    bufferCount = 0
    buffer = []
    times = 0
    #seconds
    timeInterval = 1

    def aggregate(self, logTaskResultDict):
        for logTaskResult in logTaskResultDict:
            if len(self.buffer) >= self.maxBufferSize:
                self.flushBuffer()

            self.bufferCount += 1
            self.buffer.append(logTaskResult)

    def flushBuffer(self):
        self.times += 1
        upstreamAvg = 0
        cachedQueries = 0
        upstreamQueries = 0

        for bufferEl in self.buffer:

            if bufferEl.type == 'upstreamAvg':
                upstreamAvg += bufferEl.result if bufferEl.result is not None else 0
                pass
            elif bufferEl.type == 'cachedQuery':
                if bufferEl.result == 1:
                    cachedQueries += 1
                else:
                    upstreamQueries += 1
                pass
            else:
                raise Exception("Unknown log result?")

        #print "upstreamAvg=%f, cachedQ=%d, upstreamQ=%d" % (upstreamAvg, cachedQueries, upstreamQueries)
        #print "---buffer has flushed %d times!---\n" % self.times

        self.bufferCount = 0
        self.buffer = []

    def aggregateCachedQueriesByTimeInterval(self):
        pass

    def aggregateUpstreamAvgByTimeInterval(self):
        pass