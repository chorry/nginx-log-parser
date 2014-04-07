from Sender import Sender
from Task import Task, TaskResult
from logParser import LogParser

class TaskSender(Sender):

    def getAddress(self):
        pass

    def setAddress(self):
        pass

    def sendData(self, data):
        findInLog('processedLogs/mergedU.20140403.A',data, self.fileName)
        #print "STDOUT: %s" % data


def findInLog(logFile, searchData, fileName):
    nginxLogFormat = defaultNginxLogFormat = ('abtype userid')

    config = {
        'path'         : 'processedLogs',
        'currentFile'  : logFile,
        'offset'       : 0,
        'resumeOnFile' : True,
    }

    lParser = LogParser( configParams=config, configFileName='taskSender1.config.json' )
    lParser.updateLogParams( { 'userid': '(?P<userid>uid=[A-Z0-9]+)', 'abtype': '(?P<abtype>[A|B|-])' } )
    lParser.setLogPattern(nginxLogFormat)


    resume = False
    searchDataLen = len(searchData)
    tmp = 0
    matchId = 0
    abKey = 'A'

    result = {'A': 0, 'B': 0}

    f = open('0203_A.log','a')

    for processedObj in lParser.parseFile( [ logFile ] ):
        tmp += 1
        if processedObj is not None:
            if processedObj['abtype'] == abKey:
                matchKey = processedObj['abtype'] + " " + processedObj['userid']
                if matchKey in searchData:
                    matchId += 1
                    #print matchId, tmp, processedObj['userid'] + " found in set "
                    result[ processedObj['abtype'][:1] ] += 1
                    #print "removing %s" % matchKey
                    searchData.remove( matchKey )

                    if result[ processedObj['abtype'][:1] ] == searchDataLen:
                        print ("%s: %s/%s" % (abKey, result['A'], searchDataLen  ) )
                        txt = "%s: %s/%s\n" % (abKey, result['A'], searchDataLen  )
                        print txt
                        f.write(txt)
                        f.close()
                        return

    txt = "%s: %s/%s\n" % (abKey, result['A'], searchDataLen  )
    f.write(txt)
    f.close()
    #print ("RESULT:", result)

        #if processedObj is not None:
            #print processedObj