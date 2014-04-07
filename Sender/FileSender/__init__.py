from Sender import Sender
from Task import Task, TaskResult
from logParser import LogParser

class FileSender(Sender):

    def __init__(self):
        self.fileName = None
        self.abKey = None
        self.compareFile = None

    def setFileName(self, f):
        self.fileName = f

    def getAddress(self):
        pass

    def setAddress(self):
        pass

    def setCompareFile(self, f):
        self.compareFile = f

    def setAbKey(self, key):
        self.abKey = key

    def sendData(self, data):
        findInLog(self.compareFile,data, self.fileName, self.abKey)
        #print "STDOUT: %s" % data


def findInLog(logFile, searchData, fileName, abKey):
    nginxLogFormat = defaultNginxLogFormat = ('abtype userid')

    config = {
        'path'         : 'processedLogs',
        'currentFile'  : logFile,
        'offset'       : 0,
        'resumeOnFile' : True,
    }

    lParser = LogParser( configParams=config, configFileName='taskSender.' + abKey + '.config.json' )
    lParser.updateLogParams( { 'userid': '(?P<userid>uid=[A-Z0-9]+)', 'abtype': '(?P<abtype>[A|B|-])' } )
    lParser.setLogPattern(nginxLogFormat)


    resume = False
    searchDataLen = len(searchData)
    tmp = 0
    matchId = 0
    abKey = abKey

    result = {'A': 0, 'B': 0}

    f = open(fileName,'a')

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
                        print ("%s: %s/%s" % (abKey, result[abKey], searchDataLen  ) )
                        txt = "%s: %s/%s\n" % (abKey, result[abKey], searchDataLen  )
                        print txt
                        f.write(txt)
                        f.close()
                        return

    txt = "%s: %s/%s\n" % (abKey, result[abKey], searchDataLen  )
    f.write(txt)
    f.close()
    #print ("RESULT:", result)

        #if processedObj is not None:
            #print processedObj