# -*- coding: utf-8 -*-
import json
import fcntl
import re
import os

defaultConfig = {
    'offset': 0,
    'linenum': 0,
    'path': None
}


class LogParser:
    pass

    def __init__(self, configFileName=None, configParams=None):


        if configFileName is None:
            configFileName = 'worker.config.json'
        self.configFileName = configFileName
        self.readConfigFile(configFileName)

        if configParams is not None:
            self.config.update( configParams )

        #non-blocking trick
        self.configFile = open(self.configFileName, "r+")
        fcntl.lockf(self.configFile, fcntl.LOCK_EX)

        self.displayResult = False
        self.reObject = None

    def readConfigFile(self, configFileName):
        try:
            self.configFile = open(self.configFileName, "r")
            try:
                self.config = json.load(self.configFile)
            except ValueError:
                self.config = defaultConfig
        except IOError:
            open(self.configFileName, 'a').close()
            self.config = defaultConfig

    def setRePattern(self, pattern):
        self.rePattern = pattern

    def getReObject(self):
        if self.reObject is None:
            self.reObject = re.compile(self.rePattern)
        return self.reObject

    def getFilesFromFolder(self,path):
        f = []
        for (dirpath, dirnames, filenames) in os.walk(path):
            f.extend(
                [
                    dirpath + v for v in filenames
                ]
            )
            break
        return f

    def getLogFileList(self):
        print (self.config)
        if os.path.isdir(self.config['path']):
            logFileList = self.getFilesFromFolder(self.config['path'])
        elif os.path.isfile(self.config['path']):
            logFileList = [self.config['path']]
        else:
            raise Exception( "%s not found!" % self.config['path'] )

        return logFileList

    def parseFile(self, logFileList=None):
        if logFileList is None:
            logFileList = self.getLogFileList()
        resume = False
        for logFileName in logFileList:
            if resume == False and 'currentFile' in self.config and self.config['currentFile'] != logFileName:
                continue
            resume = True
            self.config['currentFile'] = logFileName
            with open(logFileName) as f:
                f.seek(self.config['offset'])
                line = f.readline()
                while line != '':
                    self.config['offset'] = f.tell()
                    self.config['linenum'] += 1

                    """
                    if verboseMode:
                        sys.stdout.write('\r')
                        sys.stdout.flush()
                        sys.stdout.write( "Processed %d bytes " % self.config['offset'] )
                        sys.stdout.flush()
                    """
                    processedObj = self.parseLogLine(compiledReObject=self.getReObject(), text=line,
                                                     displayResult=self.displayResult)

                    json.dump(self.config, self.configFile)
                    self.configFile.seek(0)
                    line = f.readline()

                    yield processedObj

                    #if processedObj is not None:
                    #    r = processLogTasks(processedObj)
                    #if r is not None:
                    #    aggregator.aggregate(r)


    def parseLogLine(self, compiledReObject, text, displayResult=True):
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