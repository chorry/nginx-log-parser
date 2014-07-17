# -*- coding: utf-8 -*-
import json
import fcntl
import re
import res
import os

defaultConfig = {
    'offset': 0,
    'linenum': 0,
    'path': None
}


def dict_sub(text, d=res.LOG_PARAMS):
    """ Replace in 'text' non-overlapping occurences of REs whose patterns are keys
    in dictionary 'd' by corresponding values (which must be constant strings: may
    have named backreferences but not numeric ones). The keys must not contain
    anonymous matching-groups.
    Returns the new string.
    Thanks to Alex Martelli @
    http://stackoverflow.com/questions/937697/can-you-pass-a-dictionary-when-replacing-strings-in-python
    """
    if d != res.LOG_SPECIAL_SYMBOLS:
        text = dict_sub(text, res.LOG_SPECIAL_SYMBOLS)


    # Create a regular expression  from the dictionary keys
    regex = re.compile("|".join("(%s)" % k for k in d))
    # Facilitate lookup from group number to value
    lookup = dict((i + 1, v) for i, v in enumerate(d.itervalues()))
    # For each match, find which group matched and expand its value
    result = regex.sub(lambda mo: mo.expand(lookup[mo.lastindex]), text)

    return result


class LogParser:
    pass

    def __init__(self, configFileName=None, configParams=None):

        if configFileName is None:
            configFileName = 'worker.config.json'
        self.configFileName = configFileName
        self.readConfigFile(configFileName)

        if configParams is not None:
            self.config.update(configParams)

        #non-blocking trick
        self.configFile = open(self.configFileName, "r+")
        fcntl.lockf(self.configFile, fcntl.LOCK_EX)

        self.verboseMode = False
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

    def setLogPattern(self, pattern):
        self.setRePattern(dict_sub(pattern))

    def setRePattern(self, pattern):
        self.rePattern = pattern

    def getReObject(self):
        if self.reObject is None:
            self.reObject = re.compile(self.rePattern)
        return self.reObject

    def getFilesFromFolder(self, path):
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
        if os.path.isdir(self.config['path']):
            logFileList = self.getFilesFromFolder(self.config['path'])
        elif os.path.isfile(self.config['path']):
            logFileList = [self.config['path']]
        else:
            raise Exception("%s not found!" % self.config['path'])

        return logFileList

    def parseCLIText(self, text):

        processedObj = self.parseLogLine(
            compiledReObject=self.getReObject(),
            text=text,
            displayResult=self.verboseMode)
        yield processedObj

    def getCurrentFileFullPath(self):
        return self.config['path'] + '/' + self.config['currentFile']

    def parseFile(self, logFileList=None):
        if logFileList is None:
            logFileList = self.getLogFileList()

        resume = self.config['resumeOnFile']

        for logFileName in logFileList:
            if resume == False and 'currentFile' in self.config and self.config['currentFile'] != logFileName:
                continue

            if self.getCurrentFileFullPath() != logFileName:
                self.config['offset'] = 0
                self.config['linenum'] = 0

            resume = True

            self.config['currentFile'] = os.path.basename(logFileName)
            with open(logFileName) as f:
                f.seek(self.config['offset'])
                line = f.readline()
                while line != '':
                    self.config['offset'] = f.tell()
                    self.config['linenum'] += 1

                    processedObj = self.parseLogLine(compiledReObject=self.getReObject(), text=line,
                                                     displayResult=self.verboseMode)

                    json.dump(self.config, self.configFile)
                    self.configFile.seek(0)
                    line = f.readline()

                    yield processedObj


    def parseLogLine(self, compiledReObject, text, displayResult=True):

        result = compiledReObject.finditer(text)
        group_name_by_index = dict([(v, k) for k, v in compiledReObject.groupindex.items()])

        resultObj = {}
        iteratorSize = 0

        for match in result:
            iteratorSize += 1
            for group_index, group in enumerate(match.groups()):
                if group:
                    if (group_index + 1) in group_name_by_index:
                        resultObj[group_name_by_index[group_index + 1]] = group

        if iteratorSize == 0:
            if self.verboseMode:
                print("Could not parse [ %s ]" % text)
            return None

        #TODO: do something with this date
        #resultObj['t_date_local'] = resultObj['time_local'][:11]
        #resultObj['t_time_local'] = resultObj['time_local'][12:20]
        #if resultObj['upstream_response_time'] != '-':
        #    if resultObj['upstream_response_time'].find(':') != -1:
        #        splitKey = ':'
        #    else:
        #        splitKey = ','
        #    resultObj['upstreams'] = resultObj['upstream_response_time'].split(splitKey)
        return resultObj

    def updateLogParams(self, l):
        res.LOG_PARAMS.update(l)