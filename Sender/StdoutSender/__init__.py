from Sender import  Sender

class StdoutSender(Sender):

    def getAddress(self):
        pass

    def setAddress(self):
        pass

    def sendData(self, data):
        print "%s" % data

