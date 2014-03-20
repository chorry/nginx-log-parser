from abc import ABCMeta, abstractmethod, abstractproperty

class Sender:

    __metaclass__  = ABCMeta

    @abstractmethod
    def getAddress(self):
        pass

    @abstractmethod
    def setAddress(self):
        pass

    @abstractmethod
    def sendData(self, data):
        pass
