from Sender import  Sender
import socket

class UdpSender(Sender):

    def getAddress(self):
        return (self.udp_ip, self.udp_port)

    def setAddress(self, udp_ip, udp_port):
        self.udp_ip = udp_ip
        self.udp_port = udp_port

    def sendData(self, data):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(data, self.getAddress() )

