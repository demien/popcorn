# import socket

# if __name__ == '__main__':
#     HOST = '172.17.0.2'    # The remote host
#     PORT = 5555              # The same port as used by the server
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     print '1'
#     # s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     print '2'
#     s.connect((HOST, PORT))
#     print '3'
#     # s.sendall('Hello, world')
#     print '4'
#     # s1.connect((HOST, PORT))
#     print '5'
#     # s1.sendall('Hello, demien')
#     print '6'
#     data = s.recv(1024)
#     print '7'
#     # data1 = s1.recv(1024)
#     print '8'
#     s.close()
#     # s1.close()
#     print 'Received', repr(data)
#     # print 'Received', repr(data1)

'''
Created on 2012-1-5
The example client program uses some sockets to demonstrate how the server
with select() manages multiple connections at the same time . The client
starts by connecting each TCP/IP socket to the server
@author: peter
'''
 
import socket
 
messages = ["This is the message" ,
            "It will be sent" ,
            "in parts "]
 
print "Connect to the server"
 
server_address = ("172.17.0.2",5555)
 
#Create a TCP/IP sock
 
socks = []
 
for i in range(2):
    socks.append(socket.socket(socket.AF_INET,socket.SOCK_STREAM))
 
for s in socks:
    s.connect(server_address)
 
counter = 0
for message in messages :
    #Sending message from different sockets
    for s in socks:
        counter+=1
        print "  %s sending %s" % (s.getpeername(),message+" version "+str(counter))
        s.send(message+" version "+str(counter))
    #Read responses on both sockets
    for s in socks:
        data = s.recv(1024)
        print " %s received %s" % (s.getpeername(),data)
        if not data:
            print "closing socket ",s.getpeername()
            s.close()
