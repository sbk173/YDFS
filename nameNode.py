import rpyc
import time
import json

class NameNodeService(rpyc.Service):
    def __init__(self):
        with open('./config.json') as f:
            config = json.load(f)
        self.block_size = config['block_size']
        self.DataNodes = config['Data_nodes']
        self.ActiveNodes = []
        self.checkStatus()
    def on_connect(self, conn):
        pass

    def checkStatus(self):
        self.ActiveNodes = []
        for i in self.DataNodes:
            x = i.split(':')
            try:
                con = rpyc.connect(x[0],int(x[1]))
            except:
                print("DataNode not available")
                continue
            else:
                print(con.root.alive())
                self.ActiveNodes.append(i)
                con.close()


    def exposed_f(self):
        return self.ActiveNodes

if(__name__=='__main__'):
    from rpyc.utils.server import ThreadedServer
    thread= ThreadedServer(NameNodeService,port=18862)
    thread.start()