import rpyc

class DataNodeService(rpyc.Service):
    def on_connect(self, conn):
        print("Connection Extablished")
    
    def exposed_alive(self):
        return 1

if(__name__ == '__main__'):
    from rpyc.utils.server import ThreadedServer
    thread = ThreadedServer(DataNodeService,port=18861)
    thread.start()

