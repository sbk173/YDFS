import rpyc
import os

class DataNodeService(rpyc.Service):
    def on_connect(self, conn):
        print("Connection Extablished")
    
    def exposed_alive(self):
        return 1
    
    def exposed_write_block(self,block_id,block_data):
        with open(f'./DATA/{block_id}.txt','w') as f:
            f.write(block_data)
        return 1
    
    def exposed_read_block(self,block_id):
        with open(f'./DATA/{block_id}','r') as f:
            data = f.read()
        return data
    
    def exposed_delete_block(self,block_id):
        os.remove(f'./DATA/{block_id}')


if(__name__ == '__main__'):
    from rpyc.utils.server import ThreadedServer
    thread = ThreadedServer(DataNodeService,port=18861)
    thread.start()

