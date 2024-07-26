import rpyc
import os
import random

REPLICATION_FACTOR = 3

class DataNodeService(rpyc.Service):
    def on_connect(self, conn):
        print("Connection Established")

    def replicate(self,block_id,block_data,active_nodes):
        destination_node = random.sample(active_nodes,1)[0]
        host,port = destination_node.split(':')
        con = rpyc.connect(host,int(port))
        con.root.write_block(block_id,block_data,active_nodes,mode=0)
        con.close()
        return destination_node
    
    def exposed_alive(self):
        return 1
    
    def exposed_write_block(self,block_id,block_data,active_nodes,times=REPLICATION_FACTOR,mode=1):
        new_times = int(times)-1
        with open(f'./DATA/{block_id}','w') as f:
            f.write(block_data)
        # new_times = new_times-1
        if(mode == 0):
            return 1
        replicas = []
        for i in range(new_times):
            replicas.append(self.replicate(block_id,block_data,active_nodes))
        
        return 1,replicas

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

