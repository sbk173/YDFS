import rpyc
import os
import random

REPLICATION_FACTOR = 3

class DataNodeService(rpyc.Service):
    def on_connect(self, conn):
        print("Connection Extablished")

    def replicate(self,block_id,block_data,active_nodes,times,replicas=[]):
        replicas = [i for i in replicas]
        destination_node = random.sample(active_nodes,1)[0]
        host,port = destination_node.split(':')
        con = rpyc.connect(host,int(port))
        replicas.append(destination_node)
        con.root.write_block(block_id,block_data,active_nodes,times-1,replicas)
        con.close()
    
    def exposed_alive(self):
        return 1
    
    def exposed_write_block(self,block_id,block_data,active_nodes,times=REPLICATION_FACTOR,replicas = []):
        print(times,type(times))
        new_times = int(times)-1
        with open(f'./DATA/{block_id}.txt','w') as f:
            f.write(block_data)
        new_times = new_times-1
        
        if(new_times > 0):
            self.replicate(block_id,block_data,active_nodes,new_times,replicas)
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

