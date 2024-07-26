import rpyc
import os
import random
import json
import shutil

class DataNodeService(rpyc.Service):
    def __init__(self):
        self.REPLICATION_FACTOR = None

    def on_connect(self, conn):
        print("Connection Established")

    def replicate(self,block_id,block_data,active_nodes):
        destination_node = random.sample(active_nodes,1)[0]
        print(destination_node)
        host,port = destination_node.split(':')
        try:
            con = rpyc.connect(host,int(port))
            con.root.write_block(block_id,block_data,active_nodes,mode=0)
            con.close()
        except:
            print("couldnt connect to",destination_node)
        return destination_node
    
    def exposed_alive(self,config):
        config = json.loads(config)
        self.REPLICATION_FACTOR = int(config['replication_factor'])
        return 1
    
    def exposed_write_block(self,block_id,block_data,active_nodes,mode=1):
        new_times = self.REPLICATION_FACTOR - 1
        with open(f'./DATA/{block_id}','w') as f:
            f.write(block_data)
        # new_times = new_times-1
        if(mode == 0):
            return 1
        active_nodes = [i for i in active_nodes]
        replicas = []
        for i in range(new_times):
            if(len(active_nodes)>=(new_times-i)):
                replica_node = self.replicate(block_id,block_data,active_nodes)
                replicas.append(replica_node)
                active_nodes.remove(replica_node)

        return 1,replicas

    def exposed_read_block(self,block_id):
        with open(f'./DATA/{block_id}','r') as f:
            data = f.read()
        return data
    
    def exposed_delete_block(self,block_id):
        os.remove(f'./DATA/{block_id}')

    def exposed_size_remaining(self,path='.'):
        disk_usage = shutil.disk_usage(path)
        return {
            "total": disk_usage.total,
            "used": disk_usage.used,
            "free": disk_usage.free
        }


if(__name__ == '__main__'):
    from rpyc.utils.server import ThreadedServer
    import sys
    port = int(sys.argv[1])
    thread = ThreadedServer(DataNodeService(),port = port)
    thread.start()

