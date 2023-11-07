import rpyc
import os
import json
from datetime import datetime



class NameNodeService(rpyc.Service):
    def __init__(self):
        with open('./config.json') as f:
            config = json.load(f)
        self.block_size = config['block_size']
        self.DataNodes = config['Data_nodes']
        self.ActiveNodes = []
        self.ROOT_FOLDER = os.path.join(os.getcwd(),'DFS')
        self.checkStatus()

    def on_connect(self, conn):
        print("Connection Established")
        self.checkStatus()
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


    def exposed_active_nodes(self):
        return self.ActiveNodes,len(self.ActiveNodes)
    
    def exposed_add_file(self,filename,destination,block_mappings):
        dir = os.path.join(self.ROOT_FOLDER,destination)
        print(dir)
        if(os.path.exists(dir) == False):
            print("No such file or directory")
            return -1
        else:
            metadata = dict()
            print(block_mappings)
            block_mappings = {i:list(block_mappings[i]) for i in block_mappings}
            metadata['block_mappings'] = block_mappings
            metadata['date_created'] = str(datetime.now())
            fullpath = os.path.join(dir,filename)
            print(type(block_mappings))
            print(fullpath)
            with open(fullpath,'w') as f:
                f.write(json.dumps(metadata))

    
    
    def exposed_get_block_mappings(self, filename):
        file_path = os.path.join(self.ROOT_FOLDER, filename)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {filename} not found in DFS")

        with open(file_path, 'r') as f:
            metadata = json.load(f)
            block_mappings = metadata.get('block_mappings', {})
        
        return block_mappings
    

if(__name__=='__main__'):
    from rpyc.utils.server import ThreadedServer
    thread= ThreadedServer(NameNodeService,port=18862)
    thread.start()