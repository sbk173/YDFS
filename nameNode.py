import rpyc
import os
import json
from datetime import datetime
from shutil import rmtree


class NameNodeService(rpyc.Service):
    def __init__(self):
        with open('./config.json') as f:
            config = json.load(f)
        self.BLOCK_SIZE = config['block_size']
        self.DataNodes = config['Data_nodes']
        self.REPLICATION_FACTOR = config['replication_factor']
        self.ActiveNodes = []
        self.ROOT_FOLDER = os.path.join(os.getcwd(),'DFS')
        # self.checkStatus()

    def on_connect(self, conn):
        print("Connection Established")
        print(self.ActiveNodes)
        pass

    def checkStatus(self):
        self.ActiveNodes = []
        for i in self.DataNodes:
            x = i.split(':')
            try:
                con = rpyc.connect(x[0],int(x[1]))
                con._config['sync_request_timeout'] = 300
            except:
                print("DataNode not available")
                continue
            else:
                config = {'replication_factor': self.REPLICATION_FACTOR}
                con.root.alive(json.dumps(config))
                self.ActiveNodes.append(i)
                con.close()

    def absolute_path(self,path):
        path = path.lstrip('./')
        return os.path.join(self.ROOT_FOLDER,path)
    
    def exposed_is_valid_path(self,path):
        path = self.absolute_path(path)
        return os.path.exists(path) == True

    def exposed_check_file_exists(self,filename):
        return os.path.exists(os.path.join(self.ROOT_FOLDER,filename))

    def exposed_active_nodes(self):
        self.checkStatus()
        return self.ActiveNodes,len(self.ActiveNodes)
    
    def exposed_calculate_no_blocks(self,size):
        blocks = size//self.BLOCK_SIZE + 1 if(size%self.BLOCK_SIZE!=0) else 0
        return blocks , self.BLOCK_SIZE
    
    def exposed_add_file(self,filename,destination,block_mappings):
        dir = os.path.join(self.ROOT_FOLDER,destination)
        print(dir)
        if(os.path.exists(dir) == False):
            print("No such file or directory")
            return -1
        else:
            metadata = dict()
            block_mappings = json.loads(block_mappings)
            metadata['block_mappings'] = block_mappings
            metadata['date_created'] = str(datetime.now())
            fullpath = os.path.join(dir,filename)
            print(type(block_mappings))
            print(fullpath)
            with open(fullpath,'w') as f:
                f.write(json.dumps(metadata))

    
    
    def exposed_get_block_mappings(self, filename):
        file_path = os.path.join(self.ROOT_FOLDER, filename)
        print(file_path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {filename} not found in DFS")
        with open(file_path, 'r') as f:
            metadata = json.load(f)
            block_mappings = metadata.get('block_mappings', {})
        return json.dumps(block_mappings)
    
    def exposed_mkdir(self,dir_path):
        dir_path = self.absolute_path(dir_path)
        if(os.path.exists(dir_path) == True):
            raise Exception(f"Directory Already exists")
        else:
            os.mkdir(dir_path)

    # def exposed_traversal(self,path):
    #     dest_path = self.absolute_path(path)
    #     if(os.path.exists(dest_path)==True and os.path.isdir(dest_path)==True):
    #         return 1
    #     else:
    #         raise Exception(f'No such directory {path}')
    
    def exposed_copy(self,source,destination):
        source = self.absolute_path(source)
        destination = self.absolute_path(destination)

        file_name = source.split('/')[-1]
        if(os.path.exists(source)== True and os.path.exists(destination)==True):
            with open(source,'r') as f:
                with open(os.path.join(destination,file_name),'w') as f2:
                    data = f.read()
                    f2.write(data)
            
        else:
            raise Exception("Either source or destination path doesn't exist")
    
    def exposed_move(self,source,destination):
        source = self.absolute_path(source)
        destination = self.absolute_path(destination)
        if(source == destination):
            raise Exception("Source cannot be the same as destination")
        dest_dir = "/".join(destination.split('/')[:-1])
        if(os.path.exists(source) == True and os.path.exists(dest_dir) == True):
            with open(source,'r') as f:
                with open(destination,'w') as f2:
                    data = f.read()
                    f2.write(data)
            
            os.remove(source)
        else:
            raise Exception("Either source or destination path doesn't exist")
        
    def exposed_list_contents(self,path):
        path = self.absolute_path(path)
        if(os.path.exists(path) == False):
            raise Exception('No such directory')
        elif(os.path.isdir(path)==False):
            raise Exception('Given Path is not a directory')
        else:
            return os.listdir(path)
    
    def exposed_remove_file(self,path):
        path = self.absolute_path(path)
        if(os.path.exists(path) == False):
            raise Exception("No such file")
        else:
            os.remove(path)
            return 1
        
    def exposed_rmdir(self,path):
        path = self.absolute_path(path)
        if(os.path.exists(path) == False):
            raise Exception("No such directory")
        else:
            rmtree(path)
            return 1
            

    def exposed_total_size(self):
        total_used = 0
        full = 0
        for i in self.DataNodes:
            x = i.split(':')
            try:
                con = rpyc.connect(x[0],int(x[1]))
                con._config['sync_request_timeout'] = 300
            except:
                print("DataNode not available")
                continue
            else:
                disk_use = con.root.size_remaining()
                total_used += disk_use["used"]
                full += disk_use["total"]

        return total_used,full


if(__name__=='__main__'):
    from rpyc.utils.server import ThreadedServer
    thread= ThreadedServer(NameNodeService,port=18862)
    thread.start()