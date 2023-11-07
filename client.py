import rpyc
import uuid
import os
import random

REPLICATION_FACTOR = 3
BLOCK_SIZE = 10000
# con = rpyc.connect('localhost',18862)

# print(con.root.f())

# con.close()

def upload(filepath,destination):
    filename = filepath.split('/')[-1]
    con = rpyc.connect('localhost',18862)
    active_nodes,no = con.root.active_nodes()
    active_nodes = [i for i in active_nodes]
    print(active_nodes)
    con.close()
    size = os.path.getsize(filepath)
    blocks = size//BLOCK_SIZE + 1 if(size%BLOCK_SIZE!=0) else 0
    if(blocks < no):
        destination_nodes = random.sample(active_nodes,blocks)
    else:
        destination_nodes = random.choices(active_nodes,k = blocks)
    block_ids = []
    block_mappings = dict()
    with open(filepath) as f:
        for i in range(blocks):
            nodes = [destination_nodes[i]]
            data = f.read(BLOCK_SIZE)
            block_id = uuid.uuid4()
            block_ids.append(block_id)
            host,port = destination_nodes[i].split(':')
            con = rpyc.connect(host,int(port))
            status,replicas = con.root.write_block(block_id,data,active_nodes)
            replicas = [i for i in replicas]
            nodes.extend(replicas)
            con.close()
            if(status == 1):
                print(f'Block_{i} written successfully')
                block_mappings[str(block_id)] = nodes

    con = rpyc.connect('localhost',18862)
    con.root.add_file(filename,destination,block_mappings)
    con.close()
    print('File successfully added')

import rpyc

def download(filename, destination_path):
    try:
        nameNode_con = rpyc.connect('localhost', 18862)
        active_nodes,no = nameNode_con.root.active_nodes() # i havent used this 
        active_nodes = [i for i in active_nodes]

        block_mappings = nameNode_con.root.get_block_mappings(filename)
        nameNode_con.close()

        if not block_mappings:
            print(f"File '{filename}' not found in DFS.")
            nameNode_con.close()
            return
       
        with open(destination_path, 'a') as f:
            for block_id, data_nodes in block_mappings.items():
                success = False
                for data_node in data_nodes:
                    host, port = data_node.split(':')

                    try:
                        dataNode_con = rpyc.connect(host, int(port))
                        block_data = dataNode_con.root.read_block(block_id)
                        dataNode_con.close()

                        if block_data:
                            f.write(block_data)
                            success = True
                            break  

                    except Exception as e:
                        print(f"Error accessing DataNode {data_node}: {str(e)}")
                        continue 

                if not success:
                    print(f"Failed to retrieve block {block_id} from all DataNodes.")

        print(f"File '{filename}' downloaded to '{destination_path}' successfully.")

    except Exception as e:
        print(f"Error during download: {str(e)}")


if(__name__ == '__main__'):
   # upload('./test.txt','')
    download("./test.txt","./download.txt")



