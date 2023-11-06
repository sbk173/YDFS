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
            data = f.read(BLOCK_SIZE)
            block_id = uuid.uuid4()
            block_ids.append(block_id)
            host,port = destination_nodes[i].split(':')
            con = rpyc.connect(host,int(port))
            status,nodes = con.root.write_block(block_id,data,active_nodes,replicas = [destination_nodes[i]])
            con.close()
            if(status == 1):
                print(f'Block_{i} written successfully')
                block_mappings[str(block_id)] = nodes

    con = rpyc.connect('localhost',18862)
    con.root.add_file(filename,destination,block_mappings)
    con.close()
    print('File successfully added')



if(__name__ == '__main__'):
    upload('./test.txt','')



