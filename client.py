import rpyc
import uuid
import os
import random
import json

# REPLICATION_FACTOR = 3
# BLOCK_SIZE = 10000
# con = rpyc.connect('localhost',18862)
# print(con.root.f())
# con.close()


def upload(filepath,destination):
    filename = filepath.split('/')[-1]
    #Get active DataNode List
    con = rpyc.connect('localhost',18862)
    active_nodes,no = con.root.active_nodes()
    active_nodes = [i for i in active_nodes]
    # print(active_nodes)

    #Calculate Number of Blocks Required
    size = os.path.getsize(filepath)
    blocks,BLOCK_SIZE = con.root.calculate_no_blocks(size)
    blocks = int(blocks)
    BLOCK_SIZE = int(BLOCK_SIZE)
    con.close()

    #Decide on which DataNodes to use
    if(blocks < no):
        destination_nodes = random.sample(active_nodes,blocks)
    else:
        destination_nodes = random.choices(active_nodes,k = blocks)

    #Writing Blocks to DataNode
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

    #Update Metadata
    con = rpyc.connect('localhost',18862)
    con.root.add_file(filename,destination,json.dumps(block_mappings))
    con.close()
    print('File successfully added')



def download(filename, destination_path):
    try:
        #Get metadata of the file
        nameNode_con = rpyc.connect('localhost', 18862)
        block_mappings = nameNode_con.root.get_block_mappings(filename)
        nameNode_con.close()

        block_mappings = str(block_mappings)
        block_mappings = json.loads(block_mappings)
        if len(block_mappings) == 0:
            print(f"File '{filename}' not found in DFS.")
            nameNode_con.close()
            return
        
        #Write Data to output file
        with open(destination_path, 'w') as f:
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

def ls(file_path):
    if os.path.exists(file_path):
        files = [f for f in os.listdir(file_path)]
        for i, file in enumerate(files):
            print(f"{i+1}. {file}")
    else:
        print("Directory does not exist.")

if __name__ == '__main__':
    #upload('53.electricity.csv','')
    #download("53.electricity.csv","./download.csv")

    # print("In client")
    x = True
    while x:
        inp = input().strip().split()
        if inp[0] == 'exit':
            x = False
        if inp[0] == 'ls':
            ls(inp[1])