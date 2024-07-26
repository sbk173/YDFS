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

def clear_screen():
    command = 'clear' if (os.name == 'posix') else 'cls'
    os.system(command)
    print('''
 .----------------. .----------------. .----------------. .----------------. 
| .--------------. | .--------------. | .--------------. | .--------------. |
| |  ____  ____  | | |  ________    | | |  _________   | | |    _______   | |
| | |_  _||_  _| | | | |_   ___ `.  | | | |_   ___  |  | | |   /  ___  |  | |
| |   \ \  / /   | | |   | |   `. \ | | |   | |_  \_|  | | |  |  (__ \_|  | |
| |    \ \/ /    | | |   | |    | | | | |   |  _|      | | |   '.___`-.   | |
| |    _|  |_    | | |  _| |___.' / | | |  _| |_       | | |  |`\____) |  | |
| |   |______|   | | | |________.'  | | | |_____|      | | |  |_______.'  | |
| |              | | |              | | |              | | |              | |
| '--------------' | '--------------' | '--------------' | '--------------' |
 '----------------' '----------------' '----------------' '----------------' 
''')
    

def upload(filepath,destination):
    filename = filepath.split('/')[-1]
    #Get active DataNode List
    con = rpyc.connect('localhost',18862)
    con._config['sync_request_timeout'] = 300
    active_nodes,no = con.root.active_nodes()
    active_nodes = [i for i in active_nodes]
    # print(active_nodes)
    if not con.root.check_file_exists(filename):
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

                active_nodes.remove(destination_nodes[i])

                status,replicas = con.root.write_block(block_id,data,active_nodes)

                active_nodes.append(destination_nodes[i])

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
    else:
        con.close()
        print("File is present in the DFS")
        return



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



def delete_files(filepath):
    try:
        nameNode_con = rpyc.connect('localhost', 18862)
        valid_path = nameNode_con.root.is_valid_path(filepath)
        nameNode_con.close()

        if valid_path:
            nameNode_con = rpyc.connect('localhost', 18862)
            block_mappings = nameNode_con.root.get_block_mappings(filepath)
            nameNode_con.close()
            block_mappings = str(block_mappings)
            block_mappings = json.loads(block_mappings)

            if  not len(block_mappings):
                print("Could not find the corresponding file blocks")
                return 
            for block_id, data_nodes in block_mappings.items():
                for data_node in data_nodes:
                    host, port = data_node.split(':')
                    try:
                        dataNode_con = rpyc.connect(host, int(port))
                        
                        try:
                            dataNode_con.root.delete_block(block_id)
                        except:
                            #print("Exception when deleting blocks")
                            pass

                        dataNode_con.close()

                    except Exception as e:
                        print(f"Error accessing DataNode {data_node}: {str(e)}")
                        continue
            nameNode_con = rpyc.connect('localhost', 18862)
            nameNode_con.root.remove_file(filepath)
            nameNode_con.close()
            print(f"File '{filepath} deleted successfully !'")
        
        else:
            print("The file is not present in the DFS")
            return 
        

    except Exception as err:
        print("Exception in delete_files :",str(err))

def move_files(source,destination):
    nameNode_con = rpyc.connect('localhost', 18862)
    nameNode_con.root.move(source,destination)
    nameNode_con.close()

def copy_files(source,destination):
    nameNode_con = rpyc.connect('localhost', 18862)
    nameNode_con.root.copy(source,destination)
    nameNode_con.close()

def list_contents(path,mode = 0):
    nameNode_con = rpyc.connect('localhost', 18862)
    contents = nameNode_con.root.list_contents(path)
    contents = [i for i in contents]
    if(mode==0):print(contents)
    nameNode_con.close()
    return contents

def make_dir(path):
    nameNode_con = rpyc.connect('localhost', 18862)
    nameNode_con.root.mkdir(path)
    nameNode_con.close()

def get_size():
    nameNode_con = rpyc.connect('localhost', 18862)
    nameNode_con._config['sync_request_timeout'] = 300
    total_used,full = nameNode_con.root.total_size()
    nameNode_con.close()
    print(f"{total_used} Bytes used out of {full} Bytes")

# def remove_dir(path):
#     contents = list_contents(path,mode=1)
#     if (len(contents)>0):
#         choice = input("Given directory is not empty, Do you still want to delete it? [y/n]")
#         if(choice == 'y'):
#             nameNode_con = rpyc.connect('localhost', 18862)
#             nameNode_con.root.rmdir(path)
#             nameNode_con.close()
#         else:
#             return
#     else:
#         nameNode_con = rpyc.connect('localhost', 18862)
#         nameNode_con.root.rmdir(path)
#         nameNode_con.close()

            


if(__name__ == '__main__'):
    #upload('electricity.csv','')
    #download("electricity.csv","./download.csv")
    # # move_files('root/moved.csv','original.csv')
    # delete_files("root/electricity.csv")
    
    print('''
 .----------------. .----------------. .----------------. .----------------. 
| .--------------. | .--------------. | .--------------. | .--------------. |
| |  ____  ____  | | |  ________    | | |  _________   | | |    _______   | |
| | |_  _||_  _| | | | |_   ___ `.  | | | |_   ___  |  | | |   /  ___  |  | |
| |   \ \  / /   | | |   | |   `. \ | | |   | |_  \_|  | | |  |  (__ \_|  | |
| |    \ \/ /    | | |   | |    | | | | |   |  _|      | | |   '.___`-.   | |
| |    _|  |_    | | |  _| |___.' / | | |  _| |_       | | |  |`\____) |  | |
| |   |______|   | | | |________.'  | | | |_____|      | | |  |_______.'  | |
| |              | | |              | | |              | | |              | |
| '--------------' | '--------------' | '--------------' | '--------------' |
 '----------------' '----------------' '----------------' '----------------' 
''')

    x = True
    print("Type 'help' or 'man' to view all the ydfs commands")
    while x:
        inp = input("YDFS ->").strip().split()
        if(len(inp)<=0):
            continue
        inp = [i.lower() for i in inp]
        if inp[0] == 'ls':
            try:
                list_contents(inp[1])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")
        elif inp[0] == 'mkdir':
            try:
                make_dir(inp[1])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")

        elif inp[0] == 'put':
            try:
                upload(inp[1],inp[2])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")

        elif inp[0] == 'get':
            try:
                download(inp[1],inp[2])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")

        elif inp[0] == 'move':
            try:
                move_files(inp[1],inp[2])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")

        elif inp[0] == 'copy':
            try:
                copy_files(inp[1],inp[2])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")

        elif inp[0] == 'rm':
            try:
                delete_files(inp[1])
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")

        elif inp[0] == 'exit':
            x = False

        elif inp[0] == 'size':
            try:
                get_size()
            except:
                print(" Invalid syntax  Check the docs with 'help' or 'man' ")
            
        elif inp[0] == 'man' or inp[0] == 'help':
            print('''
                ls <path>                                   to list all the files or directories
                mkdir <path>                                to create a new directory
                put <input_file path> <file_path_in_dfs>    uploads the file to the dfs
                get <file_path_in_dfs> <output_file_path>   downloads the file from the dfs
                move <source_path> <destination_path>       moves the file from source_path to destination_path in dfs
                copy <source_path>  <destination_path>      copys the file from source_path to destination_path in dfs
                rm <path>                                   removes the file from dfs
                clear                                       clear the terminal
                size                                        tells the size of dfs
                exit 
            ''')

        # elif inp[0] == 'rmdir':
        #     try:
        #         remove_dir(inp[1])
        #     except:
        #         print(" Invalid syntax  Check the docs with 'help' or 'man' ")
        elif inp[0] == 'clear':
            clear_screen()
        else:
            print("Invalid Command or Syntax !")
        print()
