# Distributed File System using Python and RPC

## Overview

This project implements a distributed file system (DFS) in Python that leverages Remote Procedure Call (RPC) for inter-node communication. The system includes fault tolerance with a replication factor of three, ensuring high availability and reliability. It consists of a Name Node for managing metadata and Data Nodes for storing actual data blocks.

## Features

- **RPC Communication**: Facilitates seamless interactions between Name Node, Data Nodes, and clients.
- **Fault Tolerance**: Ensures data availability with a replication factor of three.
- **Scalability**: Easily add more Data Nodes to the cluster to scale out.
- **Consistency and Reliability**: Strong consistency through coordinated writes and data replication.

## Components

### Name Node
- Manages metadata, including directory structure, file locations, and permissions.
- Coordinates data operations such as read, write, and delete.
- Tracks the health and status of Data Nodes through regular heartbeats.
- Handles client requests and directs them to the appropriate Data Nodes for data retrieval and storage.

### Data Nodes
- Store actual data blocks.
- Handle read and write requests from clients as directed by the Name Node.
- Regularly send heartbeats to the Name Node to confirm their status.
- Replicate data blocks to ensure fault tolerance.

## Technical Implementation

- **Programming Language**: Python
- **Data Storage**: Local filesystem of each Data Node for storing data blocks.
- **Metadata Storage**: In-memory data structures or a local filesystem on the Name Node for managing metadata.

## Install Dependencies
```sh
 pip install -r requirements.txt
```
## Create the Configuration File:
Create a config.json file in the root directory of the project with the following content:
```json
{
    "block_size": 10000,
    "replication_factor": 3,
    "data_nodes": [
        "data_node_1_address",
        "data_node_2_address",
        "data_node_3_address"
    ]
}

```

## Start NameNode
```sh
python name_node.py
```

## Start DataNode
```sh
python data_node.py <NODE_PORT>
```
Replace <NODE_PORT> with a unique port for each Data Node.





