# Distributed File System

This project implements a distributed file system in Java. The system consists of:

- **Controller**: Manages the storage system, file replication, and client request coordination.
- **Dstores**: Nodes that store file data and handle file operations at the Controller's direction.
- **Client**: Provided externally — sends requests to the Controller and retrieves files directly from Dstores.

---

## Features

- **File replication**: Each file is replicated `R` times across different Dstores for redundancy.
- **Dynamic rebalance**: Files are redistributed when Dstores join or leave to maintain even distribution and replication.
- **Supports concurrent clients**: Handles multiple clients performing `STORE`, `LOAD`, `LIST`, and `REMOVE` operations simultaneously.
- **Failure tolerance**: Tolerates failures of up to `N - R` Dstores while maintaining correct operation.

---

## Getting Started

### Launch Commands

#### Controller
```
java Controller <controller_port> <R> <timeout_ms> <rebalance_period_sec>
```
- `controller_port`: Port the Controller listens on
- `R`: Replication factor (minimum number of copies per file)
- `timeout_ms`: Timeout in milliseconds for awaiting responses
- `rebalance_period_sec`: Interval for triggering rebalance operations

#### Dstore
```
java Dstore <dstore_port> <controller_port> <timeout_ms> <file_folder>
```
- `dstore_port`: Port Dstore listens on
- `controller_port`: Controller's port
- `timeout_ms`: Timeout in milliseconds
- `file_folder`: Local directory to store files (must exist before startup)

#### Client
```
java Client <controller_port> <timeout_ms>
```
- `controller_port`: Controller's port
- `timeout_ms`: Timeout in milliseconds

---

## Protocol Overview

### Join
- `Dstore → Controller`: `JOIN <port>`

### Store
1. `Client → Controller`: `STORE <filename> <filesize>`
2. `Controller → Client`: `STORE_TO <port1> <port2> ...`
3. `Client → Dstore`: `STORE <filename> <filesize>` (then sends file content)
4. `Dstore → Client`: `ACK`
5. `Dstore → Controller`: `STORE_ACK <filename>`
6. `Controller → Client`: `STORE_COMPLETE`

### Load
1. `Client → Controller`: `LOAD <filename>`
2. `Controller → Client`: `LOAD_FROM <port> <filesize>`
3. `Client → Dstore`: `LOAD_DATA <filename>`
4. `Dstore → Client`: file content

### Remove
1. `Client → Controller`: `REMOVE <filename>`
2. `Controller → Dstore`: `REMOVE <filename>`
3. `Dstore → Controller`: `REMOVE_ACK <filename>`
4. `Controller → Client`: `REMOVE_COMPLETE`

### List
- `Client → Controller`: `LIST`
- `Controller → Client`: `LIST <file_list>`

### Rebalance
Handled internally by the Controllers and DStores, clients do not interact with this protocol.
- Controller periodically or upon Dstore join:
  - Requests file lists from Dstores
  - Computes moves to maintain replication and balance
  - Directs Dstores to send/remove files
  - Awaits `REBALANCE_COMPLETE` from Dstores

---

## Failure Handling

- Timeouts trigger retries or errors (`ERROR_*`)
- Malformed messages ignored 
- Handles failure of up to `N - R` Dstores

---

## Example

```
# Start Controller
java Controller 6000 3 1000 30

# Start Dstores
java Dstore 7000 6000 1000 /tmp/dstore1
java Dstore 7001 6000 1000 /tmp/dstore2
java Dstore 7002 6000 1000 /tmp/dstore3

# Start Client
java Client 6000 1000
```

---


