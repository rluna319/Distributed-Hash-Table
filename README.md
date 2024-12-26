# Distributed Transparent Memory (Two-Layer Hash Table)

This project, **Distributed Transparent Memory**, implements a simple two-layer hash table using socket programming in C. It is designed as a learning exercise to explore concepts in distributed systems and network communication, particularly using TCP and UDP sockets.

The project is implemented in a single C file with minimal modularity, and the code can be a bit messy—please bear with it! The primary goal is to understand socket programming, hash tables, and basic distributed system concepts rather than delivering production-ready code.

---

## Overview

The project simulates a distributed memory system where multiple nodes (processes) collaborate to store and retrieve key-value pairs transparently. The memory is divided across nodes, but users can perform operations (`PUT` and `GET`) without needing to know which node stores their data.

This project uses:
- **UDP** for forwarding requests between nodes.
- **TCP** for exchanging key-value pairs between nodes.

---

## Features

- **Ring Topology**: Nodes are connected in a logical ring, and each node knows its successor's UDP port for forwarding messages.
- **Two-Layer Hashing**:
  1. First-layer hash determines which node is responsible for storing a key.
  2. Second-layer hash maps the key to a bucket within the node's local hash table.
- **PUT Operation**:
  - Hashes the key to determine the target node.
  - Forwards the request using UDP.
  - Establishes a TCP connection to exchange the value if the target node is reached.
- **GET Operation**:
  - Hashes the key to locate the target node.
  - Forwards the request via UDP and retrieves the value over TCP.
- **Single-File Implementation**: All functionality is implemented in a single C file for simplicity.

---

## How It Works

1. **PUT Request**:
   - User initiates a `PUT key value` command.
   - The key is hashed to determine the responsible node.
   - The request is forwarded via UDP until it reaches the target node.
   - The target node requests the value from the originator over TCP and stores it locally.

2. **GET Request**:
   - User initiates a `GET key` command.
   - The key is hashed to determine the responsible node.
   - The request is forwarded via UDP until it reaches the target node.
   - The target node fetches the value locally and sends it to the originator over TCP.

---

## Key Points

- The project uses `select()` to manage multiple sockets (UDP, TCP, and console input) simultaneously.
- A hash table is implemented with chaining to handle collisions.
- The code assumes a fixed number of nodes (`NODE_COUNT`) and a single thread per node for simplicity.
- Ports must be configured manually when launching nodes.

---

## Known Issues & Limitations

1. **Single-File Implementation**:
   - All logic resides in a single C file, making the code less modular and harder to debug.
2. **Minimal Error Checking**:
   - Error handling is limited and should be extended for production use.
3. **Messy Code**:
   - The code is written for learning purposes, so it may be verbose and lack polish.
4. **Manual Configuration**:
   - Node information (e.g., IP and port numbers) must be configured manually when starting nodes.
5. **No Graceful Shutdown**:
   - Connections and resources are not always closed properly upon termination.

---

## Learning Objectives

This project helped me learn:
- How to use TCP and UDP sockets in C.
- Managing multiple socket connections with `select()`.
- Implementing a basic hash table with collision handling.
- Designing a simple distributed system with a ring topology.

---

## Future Improvements

- Modularize the code by splitting it into multiple files (e.g., hash table, networking).
- Add comprehensive error checking and graceful shutdown.
- Use dynamic node discovery instead of hardcoding IP and port information.
- Improve performance by introducing threading for concurrent processing.

---

## Disclaimer

This project is for educational purposes only and is not intended for production use. The code is a bit messy, as the primary focus was on learning rather than clean implementation.
