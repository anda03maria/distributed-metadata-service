# Distributed Metadata Service

A lightweight distributed metadata management system designed to demonstrate scalable and fault-tolerant coordination in distributed environments.

## Overview
This project implements a modular distributed metadata service composed of a gateway, a service registry, and multiple metadata nodes. The system illustrates key distributed systems principles such as deterministic partitioning, service discovery, concurrency control, fault tolerance, and gateway-level caching.

## Architecture
- **Gateway** – single entry point for clients, responsible for request routing, caching, and coordination  
- **Service Registry** – tracks active metadata nodes using heartbeat-based liveness detection  
- **Metadata Nodes** – store and manage partitioned metadata with fine-grained concurrency control  

## Features
- Deterministic routing of metadata requests  
- Dynamic service discovery using TTL and heartbeats  
- Fault tolerance through node failover  
- Asynchronous inter-service communication  
- Metadata caching for improved read performance  

## Project Structure
- service implementations, CLI, and demo scripts  
- system design and evaluation report  

## Notes
The system is implemented as an educational prototype and focuses on architectural clarity and correctness rather than production-level optimizations.
