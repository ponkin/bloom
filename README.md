# Bloom filters

## Core
Pure Java library with different types of bloom filters.
Zero dependencies. Onheap and Offheap memory management.
Supports huge(more than 2GB ) filters with billions of items.
Thread safe and fast. The following filter types are implemented
* BloomFilter - classic bloom filter
* StableBloomFilter - bloom filter with the ability to automatically evict 'old' items frm filter.
* CuckooFilter - bloom filter variant with removal and more space efficient
* ScalableBloomFilter - bloom filter with dynamic size

## Server
Fast standalone bloom filter storage server.
You can create filters in '/dev/shm' for filter persistence.
Very fast and can be run in docker

## Driver
Scala asynchronous driver for bloom server

## Spark-connector
Apache Spark connector. Utility library to access
bloom server from Apache Spark jobs.
