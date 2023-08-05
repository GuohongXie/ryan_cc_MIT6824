#!/bin/sh

set -x

rm persis*
rm kv_raft_raft
g++ -o0 -g -std=c++17 raft.cc -I../ -lzmq -pthread -o kv_raft_raft

rm kv_raft_server
rm kv_raft_client
rm snap*

g++ -o0 -g -std=c++17 server.cc -I../ -lzmq -pthread -o kv_raft_server
g++ -o0 -g -std=c++17 client.cc -I../ -lzmq -pthread -o kv_raft_client


# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen