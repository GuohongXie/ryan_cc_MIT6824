#!/bin/sh

set -x

rm persis*
rm raft
#g++ -o0 -g -std=c++17 raft_ryan.cc -I../buttonrpc -lzmq -pthread -o raft

rm server
rm persis*
rm client
rm snap*

g++ -o0 -g -std=c++17 server_ryan.cc -I../buttonrpc -lzmq -pthread -o server
g++ -o0 -g -std=c++17 client_ryan.cc -I../buttonrpc -lzmq -pthread -o client


# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen