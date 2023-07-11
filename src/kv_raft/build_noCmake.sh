#!/bin/sh

set -x

#rm persis*
#rm raft
#g++ -o0 -g -std=c++17 raft_ryan.cc -I../buttonrpc -lzmq -pthread -o raft
#g++ -o0 -g -std=c++17 client.cc -I../buttonrpc -lzmq -pthread -o client
rm server
rm persis*
g++ -o0 -g -std=c++17 server.cc -I../buttonrpc -lzmq -pthread -o server


# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen