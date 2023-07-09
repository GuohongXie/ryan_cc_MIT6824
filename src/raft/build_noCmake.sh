#!/bin/sh

set -x

g++ -std=c++17 raft.cc -I../buttonrpc -lzmq -pthread -o raft


# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen