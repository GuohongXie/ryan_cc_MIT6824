#!/bin/sh

set -x
SOURCE_DIR=`pwd`

cd ./mrapps/
g++ -std=c++17 -fpic -c word_count.cc &&
g++ -std=c++17 -shared word_count.o -o libmap_reduce.so 
rm word_count.o
mv libmap_reduce.so ../main/libmap_reduce.so


cd ../main/
g++ -std=c++17 mrworker_old.cc -I../../buttonrpc -I../mr -lzmq -pthread -ldl -o mrworker_old
g++ -std=c++17 mrcoordinator.cc ../mr/coordinator.cc -I../../buttonrpc -I../mr -lzmq -pthread -o mrcoordinator


# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen