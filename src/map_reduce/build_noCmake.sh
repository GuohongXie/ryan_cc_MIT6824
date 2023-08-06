#!/bin/sh

set -x
SOURCE_DIR=`pwd`

cd ./mrapps/
rm libmr_word_count.so
g++ -std=c++17 -fpic -c word_count.cc &&
g++ -std=c++17 -shared word_count.o -o libmr_word_count.so 
rm word_count.o


cd ../main/
rm mrworker
rm mrcoordinator
g++ -std=c++17 mrworker.cc -I../.. -lzmq -pthread -ldl -o mrworker -g -Wall -O0
g++ -std=c++17 mrcoordinator.cc ../mr/coordinator.cc -I../.. -lzmq -pthread -o mrcoordinator -g -Wall -O0


# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen