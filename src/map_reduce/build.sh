#!/bin/sh

set -x
SOURCE_DIR=`pwd`

cd ./mrapps/
g++ -std=c++17 -fpic -c word_count.cc &&
g++ -std=c++17 -shared word_count.o -o libmap_reduce.so 
rm word_count.o
mv libmap_reduce.so ../main/libmap_reduce.so
cd ..

BUILD_DIR=${BUILD_DIR:-./build}
BUILD_TYPE=${BUILD_TYPE:-release}
#INSTALL_DIR=${INSTALL_DIR:-./${BUILD_TYPE}-install}
CXX=${CXX:-g++}

#ln -sf $BUILD_DIR/$BUILD_TYPE/compile_commands.json

mkdir -p $BUILD_DIR/$BUILD_TYPE \
  && cd $BUILD_DIR/$BUILD_TYPE \
  && cmake \
           -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
           -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
           $SOURCE_DIR \
  && make $*

cp ./bin/mrcoordinator ../../main/mrcoordinator
cp ./bin/mrworker ../../main/mrworker
cp ./bin/mrworker_raw ../../main/mrworker_raw
cp ./bin/mrcoordinator_raw ../../main/mrcoordinator_raw

cp ./bin/mrworker_pure_raw ../../main/mrworker_pure_raw
cp ./bin/mrcoordinator_pure_raw ../../main/mrcoordinator_pure_raw

# Use the following command to run all the unit tests
# at the dir $BUILD_DIR/$BUILD_TYPE :
# CTEST_OUTPUT_ON_FAILURE=TRUE make test

# cd $SOURCE_DIR && doxygen