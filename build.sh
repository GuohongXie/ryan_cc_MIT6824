#!/bin/bash
set -x

# delete old executables
rm src/map_reduce/main/libmr_word_count.so
rm src/map_reduce/main/mrworker
rm src/map_reduce/main/mrcoordinator
rm src/raft/raft
rm src/kv_raft/kv_raft_server
rm src/kv_raft/kv_raft_client

# delete old build files
rm -rf build
rm compile_commands.json

# build
mkdir build
cd ./build
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
cp compile_commands.json ../

make

# copy executables
cp bin/mrworker ../src/map_reduce/main/
cp bin/mrcoordinator ../src/map_reduce/main/
cp lib/libmr_word_count.so ../src/map_reduce/mrapps/
cp bin/raft ../src/raft/
cp bin/kv_raft_server ../src/kv_raft/
cp bin/kv_raft_client ../src/kv_raft/
